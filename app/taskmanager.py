"""后台拼接任务调度。

流水线:MOSAIC(gdalwarp 重投影+镶嵌)-> COG(gdal_translate 生成带概览的 COG)
-> METADATA(gdalinfo 读取范围/尺寸)。全部成功才发布结果。

进程收尾设计(修复要点):
- 每个 GDAL 阶段都是独立子进程,通过 ``start_new_session`` 成为独立会话/进程组
  的首进程,其派生的同组子进程(如 ``gdalwarp -multi`` 的工作进程)pgid 相同。
- **以主进程 ``wait()`` 退出为准,不以日志管道 EOF 为准**。GDAL 主进程异常退出后,
  同组子进程可能仍持有 stdout 管道(EOF 永不到来,任务卡在"运行中"),也可能已经
  关闭日志继续干活(EOF 提前到来,若据此收尾就会误判失败并在子进程仍工作时
  删除暂存)。因此主进程一退出就主动按进程组 SIGKILL 残留进程并确认整组消失,
  绝不等待日志自行关闭。
- 取消与失败走**同一条幂等收尾路径**(停止同组进程 -> 等待回收 -> 关闭/排空
  日志管道 -> 清理暂存 -> 置终态),全部完成后才释放并行名额;重复取消等待的是
  同一个收尾 Future,不会触发第二次收尾。
- 阶段启动前检查取消标志:取消与主进程退出交错时,绝不启动下一阶段,也绝不发布
  迟到结果或开放下载。
- 失败/取消后清理该任务的暂存目录;完成的任务保留结果供下载。
- 像素数据全程由 GDAL 流式处理,不把整幅栅格读入内存。
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

STATUS_QUEUED = "QUEUED"
STATUS_RUNNING = "RUNNING"
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"
STATUS_CANCELLED = "CANCELLED"
TERMINAL_STATUSES = {STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED}

STAGE_MOSAIC = "MOSAIC"
STAGE_COG = "COG"
STAGE_METADATA = "METADATA"

TARGET_CRS = "EPSG:3857"
DST_NODATA = "-9999"

# 演示模式(仅替换 MOSAIC 阶段的可执行程序,拼接规则与其余阶段不变)
DEMO_OFF = "off"        # 真实 gdalwarp
DEMO_HOLD = "hold"      # 主进程先退,同组子进程继续持有日志管道
DEMO_CLOSE = "close"    # 主进程先退,同组子进程关闭日志管道继续干活
DEMO_MODES = {DEMO_OFF, DEMO_HOLD, DEMO_CLOSE}

DEMO_STUB = Path(__file__).resolve().parent / "demo" / "gdalwarp_demo.py"

MAX_TASKS_KEPT = 200  # 内存中保留的最大任务记录数,超出时回收最旧的终态任务

# 各步收尾的有界等待(秒),保证取消/失败端点不会无限期挂起
PROC_WAIT_TIMEOUT = 10.0
GROUP_DRAIN_TIMEOUT = 10.0
READER_DRAIN_TIMEOUT = 10.0
CANCEL_TEARDOWN_TIMEOUT = 30.0
EXIT_POLL_INTERVAL = 0.05  # 主进程退出检测粒度(不依赖日志管道 EOF)


class StageError(Exception):
    """某个 GDAL 阶段执行失败。"""


class TaskCancelled(Exception):
    """任务被取消(在阶段边界或子进程退出后检测到)。"""


@dataclass
class Task:
    id: str
    resolution: float
    filenames: list  # 原始文件名,按覆盖优先级从低到高排列
    workdir: Path
    created_at: float = field(default_factory=time.time)
    status: str = STATUS_QUEUED
    stage: Optional[str] = None
    error: Optional[str] = None
    log_tail: str = ""
    result: Optional[dict] = None
    cancel_requested: bool = False
    demo_mode: str = DEMO_OFF
    # 运行期句柄
    proc: Optional[asyncio.subprocess.Process] = None
    pgid: Optional[int] = None
    reader: Optional[asyncio.Task] = None
    stage_output: bytes = b""
    signal_sent: bool = False  # 当前阶段是否已发过终止信号(重复取消去重)
    # 同一次收尾的共享 Future:取消端点与运行协程都等待它,重复取消复用
    teardown_future: Optional[asyncio.Future] = None
    # 停止回收只实际执行一次的守卫与其结果(残留进程计数);_exec 与 _finish 复用
    reap_future: Optional[asyncio.Future] = None
    reap_killed: int = 0
    # 收尾观测信息(终止回收的同组进程数等),用于界面/演示核对
    cleanup: Optional[dict] = None

    @property
    def inputs_dir(self) -> Path:
        return self.workdir / "inputs"

    def public(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "stage": self.stage,
            "error": self.error,
            "resolution": self.resolution,
            "filenames": list(self.filenames),
            "created_at": self.created_at,
            "log_tail": self.log_tail[-400:],
            "result": self.result,
            "demo_mode": self.demo_mode,
            "cleanup": self.cleanup,
            "download_url": f"/api/tasks/{self.id}/download"
            if self.status == STATUS_DONE
            else None,
        }


class TaskManager:
    def __init__(self, tasks_root: Path, max_parallel: int = 2):
        self.tasks_root = Path(tasks_root)
        self.tasks_root.mkdir(parents=True, exist_ok=True)
        self.sem = asyncio.Semaphore(max_parallel)
        self.tasks: dict = {}
        self._runners: set = set()

    # ---------- 任务生命周期 ----------

    def create(self, filenames, resolution: float, demo_mode: str = DEMO_OFF) -> Task:
        """建立任务记录与暂存目录(尚未调度)。上传校验失败可调用 discard。"""
        self._gc()
        tid = uuid.uuid4().hex[:12]
        workdir = self.tasks_root / tid
        (workdir / "inputs").mkdir(parents=True)
        t = Task(
            id=tid,
            resolution=resolution,
            filenames=list(filenames),
            workdir=workdir,
            demo_mode=demo_mode if demo_mode in DEMO_MODES else DEMO_OFF,
        )
        self.tasks[tid] = t
        return t

    def enqueue(self, t: Task) -> None:
        runner = asyncio.create_task(self._run(t))
        self._runners.add(runner)
        runner.add_done_callback(self._runners.discard)

    def discard(self, t: Task) -> None:
        """任务尚未入队时放弃(如上传校验失败)。"""
        self.tasks.pop(t.id, None)
        shutil.rmtree(t.workdir, ignore_errors=True)

    async def cancel(self, task_id: str) -> Optional[Task]:
        """请求取消并**等待同一次收尾完成**后才返回。

        - 置取消标志,立即对本任务当前进程组发 SIGKILL(不等日志关闭);
        - 排队任务直接走统一收尾(无进程,仅清暂存);
        - 重复取消复用同一个 teardown Future,保证只做一次收尾、两次调用
          都等到同一批进程回收与暂存清理结束。
        """
        t = self.tasks.get(task_id)
        if t is None or t.status in TERMINAL_STATUSES:
            return t
        t.cancel_requested = True
        self._signal_stop(t)  # 主动终止,不等管道 EOF
        if t.status == STATUS_QUEUED:
            # 排队任务没有运行中的进程,直接在请求内完成收尾
            await self._finish(t, STATUS_CANCELLED)
            return t
        future = self._teardown_future(t)
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout=CANCEL_TEARDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            # 收尾各步本身有界;此处兜底返回,运行协程仍会完成收尾
            pass
        return t

    def cleanup_stale(self) -> None:
        """启动时清空上次运行遗留的暂存目录(内存注册表已重置)。"""
        for child in self.tasks_root.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    # ---------- 内部:运行与收尾 ----------

    def _gc(self) -> None:
        if len(self.tasks) <= MAX_TASKS_KEPT:
            return
        terminal = sorted(
            (t for t in self.tasks.values() if t.status in TERMINAL_STATUSES),
            key=lambda t: t.created_at,
        )
        for t in terminal[: len(self.tasks) - MAX_TASKS_KEPT]:
            self.tasks.pop(t.id, None)
            if t.status != STATUS_DONE:
                shutil.rmtree(t.workdir, ignore_errors=True)

    def _teardown_future(self, t: Task) -> asyncio.Future:
        if t.teardown_future is None:
            t.teardown_future = asyncio.get_running_loop().create_future()
        return t.teardown_future

    def _resolve_teardown(self, t: Task) -> None:
        future = t.teardown_future
        if future is not None and not future.done():
            future.set_result(True)

    def _signal_stop(self, t: Task) -> None:
        """同步发出终止信号:优先整组 SIGKILL,失败再单杀主进程。幂等去重。"""
        proc = t.proc
        if proc is None or proc.returncode is not None:
            return
        if t.signal_sent:
            return  # 重复取消/收尾复用:同一次收尾只发一次信号
        pgid = t.pgid or proc.pid
        try:
            # 子进程在独立会话/进程组中,整组 SIGKILL:连同持有/关闭日志管道的
            # 同组残留子进程一起终止,只影响本任务,不影响另一并行任务。
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except (ProcessLookupError, PermissionError):
                pass
        t.signal_sent = True

    @staticmethod
    def _group_pids(pgid: int) -> list:
        """枚举 /proc 中仍属于该进程组的 pid(僵尸/存活都算,用于确认整组消失)。"""
        try:
            names = os.listdir("/proc")
        except OSError:
            return []
        pids = []
        for name in names:
            if not name.isdigit():
                continue
            try:
                raw = open(f"/proc/{name}/stat", encoding="ascii").read()
                # comm 可能含空格/括号,取最后一个 ')' 之后的字段:
                # state, ppid, pgrp, session, ...
                rest = raw[raw.rfind(")") + 2:].split()
                if int(rest[2]) == pgid:
                    pids.append(int(name))
            except (OSError, ValueError, IndexError):
                continue
        return pids

    @staticmethod
    async def _wait_parent_exit(proc: asyncio.subprocess.Process, timeout: float) -> None:
        """等待主进程退出,**只认 returncode,不认日志管道 EOF**。

        asyncio 的 ``await proc.wait()`` 在 stdout 仍被同组孙进程持有时不会返回
        (管道不 EOF,子监视器不触发 connection_lost),即使主进程早已退出、
        ``returncode`` 已由 SIGCHLD 处理置位。因此这里直接轮询 ``returncode``,
        使"主进程先退"能被立刻检测到并进入主动收尾。
        """
        deadline = time.monotonic() + timeout
        while proc.returncode is None and time.monotonic() < deadline:
            await asyncio.sleep(EXIT_POLL_INTERVAL)

    async def _stop_and_reap(self, t: Task) -> int:
        """确认本任务同组进程全部停止并完成回收(每个任务只实际执行一次)。

        ``_exec`` 的失败/取消分支与统一收尾 ``_finish`` 都会调用本方法;通过
        共享 Future 保证真正的终止-回收只做一次,重复调用等待并复用同一结果,
        既不会二次清理,也不会让残留进程计数被后续空跑覆盖为 0。
        """
        if t.reap_future is None:
            t.reap_future = asyncio.ensure_future(self._do_stop_and_reap(t))
        await asyncio.shield(t.reap_future)
        return t.reap_killed

    async def _do_stop_and_reap(self, t: Task) -> None:
        """实际执行:终止整组 -> 回收主进程 -> 确认同组消失 -> 排空日志管道。"""
        proc = t.proc
        if proc is None:
            await self._drain_reader(t, READER_DRAIN_TIMEOUT)
            t.signal_sent = False
            t.reap_killed = 0
            return
        pgid = t.pgid or proc.pid

        # 1) 主动终止整组(取消者可能已经发过,重复信号无害)
        self._signal_stop(t)

        # 2) 回收主进程(有界,超时补发信号)。轮询 returncode,避免被日志管道阻塞
        if proc.returncode is None:
            await self._wait_parent_exit(proc, PROC_WAIT_TIMEOUT)
            if proc.returncode is None:
                self._signal_stop(t)
                await self._wait_parent_exit(proc, PROC_WAIT_TIMEOUT)

        # 3) 确认同组残留(孙进程)全部消失;整组补杀直到 /proc 中再无该 pgid。
        #    主进程此刻已退出,组内剩余成员即被遗留的同组孙进程(含僵尸),
        #    用集合累积所有观测到的残留 pid,避免补杀间隙被 init 回收后计数落空。
        seen: set = set()
        deadline = time.monotonic() + GROUP_DRAIN_TIMEOUT
        while time.monotonic() < deadline:
            pids = self._group_pids(pgid)
            if not pids:
                break
            seen.update(pids)
            try:
                # 再次整组 SIGKILL(幂等);也对逐个 pid 补杀以兜底权限边界
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
            await asyncio.sleep(0.05)
        t.reap_killed = len(seen)

        # 4) 组内已无进程,日志管道必然 EOF;排空读取任务,必要时强制关闭管道
        await self._drain_reader(t, READER_DRAIN_TIMEOUT)

        t.proc = None
        t.pgid = None
        t.signal_sent = False

    async def _drain_reader(self, t: Task, timeout: float) -> None:
        reader = t.reader
        if reader is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(reader), timeout=timeout)
        except asyncio.TimeoutError:
            # 理论不可达:整组 SIGKILL 后所有管道写端都已关闭。
            # 兜底强制关闭传输并取消读取,避免收尾挂死。
            self._force_close_pipe(t)
            reader.cancel()
            try:
                await reader
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        finally:
            t.reader = None

    @staticmethod
    def _force_close_pipe(t: Task) -> None:
        proc = t.proc
        try:
            transport = proc._transport  # type: ignore[attr-defined]
            pipe = transport.get_pipe_transport(1)  # stdout(stderr 已并入)
            if pipe is not None:
                pipe.close()
        except Exception:  # noqa: BLE001 - 仅为兜底
            pass

    async def _finish(self, t: Task, status: str, error: Optional[str] = None) -> None:
        """统一终态收尾:停进程 -> 回收 -> 排管道 -> 清暂存 -> 置终态 -> 唤醒等待者。

        幂等:已终态的任务直接返回。并行名额由 _run 在本方法返回后才释放。
        """
        if t.status in TERMINAL_STATUSES:
            self._resolve_teardown(t)
            return
        killed = await self._stop_and_reap(t)
        if status in (STATUS_FAILED, STATUS_CANCELLED):
            # 进程已全部回收后才删除暂存,不会出现子进程仍在写目录时被 rmtree
            shutil.rmtree(t.workdir, ignore_errors=True)
        t.status = status
        t.stage = None
        t.error = error
        t.cleanup = {"group_pids_stopped": killed}
        self._resolve_teardown(t)

    async def _run(self, t: Task) -> None:
        # 信号量在整个收尾结束后才释放:终态可见时,进程回收与暂存清理必然已完成
        async with self.sem:
            try:
                if t.status in TERMINAL_STATUSES:
                    return  # 排队期间已被取消并清理
                if t.cancel_requested:
                    await self._finish(t, STATUS_CANCELLED)
                    return
                t.status = STATUS_RUNNING
                try:
                    await self._mosaic(t)
                    await self._cog(t)
                    await self._metadata(t)
                except TaskCancelled:
                    await self._finish(t, STATUS_CANCELLED)
                except StageError as e:
                    await self._finish(t, STATUS_FAILED, str(e))
                except Exception as e:  # noqa: BLE001 - 兜底,保证任务进入终态
                    await self._finish(t, STATUS_FAILED, f"内部错误: {e!r}")
                else:
                    # 发布前再检查:取消后绝不发布迟到结果、绝不开放下载
                    if t.cancel_requested or t.status in TERMINAL_STATUSES:
                        await self._finish(t, STATUS_CANCELLED)
                    else:
                        await self._finish(t, STATUS_DONE)
            finally:
                # 兜底:任何意外路径都保证终态 + 清理 + 名额释放
                if t.status not in TERMINAL_STATUSES:
                    await self._finish(t, STATUS_FAILED, "收尾兜底:任务异常中断")
                self._resolve_teardown(t)

    async def _pump_logs(self, t: Task, stream: asyncio.StreamReader) -> None:
        """后台持续读日志管道;读取生命周期与主进程是否退出解耦。

        本阶段的完整输出直接累积到 ``t.stage_output``(每阶段开始时已清空),
        避免读取任务的返回值在跨阶段复用时被覆盖。
        """
        try:
            while True:
                chunk = await stream.read(4096)
                if not chunk:
                    break
                t.stage_output += chunk
                t.log_tail = (t.log_tail + chunk.decode("utf-8", "replace"))[-800:]
        finally:
            try:
                stream.close()
            except Exception:  # noqa: BLE001
                pass

    async def _exec(
        self,
        t: Task,
        stage: str,
        cmd: list,
        env_extra: Optional[dict] = None,
    ) -> str:
        """执行一个阶段。以主进程退出为准,主动收尾同组残留,不依赖日志 EOF。"""
        if t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()
        t.stage = stage
        t.log_tail = ""
        t.stage_output = b""
        t.signal_sent = False
        env = None
        if env_extra:
            env = dict(os.environ)
            env.update(env_extra)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(t.workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,  # 独立进程组,便于整组终止
                env=env,
            )
        except FileNotFoundError:
            raise StageError(f"找不到可执行程序: {cmd[0]}")
        t.proc = proc
        t.pgid = proc.pid
        if t.cancel_requested:
            # 取消恰好发生在进程创建窗口:立即终止,不进入等待
            self._signal_stop(t)
        t.reader = asyncio.create_task(self._pump_logs(t, proc.stdout))

        # 关键:以主进程退出(returncode)为准,而不是等日志管道 EOF。
        # 主进程异常退出后,同组子进程可能仍持有管道(EOF 不来 -> 旧逻辑永远
        # 显示运行),也可能已关闭管道继续干活(EOF 提前来 -> 旧逻辑提前报失败并
        # 删暂存)。两种情况都不能作为收尾依据。轮询 returncode 由 SIGCHLD 置位,
        # 与管道状态无关;取消标志在此期间也能被及时响应。
        while proc.returncode is None:
            if t.cancel_requested:
                # 取消与主进程退出交错:不等下一阶段,立即主动收尾
                await self._stop_and_reap(t)
                raise TaskCancelled()
            await asyncio.sleep(EXIT_POLL_INTERVAL)
        rc = proc.returncode

        if rc != 0 or t.cancel_requested or t.status in TERMINAL_STATUSES:
            killed = await self._stop_and_reap(t)
            if t.cancel_requested or t.status in TERMINAL_STATUSES:
                raise TaskCancelled()
            raise StageError(
                f"{stage} 阶段主进程异常退出 (exit {rc});已主动终止并回收同组残留 "
                f"{killed} 个进程后才判定失败。日志尾: {t.log_tail[-300:]}"
            )

        # rc == 0:防御性确认同组无残留(正常 GDAL 会自行带走工作进程),
        # 同时完成日志读取;若此时发现取消,绝不进入下一阶段。
        killed = await self._stop_and_reap(t)
        if t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()
        output = t.stage_output.decode("utf-8", "replace")
        if killed:
            t.log_tail = (
                t.log_tail + f"[manager] 主进程成功退出后仍回收同组残留 {killed} 个进程\n"
            )[-800:]
        return output

    async def _mosaic(self, t: Task) -> None:
        inputs = sorted(p for p in t.inputs_dir.iterdir() if p.is_file())
        if not inputs:
            raise StageError("没有输入文件")
        if t.demo_mode != DEMO_OFF:
            # 演示:用可控桩替换 gdalwarp,复现"主进程先退"的两种管道行为;
            # 输出文件名、工作目录与真实流水线一致,后续阶段规则不变。
            cmd = [sys.executable, str(DEMO_STUB), str(t.workdir / "mosaic.tif")]
            await self._exec(
                t, STAGE_MOSAIC, cmd, env_extra={"DEMO_MODE": t.demo_mode}
            )
            return
        res = f"{t.resolution:g}"
        # 输入按优先级从低到高排列:gdalwarp 依次写入,后文件的有效像元覆盖先文件;
        # 各输入自带的 NoData 会被 gdalwarp 识别为掩膜,不会盖掉已有高程。
        cmd = [
            "gdalwarp",
            "-t_srs", TARGET_CRS,
            "-tr", res, res,
            "-r", "bilinear",
            "-dstnodata", DST_NODATA,
            "-ot", "Float32",
            "-of", "GTiff",
            "-co", "TILED=YES",
            "-co", "COMPRESS=DEFLATE",
            "-wm", "256",
            "--config", "GDAL_CACHEMAX", "256",
            "-multi",
            "-wo", "NUM_THREADS=2",
        ]
        cmd += [str(p) for p in inputs]
        cmd.append(str(t.workdir / "mosaic.tif"))
        await self._exec(t, STAGE_MOSAIC, cmd)

    async def _cog(self, t: Task) -> None:
        cmd = [
            "gdal_translate",
            "-of", "COG",
            "-co", "COMPRESS=DEFLATE",
            "-co", "RESAMPLING=AVERAGE",  # 概览重采样
            "-co", "NUM_THREADS=2",
            "mosaic.tif",
            "result.tif",
        ]
        await self._exec(t, STAGE_COG, cmd)

    async def _metadata(self, t: Task) -> None:
        out = await self._exec(t, STAGE_METADATA, ["gdalinfo", "-json", "result.tif"])
        try:
            info = json.loads(out)
            w, h = info["size"]
            gt = info["geoTransform"]
        except (ValueError, KeyError) as e:
            raise StageError(f"解析 gdalinfo 输出失败: {e}")
        xs = [gt[0], gt[0] + w * gt[1]]
        ys = [gt[3], gt[3] + h * gt[5]]
        t.result = {
            "width": w,
            "height": h,
            "extent": [min(xs), min(ys), max(xs), max(ys)],  # EPSG:3857 米
            "crs": TARGET_CRS,
            "file_size_bytes": (t.workdir / "result.tif").stat().st_size,
        }
