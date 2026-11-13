#!/usr/bin/env python3
"""TaskManager 调度逻辑测试(无需真实 GDAL / FastAPI)。

在临时目录生成 gdalwarp / gdal_translate / gdalinfo 桩程序并前置到 PATH。
本仓库的运行环境可能禁止向进程发信号(沙箱),因此测试将 os.killpg 替换为
"记录调用 + 写哨兵文件让同组桩退出"的假实现,以验证编排逻辑;真实的进程组
SIGKILL 在 Docker 容器中生效(见 README 的手动验证步骤)。

覆盖:
- 两任务并行与排队、取消其一不影响另一、槽位在收尾完成后才释放;
- 取消排队任务、COG 阶段取消不发布结果;
- 重复取消只做一次收尾(共享 Future),两次调用都等到同一批回收完成;
- **主进程异常先退**:同组孙进程分别 (a) 持有日志管道、(b) 关闭日志管道继续
  干活——调度器均须以主进程 wait() 为准,主动终止整组、确认回收与暂存清理后
  才报 FAILED;孙进程存活期间不得提前终态或删暂存。

运行: python3 -I tests/test_taskmanager.py
"""
import asyncio
import os
import signal
import stat
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import taskmanager as tm  # noqa: E402

# gdalwarp 桩:
# - 参数含 slow:主进程挂起,等待 KILL-<pgid> 哨兵(模拟可被取消的长任务);
# - 环境 CRASH=1:fork 同组孙进程后,父进程睡 PARENT_SLEEP 秒再以 exit 1 退出,
#   不 wait 子进程;孙进程按 CRASH_PIPE=hold/close 决定持有/关闭日志管道,
#   并持续写 child_progress.log,直到 KILL-<pgid> 哨兵出现。
GDALWARP = r'''#!/usr/bin/env python3
import os, sys, time
args = sys.argv[1:]
out = args[-1]
pgid = os.getpgid(0)
if os.environ.get("CRASH") == "1":
    mode = os.environ.get("CRASH_PIPE", "hold")
    pid = os.fork()
    if pid == 0:
        if mode == "close":
            os.close(1)  # 关闭继承来的日志管道写端,但继续干活
        progress = os.path.join(os.getcwd(), "child_progress.log")
        with open(os.path.join(os.getcwd(), "grandchild.pid"), "w") as f:
            f.write(str(os.getpid()))
        deadline = time.time() + 60
        while time.time() < deadline:
            if os.path.exists(f"KILL-{pgid}"):
                os._exit(0)
            try:
                with open(progress, "a") as f:
                    f.write(f"grandchild {os.getpid()} pgid={os.getpgid(0)} working\n")
            except OSError:
                pass
            if mode == "hold":
                try:
                    os.write(1, b"grandchild still holding log pipe\n")
                except OSError:
                    pass
            time.sleep(0.05)
        os._exit(0)
    time.sleep(float(os.environ.get("PARENT_SLEEP", "0.4")))
    open(out, "wb").write(b"partial")
    os._exit(1)  # 主进程异常退出,留下同组孙进程
if any("slow" in a for a in args):
    deadline = time.time() + 60
    while time.time() < deadline:
        if os.path.exists(f"KILL-{pgid}"):
            sys.exit(0)
        time.sleep(0.05)
open(out, "wb").write(b"mosaic")
'''

GDAL_TRANSLATE = """#!/usr/bin/env python3
import os, shutil, sys, time
args = sys.argv[1:]
if os.environ.get("COG_SLOW") == "1":
    pgid = os.getpgid(0)
    deadline = time.time() + 60
    while time.time() < deadline:
        if os.path.exists(f"KILL-{pgid}"):
            sys.exit(0)
        time.sleep(0.05)
shutil.copyfile(args[-2], args[-1])
"""

GDALINFO = """#!/usr/bin/env python3
print('{"size":[120,80],"geoTransform":[100.0,2.0,0.0,200.0,0.0,-2.0],'
      '"bands":[{}],"coordinateSystem":{"wkt":"PROJCS[stub]"}}')
"""


def make_stubs(root: Path) -> None:
    for name, body in [
        ("gdalwarp", GDALWARP),
        ("gdal_translate", GDAL_TRANSLATE),
        ("gdalinfo", GDALINFO),
    ]:
        p = root / name
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


async def wait_for(cond, timeout=10.0, what="condition"):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"超时等待: {what}")


async def wait_while(cond, timeout=10.0, what="condition to clear"):
    """轮询直到 cond() 为假(用于观察孙进程存活期间的中间态)。"""
    t0 = time.monotonic()
    saw_true = False
    while time.monotonic() - t0 < timeout:
        if cond():
            saw_true = True
        elif saw_true:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"超时等待: {what}")


def install_fake_killpg(tmp: Path):
    """假 killpg:记录调用;仅在 armed 为真时写哨兵让同组桩退出。

    disarmed 期间可以复现"信号已发但进程尚未消失"的窗口,用于观察调度器
    在孙进程仍存活时保持 RUNNING、不删暂存的中间态。
    """
    state = {"armed": True, "calls": []}
    real_killpg = os.killpg
    real_kill = os.kill

    def fake_killpg(pgid, sig):
        state["calls"].append((pgid, sig))
        if not state["armed"]:
            return
        tasks = tmp / "tasks"
        if tasks.is_dir():
            for d in tasks.iterdir():
                if d.is_dir():
                    (d / f"KILL-{pgid}").write_text("1")

    def fake_kill(pid, sig):
        # 逐个补杀同样降级为"记录不真杀":桩的退出完全由哨兵驱动,
        # 这样 disarmed 窗口内孙进程才能稳定存活以供观察中间态。
        state["calls"].append((pid, sig))

    os.killpg = fake_killpg
    os.kill = fake_kill
    return state, real_killpg, real_kill


async def main():
    tmp = Path(tempfile.mkdtemp(prefix="tm-test-"))
    (tmp / "bin").mkdir()
    make_stubs(tmp / "bin")
    os.environ["PATH"] = str(tmp / "bin") + os.pathsep + os.environ["PATH"]

    sig_state, real_killpg, real_kill = install_fake_killpg(tmp)
    # 测试环境加速组回收等待
    tm.GROUP_DRAIN_TIMEOUT = 5.0
    tm.READER_DRAIN_TIMEOUT = 5.0
    try:
        mgr = tm.TaskManager(tmp / "tasks", max_parallel=2)

        def mk(input_name, res=2.0):
            t = mgr.create([input_name], res)
            (t.inputs_dir / input_name).write_bytes(b"x")
            mgr.enqueue(t)
            return t

        # --- 场景 1:两任务并行,取消其一不影响另一;排队任务随后补位 ---
        t1 = mk("00_slow.tif")          # gdalwarp 挂起,等待取消
        t2 = mk("00.tif")
        t3 = mk("00.tif")
        await wait_for(
            lambda: t1.status == tm.STATUS_RUNNING and t2.status == tm.STATUS_RUNNING,
            what="t1/t2 进入 RUNNING",
        )
        assert t3.status == tm.STATUS_QUEUED, f"t3 应排队,实际 {t3.status}"

        await mgr.cancel(t1.id)  # 等待完整收尾后才返回
        assert t1.status == tm.STATUS_CANCELLED, "取消端点返回时任务应为终态"
        assert sig_state["calls"], "取消运行中任务应调用 killpg 终止进程组"
        assert all(sig == signal.SIGKILL for _, sig in sig_state["calls"]), "应发送 SIGKILL"
        assert not t1.workdir.exists(), "取消后暂存目录应被清理"
        assert t1.result is None, "取消任务不得发布结果"
        assert t1.public()["download_url"] is None

        await wait_for(lambda: t2.status == tm.STATUS_DONE, what="t2 完成")
        await wait_for(lambda: t3.status == tm.STATUS_DONE, what="t3 补位完成")
        assert t2.result["width"] == 120 and t2.result["height"] == 80
        assert t2.result["extent"] == [100.0, 40.0, 340.0, 200.0], t2.result["extent"]
        assert t2.result["crs"] == "EPSG:3857"
        assert (t2.workdir / "result.tif").exists(), "成功任务应保留结果文件"

        # --- 场景 2:取消排队任务,立即终态并清理,不占用并发槽 ---
        os.environ["COG_SLOW"] = "1"    # 后续任务会停在 COG 阶段
        s1 = mk("00.tif")
        s2 = mk("00.tif")
        q = mk("00.tif")
        await wait_for(
            lambda: s1.status == tm.STATUS_RUNNING and s2.status == tm.STATUS_RUNNING,
            what="s1/s2 进入 RUNNING",
        )
        assert q.status == tm.STATUS_QUEUED
        await mgr.cancel(q.id)
        assert q.status == tm.STATUS_CANCELLED, f"排队任务取消后应立即 CANCELLED,实际 {q.status}"
        assert not q.workdir.exists(), "排队任务取消后应清理暂存"

        # --- 场景 3:COG 阶段取消,不得发布结果;重复取消只做一次收尾 ---
        # 先确认 s1 已进入 COG 阶段,再记录其当前进程组(MOSAIC 与 COG 是不同进程组)
        await wait_for(lambda: s1.stage == tm.STAGE_COG and s1.pgid is not None,
                       what="s1 进入 COG 阶段")
        calls_before = len(sig_state["calls"])
        s1_pgid = s1.pgid
        cancel1 = asyncio.create_task(mgr.cancel(s1.id))
        cancel2 = asyncio.create_task(mgr.cancel(s1.id))  # 重复取消
        await asyncio.gather(cancel1, cancel2)
        await wait_for(lambda: s1.status == tm.STATUS_CANCELLED, what="s1 变为 CANCELLED")
        assert s1.result is None and not s1.workdir.exists()
        # 两次取消等待的是同一个收尾 Future
        assert s1.teardown_future is not None and s1.teardown_future.done()
        # 同一次收尾只对该进程组发一次整组 SIGKILL(其余为对已回收 pid 的幂等空操作)
        group_sigs = [c for c in sig_state["calls"][calls_before:] if c[0] == s1_pgid]
        assert len(group_sigs) == 1 and group_sigs[0][1] == signal.SIGKILL, group_sigs
        # 再取消已终态任务:直接返回,不产生新信号
        calls_terminal = len(sig_state["calls"])
        await mgr.cancel(s1.id)
        assert len(sig_state["calls"]) == calls_terminal
        await mgr.cancel(s2.id)
        await wait_for(lambda: s2.status == tm.STATUS_CANCELLED, what="s2 变为 CANCELLED")
        del os.environ["COG_SLOW"]

        # --- 场景 4:取消后不再启动新任务阶段,任务正常完成 ---
        f1 = mk("00.tif")
        await wait_for(lambda: f1.status == tm.STATUS_DONE, what="f1 完成")
        assert f1.result is not None

        # --- 场景 5:主进程先退 + 孙进程持有日志管道(hold) ---
        os.environ["CRASH"] = "1"
        os.environ["CRASH_PIPE"] = "hold"
        os.environ["PARENT_SLEEP"] = "0.4"
        sig_state["calls"].clear()
        sig_state["armed"] = False  # 挂起终止效果,制造"信号已发、孙进程仍在"的窗口
        h1 = mk("00.tif")
        # 等孙进程出现:此时父进程尚未退出
        await wait_for(lambda: (h1.workdir / "grandchild.pid").exists(),
                       what="hold: 孙进程已派生")
        grand_pid = int((h1.workdir / "grandchild.pid").read_text())
        pgid = os.getpgid(grand_pid)
        # 等父进程退出(rc!=0)、孙进程仍存活:日志管道被孙进程持有,EOF 不会到来
        await wait_for(
            lambda: grand_pid in tm.TaskManager._group_pids(pgid) and sig_state["calls"],
            what="hold: 父进程已退、调度器已发终止信号而孙进程仍存活",
        )
        assert h1.status == tm.STATUS_RUNNING, (
            f"管道不 EOF 且孙进程未回收时不得提前终态,实际 {h1.status}"
        )
        assert h1.workdir.exists(), "孙进程仍在工作时不得删除暂存"
        assert grand_pid in tm.TaskManager._group_pids(pgid), "孙进程应仍在同进程组"
        # 放行终止:哨兵出现后孙进程退出,调度器确认整组回收 -> 清暂存 -> 才报失败
        sig_state["armed"] = True
        for d in (tmp / "tasks").iterdir():
            if d.is_dir():
                for c in list(sig_state["calls"]):
                    (d / f"KILL-{c[0]}").write_text("1")
        await wait_for(lambda: h1.status == tm.STATUS_FAILED, what="hold: h1 变为 FAILED")
        assert not h1.workdir.exists(), "失败收尾后暂存应已清理"
        assert tm.TaskManager._group_pids(pgid) == [], "同组进程应全部回收,无残留"
        assert h1.cleanup and h1.cleanup["group_pids_stopped"] >= 1, (
            "应记录主动终止的同组残留进程数"
        )
        assert all(sig == signal.SIGKILL for _, sig in sig_state["calls"])
        assert "同组残留" in (h1.error or ""), h1.error

        # --- 场景 6:主进程先退 + 孙进程关闭日志管道继续干活(close) ---
        os.environ["CRASH_PIPE"] = "close"
        sig_state["calls"].clear()
        sig_state["armed"] = False
        c1 = mk("00.tif")
        await wait_for(lambda: (c1.workdir / "grandchild.pid").exists(),
                       what="close: 孙进程已派生")
        grand_pid2 = int((c1.workdir / "grandchild.pid").read_text())
        pgid2 = os.getpgid(grand_pid2)
        # 管道已提前 EOF,但孙进程仍在工作:不得据 EOF 提前报失败/删暂存
        await wait_for(
            lambda: grand_pid2 in tm.TaskManager._group_pids(pgid2) and sig_state["calls"],
            what="close: 日志已 EOF、调度器在收尾而孙进程仍在干活",
        )
        assert c1.status == tm.STATUS_RUNNING, (
            f"日志提前 EOF 不得导致提前终态,实际 {c1.status}"
        )
        assert c1.workdir.exists() and (c1.workdir / "child_progress.log").exists()
        sig_state["armed"] = True
        for d in (tmp / "tasks").iterdir():
            if d.is_dir():
                for c in list(sig_state["calls"]):
                    (d / f"KILL-{c[0]}").write_text("1")
        await wait_for(lambda: c1.status == tm.STATUS_FAILED, what="close: c1 变为 FAILED")
        assert not c1.workdir.exists(), "孙进程回收后才允许清理暂存"
        assert tm.TaskManager._group_pids(pgid2) == [], "close 模式同组进程也应全部回收"
        assert c1.cleanup["group_pids_stopped"] >= 1

        # --- 场景 7:取消与主进程退出交错(hold 模式下父进程退出前取消) ---
        os.environ["CRASH_PIPE"] = "hold"
        os.environ["PARENT_SLEEP"] = "2.0"
        sig_state["armed"] = True
        x1 = mk("00.tif")
        await wait_for(lambda: x1.status == tm.STATUS_RUNNING, what="x1 进入 RUNNING")
        await mgr.cancel(x1.id)
        assert x1.status == tm.STATUS_CANCELLED, "交错取消应在收尾后为 CANCELLED"
        assert x1.result is None and x1.public()["download_url"] is None, "绝不发布迟到结果/开放下载"
        assert not x1.workdir.exists()

        # --- 场景 8:失败/取消收尾后槽位释放,另一任务照常运行 ---
        del os.environ["CRASH"]
        del os.environ["CRASH_PIPE"]
        del os.environ["PARENT_SLEEP"]
        n1 = mk("00.tif")
        await wait_for(lambda: n1.status == tm.STATUS_DONE, what="n1 补位完成")
        assert (n1.workdir / "result.tif").exists()

        print("ALL TASKMANAGER TESTS PASSED")
    finally:
        os.killpg = real_killpg
        os.kill = real_kill


if __name__ == "__main__":
    asyncio.run(main())
