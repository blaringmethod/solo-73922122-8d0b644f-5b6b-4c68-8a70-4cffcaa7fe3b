#!/usr/bin/env python3
"""MOSAIC 阶段的可控演示桩:复现 GDAL 主进程异常退出后的两种管道局面。

真实场景里 ``gdalwarp -multi`` 会派生同组工作进程。本桩用 fork 制造同样的
进程组结构,并通过 DEMO_MODE 控制子进程对日志管道(stdout)的处理:

- ``hold``(主进程先退,子进程**持有日志管道**):
  旧实现以管道 EOF 判定阶段结束 -> EOF 永不到来 -> 取消后任务仍显示运行中。
- ``close``(主进程先退,子进程**关闭日志管道继续干活**):
  旧实现会提前读到 EOF 判定失败 -> 在子进程仍工作时删除暂存。

修复后的调度器以主进程退出(returncode,由 SIGCHLD 置位)为准,主进程一退出就按
进程组 SIGKILL 残留、确认整组回收并清理暂存后才报终态,不依赖日志管道 EOF。

桩的最后一个参数是输出栅格路径(与真实 gdalwarp 一致),会写出占位文件,
使 MOSAIC 之后的 COG/METADATA 阶段在非故障路径下可继续。

环境变量:
  DEMO_MODE       hold | close(必填)
  DEMO_PARENT_SLEEP  主进程退出前工作秒数(默认 8),便于手动观察/取消
  DEMO_CHILD_RUN    子进程残留秒数(默认 300)
"""
import os
import sys
import time

OUT = sys.argv[-1]
MODE = os.environ.get("DEMO_MODE", "hold")
PARENT_SLEEP = float(os.environ.get("DEMO_PARENT_SLEEP", "8"))
CHILD_RUN = float(os.environ.get("DEMO_CHILD_RUN", "300"))


def log(msg: str) -> None:
    # 行缓冲,确保界面日志与管道占用行为可观察
    print(f"[gdalwarp-demo:{os.getpid()}] {msg}", flush=True)


def child_work(write_fd, out_path: str) -> None:
    """同组残留子进程:按模式决定持有还是关闭日志管道,并继续干活。"""
    if write_fd is not None:
        if MODE == "close":
            # 关闭从父进程继承的日志管道写端 -> 父进程退出后读端立即 EOF,
            # 但本进程继续工作(写暂存目录),复现"提前报失败并删除暂存"
            os.close(write_fd)
        # hold 模式:刻意保留 write_fd,管道写端不关闭 -> EOF 永不到来
    progress = os.path.join(os.path.dirname(out_path), "child_progress.log")
    deadline = time.monotonic() + CHILD_RUN
    tick = 0
    while time.monotonic() < deadline:
        tick += 1
        try:
            with open(progress, "a", encoding="utf-8") as f:
                f.write(
                    f"child pid={os.getpid()} pgid={os.getpgid(0)} "
                    f"tick={tick} mode={MODE} still-working\n"
                )
        except OSError:
            # 暂存目录被修复后的调度器清理后,写失败也继续存活,
            # 直到被同组 SIGKILL —— 用于证明残留进程确实被主动终止
            pass
        if MODE == "hold":
            try:
                os.write(write_fd, f"child pid={os.getpid()} still holding log pipe tick={tick}\n".encode())
            except OSError:
                pass
        time.sleep(0.5)
    os._exit(0)


def main() -> None:
    log(f"start mosaic stub mode={MODE} pgid={os.getpgid(0)}")
    # 复刻 gdalwarp 的多进程结构:子进程通过 fork 继承 stdout(fd 1)与进程组
    pid = os.fork()
    if pid == 0:
        # 子进程:stdout(fd 1)即调度器的日志管道写端
        child_work(1, OUT)
        return
    log(f"spawned same-group worker pid={pid}, parent working {PARENT_SLEEP}s ...")
    time.sleep(PARENT_SLEEP)
    # 写出占位输出(模拟已产出的镶嵌结果),随后主进程异常退出,
    # 既不 wait 也不杀子进程 —— 与故障现场一致
    try:
        with open(OUT, "wb") as f:
            f.write(b"DEMO-MOSAIC-PLACEHOLDER\n")
    except OSError:
        pass
    log("parent GDAL process exiting abnormally NOW, worker left behind")
    os._exit(1)


if __name__ == "__main__":
    main()
