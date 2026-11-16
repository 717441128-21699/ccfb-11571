"""独立启动迁移控制台（人工操作模式）。

用法:
  py app.py --data-dir run_demo --port 8765
然后浏览器打开 http://127.0.0.1:8765/
"""
from __future__ import annotations

import argparse
import signal
import time
from pathlib import Path

from shard_mig.console import ConsoleServer
from shard_mig.gateway import Gateway
from shard_mig.migrator import Migrator


def main() -> None:
    ap = argparse.ArgumentParser(description="分片在线扩容迁移控制台")
    ap.add_argument("--data-dir", default="run_demo")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()

    Path(args.data_dir).mkdir(parents=True, exist_ok=True)
    gw = Gateway(args.data_dir)
    mg = Migrator(gw)
    server = ConsoleServer(gw, mg, args.host, args.port)
    server.start()

    print(f"迁移控制台: http://{args.host}:{server.port}/")
    print(f"数据目录  : {Path(args.data_dir).resolve()}")
    print("Ctrl+C 退出")

    stop = False

    def _sig(*_):
        nonlocal stop
        stop = True

    try:
        signal.signal(signal.SIGINT, _sig)
    except ValueError:  # 非主线程环境
        pass
    try:
        while not stop:
            time.sleep(0.5)
    finally:
        server.stop()
        gw.close()


if __name__ == "__main__":
    main()
