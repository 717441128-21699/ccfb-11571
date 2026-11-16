"""端到端压测：模拟业务持续读写，跑完整条在线扩容迁移链路。

验收标准（全部满足才退出 0）：
  A. 最终新库数据与不迁移的对照库 control.db 逐行一致；
  B. 切换/回滚期间不可写窗口 < 3 秒；
  C. 任意时刻新库无跨分片重复手机号；
  D. 校验和比对最终全部段一致；
  E. 回滚后旧库与对照库一致（回滚期间新数据不丢）。

用法:
  py stress_test.py --data-dir run_demo --port 8765 --duration 25 --seed 10000
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from shard_mig.console import ConsoleServer
from shard_mig.gateway import (
    Gateway,
    PhoneConflictError,
    ReadOnlyError,
)
from shard_mig.migrator import (
    BACKFILL_DONE,
    BACKFILLING,
    ROLLED_BACK,
    SWITCHED,
    VERIFIED,
)


def http_post(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise RuntimeError(f"POST {url} -> {exc.code}: {detail}") from exc
    if not body.get("ok"):
        raise RuntimeError(body.get("error"))
    return body["data"]


def http_get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))["data"]


class StressTraffic:
    """多线程业务流量：新增 / 更新 / 删除 / 读，手机号随机绑定。"""

    def __init__(self, gw: Gateway, workers: int):
        self.gw = gw
        self.workers = workers
        self.stop = threading.Event()
        self.uid_lock = threading.Lock()
        self.live_uids: list[int] = []
        self.uid_counter = 0
        self.phone_counter = 0
        self.blocked_windows: list[tuple[float, float]] = []
        self._bw_lock = threading.Lock()
        self.stats = {
            "insert": 0, "update": 0, "delete": 0, "read": 0,
            "conflict": 0, "readonly_reject": 0,
        }
        self.threads: list[threading.Thread] = []

    def _next_phone(self) -> str:
        with self.uid_lock:
            self.phone_counter += 1
            return f"139{self.phone_counter:08d}"

    def _record_blocked(self, start: float, end: float) -> None:
        with self._bw_lock:
            self.blocked_windows.append((start, end))

    def _write_with_window(self, fn):
        """执行写操作；只读期重试。

        不可写窗口按 ReadOnlyError 实际发生的时间戳记录
        （首次拒绝 ~ 末次拒绝），不把锁等待/线程调度时间算进去。
        """
        first_reject = None
        last_reject = None
        while True:
            try:
                return fn()
            except ReadOnlyError:
                now = time.monotonic()
                if first_reject is None:
                    first_reject = now
                last_reject = now
                self.stats["readonly_reject"] += 1
                self._record_blocked(first_reject, last_reject)
                # 密集重试，确保窗口边界被精确采样
                time.sleep(0.002)

    def _worker_loop(self, wid: int) -> None:
        rnd_step = (wid * 2654435761) & 0xFFFFFFFF
        counter = 0
        while not self.stop.is_set():
            counter += 1
            rnd_step = (rnd_step * 1103515245 + 12345) & 0xFFFFFFFF
            op = rnd_step % 10
            try:
                if op < 3:  # 新增 30%
                    with self.uid_lock:
                        self.uid_counter += 1
                        uid = self.uid_counter
                    phone = self._next_phone()
                    self._write_with_window(
                        lambda u=uid, p=phone: self.gw.upsert_user(
                            u, p, f"user_{u}"
                        )
                    )
                    with self.uid_lock:
                        self.live_uids.append(uid)
                    self.stats["insert"] += 1
                elif op < 7:  # 更新 40%
                    with self.uid_lock:
                        if not self.live_uids:
                            continue
                        uid = self.live_uids[rnd_step % len(self.live_uids)]
                    # 更新姓名为主；少量换手机号（新号，天然不冲突）
                    if rnd_step % 5 == 0:
                        phone = self._next_phone()
                        self._write_with_window(
                            lambda u=uid, p=phone: self.gw.upsert_user(
                                u, p, f"user_{u}_v{counter}"
                            )
                        )
                    else:
                        row = self.gw.get_user(uid)
                        if row is None:
                            continue
                        self._write_with_window(
                            lambda u=uid, p=row["phone"], c=counter:
                            self.gw.upsert_user(u, p, f"user_{u}_v{c}")
                        )
                    self.stats["update"] += 1
                elif op < 8:  # 删除 10%
                    with self.uid_lock:
                        if not self.live_uids:
                            continue
                        idx = rnd_step % len(self.live_uids)
                        uid = self.live_uids.pop(idx)
                    deleted = self._write_with_window(
                        lambda u=uid: self.gw.delete_user(u)
                    )
                    if not deleted:
                        with self.uid_lock:
                            self.live_uids.append(uid)
                    self.stats["delete"] += 1
                else:  # 读 20%
                    with self.uid_lock:
                        if not self.live_uids:
                            continue
                        uid = self.live_uids[rnd_step % len(self.live_uids)]
                    self.gw.get_user(uid)
                    self.stats["read"] += 1
            except PhoneConflictError:
                self.stats["conflict"] += 1
            except Exception as exc:
                print(f"[worker{wid}] 未预期异常: {exc!r}")
                raise

    def seed(self, n: int) -> None:
        for uid in range(1, n + 1):
            self.uid_counter = uid
            self.phone_counter = uid
            self.gw.upsert_user(uid, f"139{uid:08d}", f"user_{uid}")
            self.live_uids.append(uid)

    def start(self) -> None:
        for wid in range(self.workers):
            t = threading.Thread(
                target=self._worker_loop, args=(wid,), name=f"biz-{wid}", daemon=True
            )
            t.start()
            self.threads.append(t)

    def join(self) -> None:
        for t in self.threads:
            t.join(timeout=10)

    def merged_blocked_windows(self) -> list[tuple[float, float]]:
        """把各线程记录的拒绝区间合并成全局不可写窗口。

        每个窗口记录的是 [首次拒绝, 末次拒绝]；只合并时间上紧邻
        （间隔 ≤ 50ms，即同一只读事件被不同线程采样）的区间。
        """
        raw = sorted(self.blocked_windows)
        merged: list[tuple[float, float]] = []
        for a, b in raw:
            if merged and a <= merged[-1][1] + 0.05:
                merged[-1] = (merged[-1][0], max(merged[-1][1], b))
            else:
                merged.append((a, b))
        return merged


def wait_phase(status_url: str, phases: set[str], timeout: float = 120) -> dict:
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = http_get(status_url)
        if last["phase"] in phases:
            return last
        if last["phase"] == "FAILED":
            raise RuntimeError(f"迁移失败: {last.get('error')}")
        time.sleep(0.2)
    raise TimeoutError(f"等待阶段 {phases} 超时，当前 {last.get('phase')}")


def rows_by_uid(gw: Gateway, cluster) -> dict:
    out = {}
    for conn in cluster.all_conns():
        for row in conn.execute(
            "SELECT id, phone, name, gseq, updated_at FROM users"
        ):
            out[row["id"]] = tuple(row[k] for k in
                                   ("id", "phone", "name", "gseq", "updated_at"))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="在线分片扩容迁移端到端压测")
    ap.add_argument("--data-dir", default="run_demo")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--seed", type=int, default=10000, help="预置存量用户数")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--duration", type=float, default=25,
                    help="切换前让流量跑多少秒，保证搬运期有持续读写")
    ap.add_argument("--rate-limit", type=int, default=3000)
    ap.add_argument("--readonly-hold", type=float, default=1.0,
                    help="只读窗口注入保持秒数，用于确定性测量不可写时间（<3s）")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if data_dir.exists():
        for p in sorted(data_dir.rglob("*"), reverse=True):
            if p.is_file():
                p.unlink()
    data_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(" 在线分片扩容迁移压测：4 分片 -> 16 分片，业务全程不停")
    print("=" * 68)

    gw = Gateway(str(data_dir))
    mg = gw and __import__("shard_mig.migrator", fromlist=["Migrator"]).Migrator(gw)
    mg.set_rate(args.rate_limit)
    mg.readonly_hold_sec = args.readonly_hold
    server = ConsoleServer(gw, mg, "127.0.0.1", args.port)
    server.start()
    base = f"http://127.0.0.1:{server.port}"
    print(f"[1/9] 控制台已启动: {base}/  数据目录: {data_dir}")

    print(f"[2/9] 预置存量 {args.seed} 个用户（直接写旧分片 + 对照库）")
    traffic = StressTraffic(gw, args.workers)
    t_seed = time.monotonic()
    traffic.seed(args.seed)
    print(f"      预置完成，用时 {time.monotonic() - t_seed:.1f}s，"
          f"对照库 {gw.control_count()} 行")

    print("[3/9] 启动持续业务读写流量 "
          f"({args.workers} 线程：30% 增 / 40% 改 / 10% 删 / 20% 读)")
    traffic.start()

    print("[4/9] 启动迁移：存量分批搬运 + 变更日志增量追平（限速 "
          f"{args.rate_limit} 行/秒）")
    http_post(base + "/api/action", {"action": "start"})
    time.sleep(args.duration)

    st = wait_phase(base + "/api/status", {BACKFILL_DONE})
    print(f"      存量搬完: {st['backfill']['rows_copied']} 行，"
          f"追平 {st['catchup']['replayed_rows']} 行，lag={st['catchup']['lag']}")

    print("[5/9] 按主键分段计算校验和，逐段比对，不一致段自动重搬")
    verify_result = http_post(base + "/api/action", {"action": "verify"})
    print(f"      校验结果: {verify_result}")

    print("[6/9] 切换到新分片（短暂只读，秒级完成）")
    sw = http_post(base + "/api/action", {"action": "switch"})
    print(f"      切换结果: {sw}")
    wait_phase(base + "/api/status", {SWITCHED})

    print("[7/9] 切换后继续跑流量，然后一键回滚到旧分片")
    time.sleep(5)
    rb = http_post(base + "/api/action", {"action": "rollback"})
    print(f"      回滚结果: {rb}")
    wait_phase(base + "/api/status", {ROLLED_BACK})

    # 回滚后旧库已重新承载写入；先停流量再比对，避免业务在途写入造成竞态
    traffic.stop.set()
    traffic.join()
    time.sleep(0.5)

    # 验收 E：回滚后旧库与对照库逐行比对（回滚期间新数据不丢）
    old_rows = rows_by_uid(gw, gw.old)
    control_rows = {r["id"]: tuple(r[k] for k in
                    ("id", "phone", "name", "gseq", "updated_at"))
                    for r in gw.control_rows()}
    rollback_match = old_rows == control_rows
    print(f"      回滚后旧库 vs 对照库逐行一致: {rollback_match} "
          f"({len(old_rows)}/{len(control_rows)} 行)")

    print("[8/9] 回滚后重新切换到新分片（验证可重复切换）")
    rs = http_post(base + "/api/action", {"action": "reswitch"})
    print(f"      重新切换结果: {rs}")
    wait_phase(base + "/api/status", {SWITCHED, VERIFIED})

    print("[9/9] 做最终验收")
    # 等追平/校验状态稳定
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        st = http_get(base + "/api/status")
        if st["catchup"]["lag"] == 0 and st["phase"] == SWITCHED:
            break
        time.sleep(0.3)

    # ---- 验收 A：新库 vs 对照库逐行 ----
    new_rows = rows_by_uid(gw, gw.new)
    final_match = new_rows == control_rows

    # ---- 验收 C：新库手机号唯一（分片内 UNIQUE + 跨分片检查）----
    phones = []
    for conn in gw.new.all_conns():
        phones.extend(r["phone"] for r in conn.execute("SELECT phone FROM users"))
    phone_unique = len(phones) == len(set(phones))
    phone_vs_control = len(phones) == gw.control_phone_count()

    # ---- 验收 B：不可写窗口 ----
    windows = traffic.merged_blocked_windows()
    window_secs = [round(b - a, 4) for a, b in windows]
    max_window = max(window_secs, default=0.0)
    downtime_ok = max_window < 3.0

    # ---- 验收 D：校验段状态 ----
    seg_total = st["verify"]["segments_total"]
    seg_bad = st["verify"]["segments_mismatch"]

    print("\n" + "=" * 68)
    print(" 验收结果")
    print("=" * 68)
    print(f" 业务流量统计      : {json.dumps(traffic.stats, ensure_ascii=False)}")
    print(f" 网关统计          : {json.dumps(gw.stats_snapshot(), ensure_ascii=False)}")
    print(f" 不可写窗口(秒)    : {window_secs}  最大 {max_window}s "
          f"[要求 < 3s] -> {'PASS' if downtime_ok else 'FAIL'}")
    print(f" 切换只读窗口      : 首次 {sw['readonly_window_sec']}s / "
          f"回滚 {rb['readonly_window_sec']}s / 重切 {rs['readonly_window_sec']}s")
    print(f" 校验分段          : 共 {seg_total} 段，不一致 {seg_bad} 段，"
          f"修复轮次 {st['verify']['rounds']}")
    print(f" 手机号唯一(新库)  : {phone_unique}  与对照库一致: {phone_vs_control} "
          f"-> {'PASS' if phone_unique and phone_vs_control else 'FAIL'}")
    print(f" 回滚后旧库一致    : -> {'PASS' if rollback_match else 'FAIL'}")
    print(f" 新库行数 {len(new_rows)} / 对照库行数 {len(control_rows)}")
    if not final_match:
        diff_ids = list(set(new_rows) ^ set(control_rows))[:10]
        sample = []
        for uid in diff_ids:
            sample.append({"uid": uid,
                           "new": new_rows.get(uid), "control": control_rows.get(uid)})
        print(f" 不一致样本(最多10): {json.dumps(sample, ensure_ascii=False)}")
    print(f" 最终逐行一致      : -> {'PASS' if final_match else 'FAIL'}")
    print("=" * 68)

    ok = all([
        final_match, downtime_ok, phone_unique, phone_vs_control,
        rollback_match, seg_bad == 0 and seg_total > 0,
    ])
    print(f"\n总判定: {'ALL PASS ✅' if ok else 'FAIL ❌'}")
    print(f"控制台仍可查看: {base}/ （回车退出）")
    try:
        input()
    except EOFError:
        pass
    server.stop()
    gw.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
