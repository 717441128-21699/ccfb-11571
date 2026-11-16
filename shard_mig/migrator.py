"""迁移引擎：搬运 / 追平 / 校验 / 切换 / 回滚 的状态机与后台线程。

阶段（phase）：
  IDLE          未启动
  BACKFILLING   存量分批搬运（可暂停/限速）
  BACKFILL_DONE 存量搬完，后台继续追增量
  VERIFIED      校验和全部一致
  SWITCHED      已切到新分片（读写走新规则）
  ROLLBACK      回滚中（只读 + 反向追平）
  ROLLED_BACK   已回滚到旧分片
  FAILED        出现错误，需要人工查看事件日志
"""
from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass

from .gateway import (
    Gateway,
    MODE_NEW,
    MODE_OLD,
    MODE_READONLY,
    ReadOnlyError,
)
from .sharding import NEW_SHARD_COUNT, OLD_SHARD_COUNT, expansion_map

IDLE = "IDLE"
BACKFILLING = "BACKFILLING"
BACKFILL_DONE = "BACKFILL_DONE"
VERIFIED = "VERIFIED"
SWITCHED = "SWITCHED"
ROLLBACK = "ROLLBACK"
ROLLED_BACK = "ROLLED_BACK"
FAILED = "FAILED"

DEFAULT_BATCH_SIZE = 500
DEFAULT_RATE_LIMIT = 2000          # 行/秒，0 表示不限速
DEFAULT_SEGMENT_ROWS = 1000        # 校验和每段主键行数
MAX_REPAIR_ROUNDS = 4


class RateLimiter:
    """简单令牌桶：保证搬运平均速率不超过 rows_per_sec。"""

    def __init__(self, rows_per_sec: int):
        self.rows_per_sec = max(0, rows_per_sec)
        self._allowed = 0.0
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def set_rate(self, rows_per_sec: int) -> None:
        with self._lock:
            self.rows_per_sec = max(0, rows_per_sec)

    def acquire(self, n: int = 1) -> None:
        with self._lock:
            rps = self.rows_per_sec
            if rps <= 0:
                return
            now = time.monotonic()
            self._allowed = min(
                float(rps), self._allowed + (now - self._last) * rps
            )
            self._last = now
            wait = 0.0
            while self._allowed < n:
                need = n - self._allowed
                wait += need / rps
                self._allowed += need
            self._allowed -= n
        if wait > 0:
            time.sleep(wait)


@dataclass
class BackfillPos:
    old_shard: int
    last_id: int = 0
    rows_copied: int = 0


@dataclass
class Segment:
    index: int
    id_start: int
    id_end: int
    rows: int
    old_hash: str
    new_hash: str

    @property
    def match(self) -> bool:
        return self.old_hash == self.new_hash


class Migrator:
    def __init__(self, gateway: Gateway):
        self.gw = gateway
        self.phase = IDLE
        self.plan = {
            "old_shard_count": OLD_SHARD_COUNT,
            "new_shard_count": NEW_SHARD_COUNT,
            "expansion_map": expansion_map(),
            "batch_size": DEFAULT_BATCH_SIZE,
            "rate_limit_rows_per_sec": DEFAULT_RATE_LIMIT,
            "segment_rows": DEFAULT_SEGMENT_ROWS,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.limiter = RateLimiter(DEFAULT_RATE_LIMIT)
        # 只读窗口额外保持秒数：默认 0（真实切换毫秒级）；压测可调大，
        # 让业务线程确定性地采样到只读拒绝，从而验证不可写时长 < 3s
        self.readonly_hold_sec = 0.0
        self.positions: list[BackfillPos] = [
            BackfillPos(old_shard=i) for i in range(OLD_SHARD_COUNT)
        ]
        self.backfill_rows = 0
        self.replayed_rows = 0
        self.rollback_rows = 0
        self.last_catchup_gseq = 0
        self.last_rollback_gseq = 0
        self.last_verify_at: str | None = None
        self.verify_rounds = 0
        self.segments: list[Segment] = []
        self.repaired_segments = 0
        self.events: list[dict] = []
        self.error: str | None = None

        self._pause = threading.Event()        # set = 运行中
        self._pause.set()
        self._stop = threading.Event()          # 控制搬运线程
        self._catchup_stop = threading.Event()  # 独立控制追平线程
        self._threads: list[threading.Thread] = []
        self._state_lock = threading.RLock()

    # ---------------- 事件与状态 ----------------
    def log(self, message: str) -> None:
        evt = {
            "ts": time.strftime("%H:%M:%S"),
            "phase": self.phase,
            "message": message,
        }
        with self._state_lock:
            self.events.append(evt)
            self.events = self.events[-300:]

    def _set_phase(self, phase: str) -> None:
        with self._state_lock:
            self.phase = phase
        self.log(f"阶段 -> {phase}")

    def _fail(self, exc: Exception) -> None:
        import traceback

        tb = traceback.format_exc()
        with self._state_lock:
            self.phase = FAILED
            self.error = repr(exc)
        self.log(f"失败: {exc!r}\n{tb}")

    def wait_running(self) -> bool:
        """暂停点；返回 False 表示要求线程退出。"""
        while not self._pause.is_set():
            if self._stop.is_set():
                return False
            time.sleep(0.05)
        return not self._stop.is_set()

    def pause(self) -> None:
        self._pause.clear()
        self.log("已暂停")

    def resume(self) -> None:
        self._pause.set()
        self.log("已恢复")

    def set_rate(self, rows_per_sec: int) -> None:
        self.limiter.set_rate(rows_per_sec)
        self.plan["rate_limit_rows_per_sec"] = rows_per_sec
        self.log(f"限速调整为 {rows_per_sec} 行/秒")

    # ---------------- 启动 ----------------
    def start(self) -> None:
        if self.phase not in (IDLE, FAILED):
            raise RuntimeError(f"当前阶段 {self.phase} 不能启动迁移")
        self.error = None
        self._stop.clear()
        self._pause.set()
        self._set_phase(BACKFILLING)
        t1 = threading.Thread(
            target=self._backfill_loop, name="backfill", daemon=True
        )
        self._threads = [t1]
        t1.start()
        self._start_catchup_thread()
        self.log("迁移后台线程已启动（搬运 + 追增量）")

    # ---------------- 存量搬运 ----------------
    def _backfill_loop(self) -> None:
        try:
            while self._stop.is_set() is False:
                if not self.wait_running():
                    return
                progressed = False
                for pos in self.positions:
                    if not self.wait_running():
                        return
                    with self.gw.old.read_lock(pos.old_shard) as conn:
                        rows = conn.execute(
                            "SELECT id, phone, name, gseq, updated_at FROM users "
                            "WHERE id > ? ORDER BY id LIMIT ?",
                            (pos.last_id, self.plan["batch_size"]),
                        ).fetchall()
                    if not rows:
                        continue
                    progressed = True
                    self.limiter.acquire(len(rows))
                    for r in rows:
                        new_shard = r["id"] % NEW_SHARD_COUNT
                        # 版本守护：已被追平线程写入过更新版本则不覆盖；不产生日志
                        self.gw.new.replay_upsert(
                            new_shard, r["id"], r["phone"], r["name"],
                            r["gseq"], r["updated_at"],
                        )
                        pos.last_id = r["id"]
                        pos.rows_copied += 1
                        self.backfill_rows += 1
                if not progressed:
                    self._set_phase(BACKFILL_DONE)
                    self.log(
                        f"存量搬运完成，共 {self.backfill_rows} 行；"
                        "追增量线程继续运行"
                    )
                    return
        except Exception as exc:  # pragma: no cover - 防御
            self._fail(exc)

    # ---------------- 增量追平 ----------------
    def _fetch_changelog_batch(self, after_gseq: int, limit: int):
        """从旧分片 changelog 按 gseq 归并读取一批。"""
        per_shard = []
        for i in range(OLD_SHARD_COUNT):
            with self.gw.old.read_lock(i) as conn:
                rows = conn.execute(
                    "SELECT gseq, op, id, phone, name, updated_at FROM changelog "
                    "WHERE gseq > ? ORDER BY gseq LIMIT ?",
                    (after_gseq, limit),
                ).fetchall()
            # 次级键用分片号：gseq 全局唯一，正常不会用到，仅用于杜绝 Row 比较
            per_shard.append(iter((r, i) for r in rows))
        import heapq

        for r, _shard in heapq.merge(*per_shard, key=lambda t: (t[0]["gseq"], t[1])):
            yield r

    def _apply_change(self, row) -> None:
        uid = row["id"]
        new_shard = uid % NEW_SHARD_COUNT
        if row["op"] == "UPSERT":
            self.gw.new.replay_upsert(
                new_shard, uid, row["phone"], row["name"],
                row["gseq"], row["updated_at"],
            )
        else:  # DELETE
            self.gw.new.replay_delete(new_shard, uid, row["gseq"])

    def _catchup_wait(self) -> bool:
        """追平线程的暂停/停止检查（独立于搬运线程的 _stop）。"""
        while not self._pause.is_set():
            if self._catchup_stop.is_set():
                return False
            time.sleep(0.05)
        return not self._catchup_stop.is_set()

    def _catchup_loop(self) -> None:
        try:
            while not self._catchup_stop.is_set():
                if not self._catchup_wait():
                    return
                merged = self._fetch_changelog_batch(
                    self.last_catchup_gseq, self.plan["batch_size"]
                )
                n = 0
                last_gseq = self.last_catchup_gseq
                for row in merged:
                    if not self._catchup_wait():
                        return
                    self._apply_change(row)
                    last_gseq = row["gseq"]
                    n += 1
                if n:
                    self.last_catchup_gseq = last_gseq
                    self.replayed_rows += n
                else:
                    # 切换完成后网关不再写旧库，追平线程自然空转，等待停止信号
                    time.sleep(0.05)
        except Exception as exc:  # pragma: no cover - 防御
            self._fail(exc)

    def _start_catchup_thread(self) -> None:
        self._catchup_stop.clear()
        t = threading.Thread(
            target=self._catchup_loop, name="catchup", daemon=True
        )
        self._threads.append(t)
        t.start()

    def _stop_catchup_thread(self) -> None:
        self._catchup_stop.set()
        # 等待追平线程真正退出，避免它与本流程的 drain/repair 并发写新库
        for t in list(self._threads):
            if t.name == "catchup" and t.is_alive():
                t.join(timeout=10)
        self._threads = [t for t in self._threads if t.is_alive()]

    def catchup_lag(self) -> int:
        """旧库最新 gseq 与已追平 gseq 的差值。"""
        latest = 0
        for i in range(OLD_SHARD_COUNT):
            with self.gw.old.read_lock(i) as conn:
                row = conn.execute(
                    "SELECT MAX(gseq) AS m FROM changelog"
                ).fetchone()
            if row["m"]:
                latest = max(latest, row["m"])
        return max(0, latest - self.last_catchup_gseq)

    def drain_catchup(self, max_wait: float = 30.0) -> None:
        """切换前在调用线程里把增量追干净。"""
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            merged = self._fetch_changelog_batch(
                self.last_catchup_gseq, self.plan["batch_size"]
            )
            n = 0
            last_gseq = self.last_catchup_gseq
            for row in merged:
                self._apply_change(row)
                last_gseq = row["gseq"]
                n += 1
            if n:
                self.last_catchup_gseq = last_gseq
                self.replayed_rows += n
            if self.catchup_lag() == 0:
                return
            time.sleep(0.02)
        raise TimeoutError("增量追平在限定时间内未收敛")

    # ---------------- 校验和 ----------------
    @staticmethod
    def _canonical(row) -> bytes:
        return "\x1f".join(
            str(row[k]) for k in ("id", "phone", "name", "gseq", "updated_at")
        ).encode("utf-8")

    def _cluster_segment_rows(self, cluster, id_start: int, id_end: int):
        """跨分片按主键区间拉全量行，再按 id 归并。"""
        import heapq

        iters = []
        for shard, conn in enumerate(cluster.all_conns()):
            with cluster.read_lock(shard) as locked:
                rows = locked.execute(
                    "SELECT id, phone, name, gseq, updated_at FROM users "
                    "WHERE id >= ? AND id <= ? ORDER BY id",
                    (id_start, id_end),
                ).fetchall()
            iters.append(iter((r, shard) for r in rows))
        for r, _shard in heapq.merge(*iters, key=lambda t: (t[0]["id"], t[1])):
            yield r

    def _segment_hash(self, cluster, id_start: int, id_end: int):
        digest = hashlib.sha256()
        count = 0
        for row in self._cluster_segment_rows(cluster, id_start, id_end):
            digest.update(self._canonical(row))
            digest.update(b"\x1e")
            count += 1
        return count, digest.hexdigest()

    def _all_id_bounds(self) -> tuple[int, int]:
        lo = hi = 0
        for cluster in (self.gw.old, self.gw.new):
            for shard, _conn in enumerate(cluster.all_conns()):
                with cluster.read_lock(shard) as conn:
                    row = conn.execute(
                        "SELECT MIN(id) AS lo, MAX(id) AS hi FROM users"
                    ).fetchone()
                if row["lo"] is None:
                    continue
                lo = min(lo or row["lo"], row["lo"])
                hi = max(hi, row["hi"])
        return lo, hi

    def _build_boundaries(self) -> list[tuple[int, int]]:
        """用旧库全量有序 id 切分段边界（每 segment_rows 个主键一段）。"""
        import heapq

        id_iters = []
        for shard, _conn in enumerate(self.gw.old.all_conns()):
            with self.gw.old.read_lock(shard) as locked:
                id_rows = locked.execute(
                    "SELECT id FROM users ORDER BY id"
                ).fetchall()
            id_iters.append(iter((r, shard) for r in id_rows))
        ordered_ids = [
            r["id"]
            for r, _s in heapq.merge(*id_iters, key=lambda t: (t[0]["id"], t[1]))
        ]
        boundaries: list[tuple[int, int]] = []
        segment_rows = self.plan["segment_rows"]
        for i in range(0, len(ordered_ids), segment_rows):
            chunk = ordered_ids[i : i + segment_rows]
            boundaries.append((chunk[0], chunk[-1]))
        return boundaries

    def _repair_mismatched_segments(self, boundaries) -> int:
        """重搬所有不一致段，返回本轮不一致段数。"""
        mismatches = 0
        for idx, (a, b) in enumerate(boundaries):
            old_n, old_hash = self._segment_hash(self.gw.old, a, b)
            new_n, new_hash = self._segment_hash(self.gw.new, a, b)
            seg = Segment(idx, a, b, max(old_n, new_n), old_hash, new_hash)
            self.segments.append(seg)
            if seg.match:
                continue
            mismatches += 1
            # 旧库该段的权威行集合
            old_ids = set()
            for old_shard in range(OLD_SHARD_COUNT):
                with self.gw.old.read_lock(old_shard) as oc:
                    rows = oc.execute(
                        "SELECT id, phone, name, gseq, updated_at FROM users "
                        "WHERE id >= ? AND id <= ? ORDER BY id",
                        (a, b),
                    ).fetchall()
                for r in rows:
                    old_ids.add(r["id"])
                    new_shard = r["id"] % NEW_SHARD_COUNT
                    self.gw.new.replay_upsert(
                        new_shard, r["id"], r["phone"], r["name"],
                        r["gseq"], r["updated_at"],
                    )
            # 新库该段多出来的行（旧库已删除）：按段清理，使其与旧库一致
            for new_shard in range(NEW_SHARD_COUNT):
                with self.gw.new.read_lock(new_shard) as nc:
                    extra = nc.execute(
                        "SELECT id FROM users WHERE id >= ? AND id <= ?",
                        (a, b),
                    ).fetchall()
                for x in extra:
                    if x["id"] not in old_ids:
                        # 旧库已无此行，物理删除新库对应残留
                        self.gw.new.force_delete(new_shard, x["id"])
            self.repaired_segments += 1
        return mismatches

    def verify_checksum(self) -> dict:
        """在线预检：在线流量仍在写，分段比对 + 重搬，尽力收敛但不硬失败。

        在线期间新旧库永远存在在途写入差异，少量不一致属正常，
        最终的强制全绿校验在切换只读窗口内由 final_verify 完成。
        """
        if self.phase == BACKFILLING:
            deadline = time.monotonic() + 60
            while self.phase == BACKFILLING and time.monotonic() < deadline:
                time.sleep(0.1)
        if self.phase not in (BACKFILL_DONE, VERIFIED, ROLLED_BACK):
            raise RuntimeError(f"阶段 {self.phase} 不允许校验")
        self.log("在线预检：分段校验和比对 + 重搬（容忍在途写入差异）")
        boundaries = self._build_boundaries()
        if not boundaries:
            self.segments = []
            self._set_phase(VERIFIED)
            return {"segments": 0, "mismatches": 0, "rounds": 0, "online": True}

        total_repaired = 0
        last_mismatches = 0
        for rounds in range(1, MAX_REPAIR_ROUNDS + 1):
            self.segments = []
            self.last_verify_at = time.strftime("%H:%M:%S")
            self.verify_rounds = rounds
            last_mismatches = self._repair_mismatched_segments(boundaries)
            total_repaired += last_mismatches
            self.log(
                f"预检第 {rounds} 轮：{len(boundaries)} 段，不一致 {last_mismatches} 段"
            )
            self.drain_catchup()
            if last_mismatches == 0:
                break
        # 在线预检不强制全绿；能进入 VERIFIED 即可发起切换，最终校验在只读窗口
        self._set_phase(VERIFIED)
        return {
            "segments": len(boundaries),
            "mismatches": last_mismatches,
            "rounds": self.verify_rounds,
            "repaired": total_repaired,
            "online": True,
            "note": "在线预检，残留不一致将在切换只读窗口内强制收敛",
        }

    def final_verify(self) -> dict:
        """只读窗口内的最终校验：无新写入，循环重搬直到全部段一致。"""
        boundaries = self._build_boundaries()
        if not boundaries:
            self.segments = []
            return {"segments": 0, "mismatches": 0, "rounds": 0}
        rounds = 0
        while True:
            rounds += 1
            self.segments = []
            self.last_verify_at = time.strftime("%H:%M:%S")
            mismatches = self._repair_mismatched_segments(boundaries)
            self.log(
                f"最终校验第 {rounds} 轮：{len(boundaries)} 段，不一致 {mismatches} 段"
            )
            if mismatches == 0:
                return {"segments": len(boundaries), "mismatches": 0,
                        "rounds": rounds}
            if rounds > MAX_REPAIR_ROUNDS + 4:
                raise RuntimeError("只读窗口内最终校验仍无法收敛")
            self.drain_catchup()

    def duplicate_phones_in_new(self) -> list[str]:
        """新库跨分片手机号重复检查（迁移中任何时刻都不允许有重复）。"""
        seen: set[str] = set()
        dupes: list[str] = []
        for shard, _conn in enumerate(self.gw.new.all_conns()):
            with self.gw.new.read_lock(shard) as conn:
                phones = [r["phone"] for r in conn.execute(
                    "SELECT DISTINCT phone FROM users"
                ).fetchall()]
            for phone in phones:
                if phone in seen:
                    dupes.append(phone)
                seen.add(phone)
        return dupes

    # ---------------- 切换 ----------------
    def switch_to_new(self) -> dict:
        if self.phase != VERIFIED:
            raise RuntimeError(f"阶段 {self.phase}，必须先通过校验才能切换")
        t0 = time.monotonic()
        # 1) 锁内瞬间进入只读，读仍走旧库
        with self.gw.write_lock():
            self.gw.switch_mode(MODE_READONLY, ro_target=MODE_OLD)
        t_readonly = time.monotonic()
        self.log("进入只读窗口，最终追平增量")
        # 停掉后台追平线程：只读窗口内由本流程独占 drain/repair，
        # 避免两个应用者交错推进新库水位，导致校验和无法收敛
        self._stop_catchup_thread()
        # 只读期不持写锁：业务写立即收到 ReadOnlyError，读不受影响
        if self.readonly_hold_sec:
            time.sleep(self.readonly_hold_sec)

        # 2) 追平最后一小段增量（本线程独占）
        self.drain_catchup()
        lag = self.catchup_lag()

        # 3) 只读窗口内无新写入，最终校验和强制收敛（不一致段重搬到全绿）
        fv = self.final_verify()
        self.log(f"只读窗口最终校验通过：{fv['segments']} 段，{fv['rounds']} 轮")

        # 4) 清空回滚日志：切换后网关在线写新库会自动记录 rollback_log
        self.gw.new.reset_rollback_log()

        # 5) 手机号唯一性终检（跨分片全局扫描）
        dupes = self.duplicate_phones_in_new()
        if dupes:
            with self.gw.write_lock():
                self.gw.switch_mode(MODE_OLD)
            raise RuntimeError(f"新库存在重复手机号: {dupes[:5]}")

        # 6) 干净才装分片内唯一索引，承载切换后的在线写入约束
        self.gw.new.install_phone_unique_index()

        # 7) 锁内瞬间切到新规则，恢复读写
        with self.gw.write_lock():
            self.gw.switch_mode(MODE_NEW)
        t_done = time.monotonic()
        result = {
            "readonly_window_sec": round(t_done - t_readonly, 4),
            "total_switch_sec": round(t_done - t0, 4),
            "remaining_lag": lag,
        }
        self._set_phase(SWITCHED)
        self.log(f"切换完成：只读窗口 {result['readonly_window_sec']}s")
        return result

    # ---------------- 回滚 ----------------
    def _fetch_rollback_batch(self, after_gseq: int, limit: int):
        import heapq

        iters = []
        for shard, _conn in enumerate(self.gw.new.all_conns()):
            with self.gw.new.read_lock(shard) as conn:
                rows = conn.execute(
                    "SELECT gseq, op, id, phone, name, updated_at FROM rollback_log "
                    "WHERE gseq > ? ORDER BY gseq LIMIT ?",
                    (after_gseq, limit),
                ).fetchall()
            iters.append(iter((r, shard) for r in rows))
        for r, _shard in heapq.merge(*iters, key=lambda t: (t[0]["gseq"], t[1])):
            yield r

    def rollback_to_old(self) -> dict:
        if self.phase != SWITCHED:
            raise RuntimeError(f"阶段 {self.phase}，仅 SWITCHED 可回滚")
        t0 = time.monotonic()
        # 锁内瞬间进入只读，读仍走新库
        with self.gw.write_lock():
            self.gw.switch_mode(MODE_READONLY, ro_target=MODE_NEW)
        t_readonly = time.monotonic()
        self.log("回滚：进入只读窗口，反向追平切换后的新数据")
        # 只读期不持写锁，业务写立即收到 ReadOnlyError
        if self.readonly_hold_sec:
            time.sleep(self.readonly_hold_sec)

        # 把新库 rollback_log 全部按 gseq 序回放到旧库（replay 不产生回声日志）
        while True:
            batch = list(
                self._fetch_rollback_batch(
                    self.last_rollback_gseq, self.plan["batch_size"]
                )
            )
            if not batch:
                break
            for row in batch:
                uid = row["id"]
                old_shard = uid % OLD_SHARD_COUNT
                if row["op"] == "UPSERT":
                    self.gw.old.replay_upsert(
                        old_shard, uid, row["phone"], row["name"],
                        row["gseq"], row["updated_at"],
                    )
                else:
                    self.gw.old.replay_delete(old_shard, uid, row["gseq"])
                self.last_rollback_gseq = row["gseq"]
                self.rollback_rows += 1

        # 锁内瞬间切回旧规则，恢复读写
        with self.gw.write_lock():
            self.gw.switch_mode(MODE_OLD)
        result = {
            "readonly_window_sec": round(time.monotonic() - t_readonly, 4),
            "total_rollback_sec": round(time.monotonic() - t0, 4),
            "rows_replayed": self.rollback_rows,
        }
        self._set_phase(ROLLED_BACK)
        self.log(f"回滚完成：反向回放 {self.rollback_rows} 行")
        return result

    # ---------------- 二次切换 ----------------
    def reswitch_to_new(self) -> dict:
        if self.phase != ROLLED_BACK:
            raise RuntimeError(f"阶段 {self.phase}，仅 ROLLED_BACK 可重新切换")
        self.log("重新切换前：清空回滚日志与唯一索引，重新追平旧库增量")
        self.gw.new.reset_rollback_log()
        self.gw.new.drop_phone_unique_index()

        # 回滚期间旧库产生的 gseq 继续追平到新库（启动全新的追平线程）
        self._start_catchup_thread()
        self.drain_catchup()
        self.verify_checksum()
        return self.switch_to_new()

    # ---------------- 状态快照 ----------------
    def status(self) -> dict:
        with self._state_lock:
            return {
                "phase": self.phase,
                "plan": self.plan,
                "gateway_mode": self.gw.mode,
                "ro_target": self.gw.ro_target,
                "current_gseq": self.gw.current_gseq(),
                "backfill": {
                    "rows_copied": self.backfill_rows,
                    "positions": [
                        {"old_shard": p.old_shard, "last_id": p.last_id,
                         "rows_copied": p.rows_copied}
                        for p in self.positions
                    ],
                },
                "catchup": {
                    "replayed_rows": self.replayed_rows,
                    "last_gseq": self.last_catchup_gseq,
                    "lag": self.catchup_lag(),
                },
                "rollback": {
                    "replayed_rows": self.rollback_rows,
                    "last_gseq": self.last_rollback_gseq,
                },
                "verify": {
                    "last_at": self.last_verify_at,
                    "rounds": self.verify_rounds,
                    "segments_total": len(self.segments),
                    "segments_mismatch": sum(1 for s in self.segments if not s.match),
                    "repaired_segments": self.repaired_segments,
                },
                "paused": not self._pause.is_set(),
                "error": self.error,
                "segments": [
                    {
                        "index": s.index,
                        "id_start": s.id_start,
                        "id_end": s.id_end,
                        "rows": s.rows,
                        "old_hash": s.old_hash,
                        "new_hash": s.new_hash,
                        "match": s.match,
                    }
                    for s in self.segments
                ],
                "counts": {
                    "old_users": self.gw.old.total_user_count(),
                    "new_users": self.gw.new.total_user_count(),
                    "control_users": self.gw.control_count(),
                    "old_shards": self.gw.old.shard_counts(),
                    "new_shards": self.gw.new.shard_counts(),
                },
                "events": list(self.events[-40:]),
            }
