"""业务读写网关。

职责：
  1. 按当前模式（old / new / readonly）路由读写；
  2. 分配全局单调 gseq，写库时由 trigger 落到变更日志；
  3. 协调库 phone_registry 保证跨分片手机号唯一；
  4. 每次成功写入同步镜像到不迁移的对照库 control.db，作为最终正确性基准；
  5. 切换/回滚在同一把写锁内完成，业务线程要么在旧状态写完，要么等新状态。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from .cluster import Cluster
from .sharding import NEW_SHARD_COUNT, OLD_SHARD_COUNT

MODE_OLD = "old"
MODE_NEW = "new"
MODE_READONLY = "readonly"


class GatewayError(RuntimeError):
    pass


class ReadOnlyError(GatewayError):
    """只读窗口期业务写入被拒。压测脚本据此统计不可写时间。"""


class PhoneConflictError(GatewayError):
    def __init__(self, phone: str, owner_id: int, request_id: int):
        super().__init__(
            f"手机号 {phone} 已属于用户 {owner_id}，不能绑定给用户 {request_id}"
        )
        self.phone = phone
        self.owner_id = owner_id
        self.request_id = request_id


META_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)
"""
SEQ_SCHEMA = """
CREATE TABLE IF NOT EXISTS seq (k INTEGER PRIMARY KEY CHECK (k = 1), v INTEGER NOT NULL)
"""
PHONE_SCHEMA = """
CREATE TABLE IF NOT EXISTS phone_registry (
    phone TEXT PRIMARY KEY,
    uid   INTEGER NOT NULL,
    gseq  INTEGER NOT NULL
)
"""
CONTROL_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY,
    phone      TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    gseq       INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
)
"""


class Gateway:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.old = Cluster("old", data_dir, OLD_SHARD_COUNT, "old")
        self.new = Cluster("new", data_dir, NEW_SHARD_COUNT, "new")

        coord_path = os.path.join(data_dir, "coordinator.db")
        self._coord = sqlite3.connect(
            coord_path, check_same_thread=False, timeout=30, isolation_level=None
        )
        self._coord.row_factory = sqlite3.Row
        self._coord.execute("PRAGMA journal_mode=WAL")
        self._coord.execute("PRAGMA busy_timeout=30000")
        self._coord.execute(META_SCHEMA)
        self._coord.execute(SEQ_SCHEMA)
        self._coord.execute("INSERT OR IGNORE INTO seq(k, v) VALUES (1, 0)")
        self._coord.execute(PHONE_SCHEMA)
        self._coord.commit()

        control_path = os.path.join(data_dir, "control.db")
        self._control = sqlite3.connect(
            control_path, check_same_thread=False, timeout=30, isolation_level=None
        )
        self._control.row_factory = sqlite3.Row
        self._control.execute("PRAGMA journal_mode=WAL")
        self._control.execute("PRAGMA busy_timeout=30000")
        self._control.execute(CONTROL_SCHEMA)
        self._control.commit()

        self._write_lock = threading.RLock()
        self.mode = self._get_meta("mode", MODE_OLD)
        # 只读窗口内读请求走哪个集群（切换时 old，回滚时 new）
        self.ro_target = self._get_meta("ro_target", MODE_OLD)

        self.stats = {
            "upsert_ok": 0,
            "upsert_conflict": 0,
            "delete_ok": 0,
            "delete_miss": 0,
            "reads": 0,
            "rejected_readonly": 0,
        }

    # ---------- 持久化元数据 ----------
    def _get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self._coord.execute(
            "SELECT v FROM meta WHERE k = ?", (key,)
        ).fetchone()
        return row["v"] if row else default

    def _set_meta(self, key: str, value: str) -> None:
        self._coord.execute(
            "INSERT INTO meta(k, v) VALUES (?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (key, value),
        )
        self._coord.commit()

    # ---------- 模式与锁 ----------
    @contextmanager
    def write_lock(self):
        with self._write_lock:
            yield

    def switch_mode(self, mode: str, ro_target: str | None = None) -> None:
        with self._write_lock:
            self.mode = mode
            self._set_meta("mode", mode)
            if ro_target is not None:
                self.ro_target = ro_target
                self._set_meta("ro_target", ro_target)

    def _serving_cluster(self) -> Cluster:
        if self.mode == MODE_NEW:
            return self.new
        if self.mode == MODE_OLD:
            return self.old
        return self.new if self.ro_target == MODE_NEW else self.old

    # ---------- gseq ----------
    def _next_gseq(self) -> int:
        # 调用方已持有 write_lock；coord 单连接，事务串行
        self._coord.execute("BEGIN IMMEDIATE")
        try:
            self._coord.execute("UPDATE seq SET v = v + 1 WHERE k = 1")
            gseq = self._coord.execute("SELECT v FROM seq WHERE k = 1").fetchone()[0]
            self._coord.commit()
        except Exception:
            self._coord.rollback()
            raise
        return gseq

    def current_gseq(self) -> int:
        with self._write_lock:
            return self._coord.execute(
                "SELECT v FROM seq WHERE k = 1"
            ).fetchone()[0]

    # ---------- 业务写 ----------
    def upsert_user(self, user_id: int, phone: str, name: str) -> int:
        with self._write_lock:
            if self.mode == MODE_READONLY:
                self.stats["rejected_readonly"] += 1
                raise ReadOnlyError("集群处于切换只读窗口，写入暂不可用")
            gseq = self._next_gseq()
            target = self._serving_cluster()
            shard = user_id % target.shard_count
            now = time.time_ns()

            # 跨分片手机号唯一：全局注册表先判定，再落分片 UNIQUE 索引兜底
            self._coord.execute("BEGIN IMMEDIATE")
            try:
                row = self._coord.execute(
                    "SELECT uid FROM phone_registry WHERE phone = ?", (phone,)
                ).fetchone()
                if row is not None and row["uid"] != user_id:
                    self._coord.rollback()
                    self.stats["upsert_conflict"] += 1
                    raise PhoneConflictError(phone, row["uid"], user_id)
                self._coord.execute(
                    "INSERT INTO phone_registry(phone, uid, gseq) VALUES (?, ?, ?) "
                    "ON CONFLICT(phone) DO UPDATE SET uid = excluded.uid, gseq = excluded.gseq",
                    (phone, user_id, gseq),
                )
                self._coord.commit()
            except PhoneConflictError:
                raise
            except Exception:
                self._coord.rollback()
                raise

            # 业务行与变更日志（或回滚日志）在分片内同事务原子写入
            target.online_upsert(shard, gseq, user_id, phone, name, now)

            # 对照库镜像：与业务写同在 write_lock 内，顺序严格一致
            self._control.execute(
                "INSERT INTO users(id, phone, name, gseq, updated_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET phone = excluded.phone, "
                "name = excluded.name, gseq = excluded.gseq, "
                "updated_at = excluded.updated_at",
                (user_id, phone, name, gseq, now),
            )
            self._control.commit()
            self.stats["upsert_ok"] += 1
            return gseq

    def delete_user(self, user_id: int) -> bool:
        with self._write_lock:
            if self.mode == MODE_READONLY:
                self.stats["rejected_readonly"] += 1
                raise ReadOnlyError("集群处于切换只读窗口，写入暂不可用")
            target = self._serving_cluster()
            shard = user_id % target.shard_count
            with target.read_lock(shard) as rc:
                row = rc.execute(
                    "SELECT id, phone, name, gseq, updated_at FROM users WHERE id = ?",
                    (user_id,),
                ).fetchone()
            if row is None:
                self.stats["delete_miss"] += 1
                return False
            gseq = self._next_gseq()
            target.online_delete(
                shard, gseq, user_id, row["phone"], row["name"], row["updated_at"]
            )
            self._coord.execute(
                "DELETE FROM phone_registry WHERE phone = ?", (row["phone"],)
            )
            self._coord.commit()
            self._control.execute("DELETE FROM users WHERE id = ?", (user_id,))
            self._control.commit()
            self.stats["delete_ok"] += 1
            return True

    # ---------- 业务读 ----------
    def get_user(self, user_id: int) -> sqlite3.Row | None:
        target = self._serving_cluster()
        shard = user_id % target.shard_count
        with target.read_lock(shard) as conn:
            row = conn.execute(
                "SELECT id, phone, name, gseq, updated_at FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
        self.stats["reads"] += 1
        return row

    def control_count(self) -> int:
        with self._write_lock:
            return self._control.execute(
                "SELECT COUNT(*) FROM users"
            ).fetchone()[0]

    def control_rows(self):
        with self._write_lock:
            return self._control.execute(
                "SELECT id, phone, name, gseq, updated_at FROM users ORDER BY id"
            ).fetchall()

    def control_phone_count(self) -> int:
        with self._write_lock:
            return self._control.execute(
                "SELECT COUNT(DISTINCT phone) FROM users"
            ).fetchone()[0]

    def stats_snapshot(self) -> dict:
        with self._write_lock:
            return dict(self.stats)

    def close(self) -> None:
        self.old.close()
        self.new.close()
        self._coord.close()
        self._control.close()
