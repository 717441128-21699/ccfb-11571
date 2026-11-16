"""分片集群：用 SQLite 文件模拟一组物理分片库。

每个分片文件包含：
  users         业务表（uid 主键）
  changelog     变更日志（旧分片在线写入时由代码同事务显式记录，追平数据源）
  rollback_log  回滚日志（新分片切换承载写入后由代码同事务显式记录）

设计要点：不使用 SQL trigger + 上下文表的隐式方案（跨连接复用脆弱），
日志行与业务行在同一个事务、同一段代码里写入，原子且无隐式状态。
迁移搬运/追平的写入通过 log=False / replay=True 控制，不产生回声日志。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

USERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY,
    phone      TEXT NOT NULL,
    name       TEXT NOT NULL,
    gseq       INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
)
"""

# 迁移期新库不建 phone 的硬 UNIQUE 索引：存量搬运与增量追平对不同分片的
# 写入存在交错（换号先落地、旧持有者删除后落地），会瞬时撞跨分片唯一约束。
# 迁移期唯一性由协调库 phone_registry + 切换前全局重复扫描保证；
# 切换时由 migrator 安装唯一索引，承载切换后的在线约束。
PHONE_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_phone ON users(phone)"
)
PHONE_DROP_INDEX = "DROP INDEX IF EXISTS idx_users_phone"

CHANGELOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS changelog (
    gseq       INTEGER PRIMARY KEY,
    op         TEXT NOT NULL,
    id         INTEGER NOT NULL,
    phone      TEXT,
    name       TEXT,
    updated_at INTEGER
)
"""

ROLLBACK_SCHEMA = """
CREATE TABLE IF NOT EXISTS rollback_log (
    gseq       INTEGER PRIMARY KEY,
    op         TEXT NOT NULL,
    id         INTEGER NOT NULL,
    phone      TEXT,
    name       TEXT,
    updated_at INTEGER
)
"""

UPSERT_SQL = (
    "INSERT INTO users(id, phone, name, gseq, updated_at) "
    "VALUES (?, ?, ?, ?, ?) "
    "ON CONFLICT(id) DO UPDATE SET "
    "phone = excluded.phone, name = excluded.name, "
    "gseq = excluded.gseq, updated_at = excluded.updated_at "
    "WHERE excluded.gseq > users.gseq"
)


class ShardError(RuntimeError):
    pass


class Cluster:
    """一个分片集群：cluster_dir 下的 N 个 SQLite 文件。"""

    def __init__(self, name: str, base_dir: str, shard_count: int, role: str):
        assert role in ("old", "new")
        self.name = name
        self.dir = os.path.join(base_dir, name)
        os.makedirs(self.dir, exist_ok=True)
        self.shard_count = shard_count
        self.role = role
        self.paths = [
            os.path.join(self.dir, f"{name}_shard_{i:02d}.db")
            for i in range(shard_count)
        ]
        self._conns: list[sqlite3.Connection] = []
        # 每个分片连接一把锁：SQLite 连接不能被多线程并发使用，
        # 串行化该分片上的事务，避免两个线程的 BEGIN/COMMIT 交错
        self._tx_locks = [threading.RLock() for _ in range(shard_count)]
        for path in self.paths:
            # isolation_level=None：关闭 Python sqlite3 的隐式事务，
            # 事务边界完全由 transaction() 里的 BEGIN/COMMIT 控制
            conn = sqlite3.connect(
                path, check_same_thread=False, timeout=30, isolation_level=None
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._conns.append(conn)
            self._init_shard(conn)

    def _init_shard(self, conn: sqlite3.Connection) -> None:
        conn.execute(USERS_SCHEMA)
        conn.execute(CHANGELOG_SCHEMA)
        conn.execute(ROLLBACK_SCHEMA)
        if self.role == "old":
            # 旧库在线承载写入，分片内手机号唯一约束始终在线
            conn.execute(PHONE_UNIQUE_INDEX)
        conn.commit()

    def conn(self, shard: int) -> sqlite3.Connection:
        return self._conns[shard]

    @contextmanager
    def read_lock(self, shard: int):
        """读也走分片锁：SQLite 连接不能被多线程并发使用，
        读与写事务互斥，避免 'bad parameter or other API misuse'。"""
        with self._tx_locks[shard]:
            yield self._conns[shard]

    def all_conns(self) -> list[sqlite3.Connection]:
        return list(self._conns)

    @contextmanager
    def transaction(self, shard: int):
        """分片事务；分片锁保证同一连接上的事务不被其他线程交错。"""
        conn = self.conn(shard)
        with self._tx_locks[shard]:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---------------- 在线写入（带日志） ----------------
    def online_upsert(self, shard: int, gseq: int, uid: int, phone: str,
                      name: str, updated_at: int) -> None:
        """业务在线 upsert：业务行 + 日志行同事务原子写入。"""
        conn = self.conn(shard)
        log_table = "changelog" if self.role == "old" else "rollback_log"
        with self._tx_locks[shard]:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    UPSERT_SQL, (uid, phone, name, gseq, updated_at)
                )
                conn.execute(
                    f"INSERT INTO {log_table}(gseq, op, id, phone, name, updated_at) "
                    "VALUES (?, 'UPSERT', ?, ?, ?, ?) "
                    "ON CONFLICT(gseq) DO UPDATE SET op=excluded.op, id=excluded.id, "
                    "phone=excluded.phone, name=excluded.name, updated_at=excluded.updated_at",
                    (gseq, uid, phone, name, updated_at),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def online_delete(self, shard: int, gseq: int, uid: int, phone: str,
                      name: str, updated_at: int) -> None:
        """业务在线删除：删除业务行 + 写 DELETE 日志，同事务原子完成。"""
        conn = self.conn(shard)
        log_table = "changelog" if self.role == "old" else "rollback_log"
        with self._tx_locks[shard]:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM users WHERE id = ?", (uid,))
                conn.execute(
                    f"INSERT INTO {log_table}(gseq, op, id, phone, name, updated_at) "
                    "VALUES (?, 'DELETE', ?, ?, ?, ?) "
                    "ON CONFLICT(gseq) DO UPDATE SET op=excluded.op, id=excluded.id, "
                    "phone=excluded.phone, name=excluded.name, updated_at=excluded.updated_at",
                    (gseq, uid, phone, name, updated_at),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---------------- 迁移写入（不带日志，带版本守护） ----------------
    def replay_upsert(self, shard: int, uid: int, phone: str, name: str,
                      gseq: int, updated_at: int) -> None:
        """搬运/追平写入：只改业务表，不产生任何日志回声。"""
        with self._tx_locks[shard]:
            conn = self.conn(shard)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    UPSERT_SQL, (uid, phone, name, gseq, updated_at)
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def replay_delete(self, shard: int, uid: int, gseq: int) -> None:
        """追平删除：版本守护，只删旧版本行，不产生日志。"""
        with self._tx_locks[shard]:
            conn = self.conn(shard)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "DELETE FROM users WHERE id = ? AND gseq < ?", (uid, gseq)
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def force_delete(self, shard: int, uid: int) -> None:
        """校验修复用：权威侧已无此行，物理删除本侧残留，不产生日志。"""
        with self._tx_locks[shard]:
            conn = self.conn(shard)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM users WHERE id = ?", (uid,))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---------------- 索引与回滚日志管理 ----------------
    def install_phone_unique_index(self) -> None:
        """切换前安装：此后新库在线承载写入，分片内手机号强制唯一。"""
        for shard, conn in enumerate(self._conns):
            with self._tx_locks[shard]:
                conn.execute(PHONE_UNIQUE_INDEX)
                conn.commit()

    def drop_phone_unique_index(self) -> None:
        """二次切换重新追平前拆除，避免搬运交错期的瞬时冲突。"""
        for shard, conn in enumerate(self._conns):
            with self._tx_locks[shard]:
                conn.execute(PHONE_DROP_INDEX)
                conn.commit()

    def reset_rollback_log(self) -> None:
        for shard, conn in enumerate(self._conns):
            with self._tx_locks[shard]:
                conn.execute("DELETE FROM rollback_log")
                conn.commit()

    def total_user_count(self) -> int:
        total = 0
        for shard, _c in enumerate(self._conns):
            with self.read_lock(shard) as conn:
                total += conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        return total

    def total_changelog_count(self) -> int:
        total = 0
        for shard, _c in enumerate(self._conns):
            with self.read_lock(shard) as conn:
                total += conn.execute(
                    "SELECT COUNT(*) FROM changelog"
                ).fetchone()[0]
        return total

    def total_rollback_count(self) -> int:
        total = 0
        for shard, _c in enumerate(self._conns):
            with self.read_lock(shard) as conn:
                total += conn.execute(
                    "SELECT COUNT(*) FROM rollback_log"
                ).fetchone()[0]
        return total

    def shard_counts(self) -> list[int]:
        out = []
        for shard, _c in enumerate(self._conns):
            with self.read_lock(shard) as conn:
                out.append(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0])
        return out

    def close(self) -> None:
        for conn in self._conns:
            conn.close()
        self._conns.clear()
