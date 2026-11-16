"""分片规则：旧库 4 分片 (uid % 4)，新库 16 分片 (uid % 16)。

扩容映射关系：旧分片 k 的数据会落到新分片 k、k+4、k+8、k+12
（即 uid % 4 == k 的 uid，其 uid % 16 必为这四个值之一）。
"""
from dataclasses import dataclass, asdict

OLD_SHARD_COUNT = 4
NEW_SHARD_COUNT = 16


def shard_index(user_id: int, shard_count: int) -> int:
    return user_id % shard_count


@dataclass(frozen=True)
class Location:
    rule: str
    shard_count: int
    shard: int
    db_file: str


def locate(user_id: int, shard_count: int, rule: str) -> Location:
    idx = shard_index(user_id, shard_count)
    return Location(
        rule=rule,
        shard_count=shard_count,
        shard=idx,
        db_file=f"{rule}_shard_{idx:02d}.db",
    )


def locate_both(user_id: int) -> dict:
    """按用户 ID 同时算出旧库和新库位置（控制台分片规则页使用）。"""
    return {
        "user_id": user_id,
        "old": asdict(locate(user_id, OLD_SHARD_COUNT, "old")),
        "new": asdict(locate(user_id, NEW_SHARD_COUNT, "new")),
    }


def expansion_map() -> list[dict]:
    """旧分片 -> 新分片的拆分映射。"""
    result = []
    for k in range(OLD_SHARD_COUNT):
        targets = [k + 4 * j for j in range(NEW_SHARD_COUNT // OLD_SHARD_COUNT)]
        result.append({"old_shard": k, "new_shards": targets})
    return result
