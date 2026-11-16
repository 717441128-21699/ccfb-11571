"""在线分片扩容迁移工具（SQLite 模拟）。

模块:
  sharding  新旧分片规则与定位
  cluster   分片集群（SQLite 文件、表结构、变更日志 trigger）
  gateway   业务读写网关（模式切换、全局 gseq、手机号唯一约束、对照库镜像）
  migrator  迁移引擎（搬运 / 追平 / 校验 / 切换 / 回滚）
  console   迁移控制台（HTTP + 单页 UI）
"""

__version__ = "1.0.0"
