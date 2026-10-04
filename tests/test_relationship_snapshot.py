"""两类关系查询“同一完整视图”混版修复的回归测试（无需真实数据库）。

修复口径：
- 正式关系查询 app.service.query_relationship 与推演关系查询
  app.projections.query_projection_relationship 均以
  `SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ` 开启只读快照事务；
- 修订号（推演另含推演修订号 / 状态）、关系结论、最优证据路径、逐步证据明细必须取自
  同一事务快照；并发批次在读取之间提交时，整体落在提交前或提交后，不允许混版。

假连接把每个表显式绑定到“批次提交前 / 提交后”两个视图之一：

- snapshot 模式：全部表读提交前视图（修复后 REPEATABLE READ 的行为）；
- mix_* 模式：复刻旧 READ COMMITTED 多次读取的混版窗口——修订号 / 推演行先读、
  生效断言与逐步明细后读，二者跨越并发批次的提交点：
    * formal_mix_conclusion：旧修订号配上提交后才存在的新结论 / 新路径；
    * formal_mix_status：新修订号配上提交前路径，但路径证据状态读成已撤回；
    * proj_mix_conclusion：旧推演修订号配上推演批次提交后的新结论。

运行：python3 tests/test_relationship_snapshot.py（或 pytest 收集）
"""
from __future__ import annotations

import os
import sys
import types
from contextlib import contextmanager

# app.db 顶部 import psycopg2 / psycopg2.pool；本测试不需要真实驱动
_psycopg2 = types.ModuleType("psycopg2")
_psycopg2_pool = types.ModuleType("psycopg2.pool")
_psycopg2_extras = types.ModuleType("psycopg2.extras")
_psycopg2_extras.Json = lambda obj: obj
_psycopg2.pool = _psycopg2_pool
_psycopg2.extras = _psycopg2_extras
sys.modules.setdefault("psycopg2", _psycopg2)
sys.modules.setdefault("psycopg2.pool", _psycopg2_pool)
sys.modules.setdefault("psycopg2.extras", _psycopg2_extras)

# app.models 依赖 pydantic；本环境未安装，且关系查询路径不会实例化请求模型，
# 仅需满足类定义期的 import 与 Field(...) 调用。
try:
    import pydantic  # noqa: F401
except ImportError:
    _pydantic = types.ModuleType("pydantic")

    class _BaseModel:  # 最小桩：仅承担类定义，不做校验
        pass

    def _Field(*args, **kwargs):
        return (args, kwargs)

    _pydantic.BaseModel = _BaseModel
    _pydantic.Field = _Field
    sys.modules.setdefault("pydantic", _pydantic)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import projections, service  # noqa: E402
from app.errors import ApiError  # noqa: E402

FAILURES = []


def check(cond, message):
    if cond:
        print(f"  PASS: {message}")
    else:
        FAILURES.append(message)
        print(f"  FAIL: {message}")


# ------------------------------------------------------------------ 两个世界的视图
# 正式世界：并发正式批次（修订 1 -> 2）撤回 EV-002、新增直达证据 EV-009。
FORMAL_BEFORE = {
    "revision": 1,
    # evidence_id -> (a, b, relation, status)
    "rows": {
        "EV-001": ("L1", "L2", "same_period", "active"),
        "EV-002": ("L2", "L3", "earlier_than", "active"),
    },
}
FORMAL_AFTER = {
    "revision": 2,
    "rows": {
        "EV-001": ("L1", "L2", "same_period", "active"),
        "EV-002": ("L2", "L3", "earlier_than", "retracted"),
        "EV-009": ("L1", "L3", "earlier_than", "active"),
    },
}

# 推演世界：基线固定在正式修订 1（EV-001 / EV-002）；并发推演批次（推演修订
# 0 -> 1）撤回基线 EV-002、新增推演证据 EV-PX（L1 早于 L3）。
PID = 7
PROJ_BEFORE = {
    "row": (PID, "t", 1, ["L1", "L2", "L3"],
            [{"evidence_id": "EV-001", "stratum_a": "L1", "stratum_b": "L2", "relation": "same_period"},
             {"evidence_id": "EV-002", "stratum_a": "L2", "stratum_b": "L3", "relation": "earlier_than"}],
            0, "open", None),
    "added": {},   # evidence_id -> (a, b, relation, status, added_rev, retracted_rev)
    "retracted": {},  # evidence_id -> retracted_rev
}
PROJ_AFTER = {
    "row": (PID, "t", 1, PROJ_BEFORE["row"][3], PROJ_BEFORE["row"][4], 1, "open", None),
    "added": {"EV-PX": ("L1", "L3", "earlier_than", "active", 1, None)},
    "retracted": {"EV-002": 1},
}
# 推演逐步明细中基线证据回正式表取端点 / 关系（本场景正式表始终为基线态）。
PROJ_BASE_FORMAL_ROWS = {
    "EV-001": ("L1", "L2", "same_period", "active"),
    "EV-002": ("L2", "L3", "earlier_than", "active"),
}

STRATA = ["L1", "L2", "L3"]

# 每种模式把四类读取分别绑到提交前 / 提交后视图：
#   revision       正式修订计数器
#   formal_active  正式生效断言集（推导结论 + 最优路径）
#   formal_detail  正式逐步明细（含 status）
#   proj_row       推演行（推演修订号 / 状态 / 基线）
#   proj_tables    推演新增 + 撤回记录（推导推演视图结论 + 路径）
#   proj_detail    推演逐步明细（projection_assertions 侧）
MODES = {
    "formal_snapshot":        dict(revision="before", formal_active="before", formal_detail="before"),
    "formal_mix_conclusion":  dict(revision="before", formal_active="after",  formal_detail="after"),
    "formal_mix_status":      dict(revision="after",  formal_active="before", formal_detail="after"),
    "proj_snapshot":          dict(proj_row="before", proj_tables="before", proj_detail="before"),
    "proj_mix_conclusion":    dict(proj_row="before", proj_tables="after",  proj_detail="after"),
}


class FakeCursor:
    """按 SQL 文本分发到模式绑定的提交前 / 提交后视图。"""

    def __init__(self, mode: str):
        self.mode = mode
        self.binding = MODES[mode]
        self._rows = []
        self.started = False

    def _formal_view(self, key: str):
        return FORMAL_BEFORE if self.binding[key] == "before" else FORMAL_AFTER

    def _proj_view(self, key: str):
        return PROJ_BEFORE if self.binding[key] == "before" else PROJ_AFTER

    # ---------------------------------------------------------------- 分发
    def execute(self, sql, params=None):
        norm = " ".join(sql.split()).upper()
        if norm.startswith("SET TRANSACTION"):
            self.started = True
            self._rows = []
            return
        if not self.started:
            raise AssertionError(f"关系查询未先开启只读快照事务即读库: {sql}")

        if "REVISION_COUNTER" in norm:
            self._rows = [(self._formal_view("revision")["revision"],)]
        elif "FROM STRATA" in norm and "WHERE STRATUM_ID" in norm:
            self._rows = [(1,)] if params and params[0] in STRATA else []
        elif "FROM PROJECTIONS WHERE ID" in norm:
            view = self._proj_view("proj_row")
            self._rows = [view["row"]] if params and params[0] == PID else []
        elif "FROM PROJECTION_RETRACTIONS" in norm:
            view = self._proj_view("proj_tables")
            self._rows = sorted((eid, rev) for eid, rev in view["retracted"].items())
        elif "FROM PROJECTION_ASSERTIONS" in norm and "ANY" in norm:
            # 逐步明细：5 列 (eid, a, b, relation, status)
            view = self._proj_view("proj_detail")
            wanted = set(params[1])
            self._rows = [
                (eid, a, b, rel, status)
                for eid, (a, b, rel, status, _ar, _rr) in view["added"].items()
                if eid in wanted
            ]
        elif "FROM PROJECTION_ASSERTIONS" in norm:
            # 推演新增全量：7 列
            view = self._proj_view("proj_tables")
            self._rows = [
                (eid, a, b, rel, status, ar, rr)
                for eid, (a, b, rel, status, ar, rr) in view["added"].items()
            ]
        elif "FROM ASSERTIONS" in norm and "ANY" in norm:
            # 正式逐步明细（推演查询回查基线证据也走这里）：5 列
            if self.mode.startswith("proj"):
                rows = PROJ_BASE_FORMAL_ROWS
            else:
                rows = self._formal_view("formal_detail")["rows"]
            wanted = set(params[0])
            self._rows = [(eid, *rows[eid]) for eid in sorted(wanted) if eid in rows]
        elif "FROM ASSERTIONS" in norm:
            # 正式生效断言全量：4 列（无 status）
            view = self._formal_view("formal_active")
            self._rows = sorted(
                (eid, a, b, rel)
                for eid, (a, b, rel, status) in view["rows"].items()
                if status == "active"
            )
        else:
            raise AssertionError(f"关系查询路径出现意外 SQL: {sql}")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, mode):
        self.cursor_obj = FakeCursor(mode)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


LAST_CONN: dict = {}


@contextmanager
def fake_db_conn(mode="formal_snapshot"):
    conn = FakeConn(mode)
    LAST_CONN["conn"] = conn
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def assert_snapshot_opened(scope: str):
    conn = LAST_CONN.get("conn")
    check(conn is not None and conn.cursor_obj.started,
          f"{scope}：生产代码在任何读取前执行了 SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")


# ------------------------------------------------------------------ 正式查询
def test_formal(mode: str, label: str):
    print(f"[正式关系查询] {label}")
    token = service.db_conn
    service.db_conn = lambda: fake_db_conn(mode)
    try:
        payload = service.query_relationship("L1", "L3")
    finally:
        service.db_conn = token

    if mode == "formal_snapshot":
        assert_snapshot_opened("正式关系查询")
        # 提交前完整视图：修订 1；L1~L2 + L2<L3 => 早于，路径 [EV-001, EV-002]
        check(payload["revision"] == 1, "修订号为提交前的 1")
        check(payload["relation"] == "earlier_than", "关系为早于（提交前完整视图）")
        check(payload["evidence_path"] == ["EV-001", "EV-002"],
              "最优路径为 [EV-001, EV-002]（与修订号 1 对应）")
        statuses = {d["evidence_id"]: d["status"] for d in payload["path_detail"]}
        check(statuses == {"EV-001": "active", "EV-002": "active"},
              "逐步明细中 EV-002 仍生效（路径证据不显示为已撤回）")
    elif mode == "formal_mix_conclusion":
        check(payload["revision"] == 1, "（旧口径）修订号读到提交前的 1")
        check(payload["evidence_path"] == ["EV-009"],
              "（旧口径）结论 / 路径却读到提交后才新增的 EV-009（旧修订号配新结论）")
        check(any(d["evidence_id"] == "EV-009" for d in payload["path_detail"]),
              "（旧口径）逐步明细也来自提交后视图")
    elif mode == "formal_mix_status":
        check(payload["revision"] == 2, "（旧口径）修订号读到提交后的 2")
        check(payload["evidence_path"] == ["EV-001", "EV-002"],
              "（旧口径）结论 / 路径仍取自提交前视图")
        bad = {d["evidence_id"]: d["status"] for d in payload["path_detail"]}
        check(bad.get("EV-002") == "retracted",
              "（旧口径）路径中的 EV-002 被显示为已撤回（证据状态混版）")


# ------------------------------------------------------------------ 推演查询
def test_projection(mode: str, label: str):
    print(f"[推演关系查询] {label}")
    token = projections.db_conn
    projections.db_conn = lambda: fake_db_conn(mode)
    try:
        payload = projections.query_projection_relationship(PID, "L1", "L3")
    finally:
        projections.db_conn = token

    if mode == "proj_snapshot":
        assert_snapshot_opened("推演关系查询")
        # 提交前完整冻结视图：推演修订 0 / open，基线两条证据
        check(payload["projection_revision"] == 0, "推演修订号为提交前的 0")
        check(payload["status"] == "open", "推演状态为 open（与修订号同一视图）")
        check(payload["relation"] == "earlier_than", "关系为早于（提交前冻结视图）")
        check(payload["evidence_path"] == ["EV-001", "EV-002"],
              "最优路径为基线 [EV-001, EV-002]（与推演修订 0 对应）")
        statuses = {d["evidence_id"]: d["status"] for d in payload["path_detail"]}
        scopes = {d["evidence_id"]: d["scope"] for d in payload["path_detail"]}
        check(statuses == {"EV-001": "active", "EV-002": "active"},
              "逐步明细中基线 EV-002 仍生效（不显示为已撤回）")
        check(scopes == {"EV-001": "base", "EV-002": "base"}, "逐步明细 scope 均为 base")
    elif mode == "proj_mix_conclusion":
        check(payload["projection_revision"] == 0, "（旧口径）推演修订号读到提交前的 0")
        check(payload["status"] == "open", "（旧口径）推演状态读到提交前的 open")
        check(payload["evidence_path"] == ["EV-PX"],
              "（旧口径）结论 / 路径却读到推演批次提交后的 EV-PX（旧推演修订号配新结论）")
        scopes = {d["evidence_id"]: d["scope"] for d in payload["path_detail"]}
        check(scopes == {"EV-PX": "projection"}, "（旧口径）逐步明细也来自提交后的推演视图")


# ------------------------------------------------------------------ 错误约定
def test_errors():
    print("[错误约定]")
    token_svc, token_proj = service.db_conn, projections.db_conn
    service.db_conn = lambda: fake_db_conn("formal_snapshot")
    projections.db_conn = lambda: fake_db_conn("proj_snapshot")
    try:
        for fn, args, code in (
            (service.query_relationship, ("NOPE", "L3"), "INVALID_STRATUM"),
            (projections.query_projection_relationship, (PID, "L1", "NOPE"), "INVALID_STRATUM"),
            (projections.query_projection_relationship, (999, "L1", "L3"), "PROJECTION_NOT_FOUND"),
        ):
            try:
                fn(*args)
                check(False, f"应抛出 {code}")
            except ApiError as exc:
                check(exc.code == code, f"{args} -> {exc.code}（期望 {code}）")
    finally:
        service.db_conn, projections.db_conn = token_svc, token_proj


def main():
    test_formal("formal_snapshot", "REPEATABLE READ 快照（修复后口径，提交前完整视图）")
    test_formal("formal_mix_conclusion", "READ COMMITTED 混版演示 A：旧修订号 + 新结论")
    test_formal("formal_mix_status", "READ COMMITTED 混版演示 B：新修订号 + 路径证据显示已撤回")
    test_projection("proj_snapshot", "REPEATABLE READ 快照（修复后口径，提交前完整冻结视图）")
    test_projection("proj_mix_conclusion", "READ COMMITTED 混版演示：旧推演修订号 + 新结论")
    test_errors()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} 项断言失败：")
        for m in FAILURES:
            print(" -", m)
        raise SystemExit(1)
    print("全部断言通过：快照模式响应内部一致；旧口径混版被准确复现；错误约定不变。")


if __name__ == "__main__":
    main()
