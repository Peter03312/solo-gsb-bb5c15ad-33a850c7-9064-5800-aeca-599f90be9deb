#!/usr/bin/env bash
# 考古地层关系核验服务 —— 两类关系查询的“同一完整视图”并发一致性演示
#
# 修复口径：
# - 正式关系查询 GET /relationship 与推演关系查询 GET /projections/{id}/relationship
#   均在 REPEATABLE READ READ ONLY 只读快照事务内完成；
# - 一次响应中的修订号（推演另含推演状态 / 推演修订号）、关系结论、最优证据路径、
#   逐步证据明细恒对应同一完整视图；并发批次在读取之间提交时，只可能整体取提交前
#   或提交后的结果，绝不混版（不会旧修订号配新结论，也不会把路径证据显示为已撤回）；
# - 已关闭推演（merged/discarded）仍可查询冻结视图；
# - 未知地层 400 INVALID_STRATUM、未知推演 404 PROJECTION_NOT_FOUND 约定不变。
#
# 做法：写线程连续提交批次，每个修订对应的真实结论由仓库自带的纯 Python 推导引擎
# （app/derivation.py）离线精算；多个读线程并发查询。全部写完成后逐条校验每个响应的
# revision / projection_revision 对应的关系、路径、逐步明细与该修订精算结果完全一致。
# 旧实现（READ COMMITTED 下多次读取）在该压力下可能出现“旧修订号 + 新结论 /
# 证据状态混版”的响应。
#
# 前提：服务已启动（docker compose up --build），监听 http://localhost:8000
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8000}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BASE_URL="$BASE" REPO_ROOT="$ROOT" python3 - <<'PY'
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = os.environ["BASE_URL"].rstrip("/")
sys.path.insert(0, os.environ["REPO_ROOT"])
from app.derivation import Assertion, Derivation  # noqa: E402  纯标准库模块，无需数据库驱动

SAME, EARLIER = "same_period", "earlier_than"
N = int(os.environ.get("CONSISTENCY_ROUNDS", "12"))


def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def formal_batch(expected, ops):
    status, payload = http("POST", "/batches", {"expected_revision": expected, "operations": ops})
    assert status == 200, (status, payload)
    return payload["revision"]


def projection_batch(pid, expected, ops):
    status, payload = http(
        "POST", f"/projections/{pid}/batches", {"expected_revision": expected, "operations": ops}
    )
    assert status == 200, (status, payload)
    return payload["projection_revision"]


def expected_view(active, a, b):
    rel, path = Derivation(list(active.values())).relation(a, b)
    return rel, list(path or [])


failures = []


def check(cond, message, ctx=None):
    if not cond:
        failures.append((message, ctx))


def validate_formal_response(resp, truth):
    r = resp["revision"]
    check(r in truth, f"正式查询返回未知修订号 {r}（混版）", resp)
    if r not in truth:
        return
    exp_rel, exp_path = expected_view(truth[r], X1, X3)
    check(resp["relation"] == exp_rel, "正式查询：修订号与关系结论不符（旧修订号配新结论）",
          {"response": resp, "expected_relation": exp_rel})
    check(list(resp["evidence_path"] or []) == exp_path,
          "正式查询：修订号与最优证据路径不符（旧修订号配新路径）",
          {"response": resp, "expected_path": exp_path})
    check([d["evidence_id"] for d in resp["path_detail"]] == (resp["evidence_path"] or []),
          "正式查询：evidence_path 与 path_detail 不是同一视图", resp)
    bad = [d["evidence_id"] for d in resp["path_detail"] if d["status"] != "active"]
    check(not bad, "正式查询：最优路径中出现已撤回证据（状态混版）", {"response": resp, "bad": bad})


# ------------------------------------------------------------------ 独立测试数据
TAG = f"QC{int(time.time() * 1000) % 10_000_000:07d}"
X1, X2, X3 = f"{TAG}A", f"{TAG}B", f"{TAG}C"
for s in (X1, X2, X3):
    status, payload = http("POST", "/strata", {"stratum_id": s, "site": "并发一致性演示"})
    assert status in (200, 201, 409), (status, payload)

print(f"== 准备：登记 {X1} < {X2} < {X3}，正式基线批次含 2 条早于证据 ==")
rev = http("GET", "/revision")[1]["revision"]
B1, B2 = f"{TAG}-QB-1", f"{TAG}-QB-2"
rev = formal_batch(rev, [
    {"op": "assert", "evidence_id": B1, "stratum_a": X1, "stratum_b": X2, "relation": EARLIER},
    {"op": "assert", "evidence_id": B2, "stratum_a": X2, "stratum_b": X3, "relation": EARLIER},
])

# truth[revision] = 该修订提交后全部生效断言的精算快照（写线程同步维护）
truth = {rev: {
    B1: Assertion(B1, X1, X2, EARLIER),
    B2: Assertion(B2, X2, X3, EARLIER),
}}
active = dict(truth[rev])

# ------------------------------------------------------------------ 正式查询压测
# 交替新增编号更小的平行证据 QA-k（字典序更小，最优路径立即改道）再撤回（路径恢复）。
formal_responses = []
stop = threading.Event()


def formal_writer():
    cur = rev
    for k in range(N):
        eid = f"{TAG}-QA-{k:04d}"  # 'QA' < 'QB'：生效时 X2<X3 段最优证据改道
        cur = formal_batch(cur, [
            {"op": "assert", "evidence_id": eid, "stratum_a": X2, "stratum_b": X3, "relation": EARLIER}
        ])
        active[eid] = Assertion(eid, X2, X3, EARLIER)
        truth[cur] = dict(active)
        cur = formal_batch(cur, [{"op": "retract", "evidence_id": eid}])
        del active[eid]
        truth[cur] = dict(active)
    return cur


def formal_reader():
    while not stop.is_set():
        status, payload = http("GET", f"/relationship?a={X1}&b={X3}")
        if status == 200:
            formal_responses.append(payload)


print(f"== 正式查询：1 写线程提交 {2 * N} 个修订（路径反复改道），4 读线程并发 ==")
with ThreadPoolExecutor(max_workers=5) as pool:
    writer = pool.submit(formal_writer)
    readers = [pool.submit(formal_reader) for _ in range(4)]
    final_rev = writer.result()
    stop.set()
    for f in readers:
        f.result()

for resp in formal_responses:
    validate_formal_response(resp, truth)
revisions_seen = sorted({r["revision"] for r in formal_responses})
print(f"   收集读响应 {len(formal_responses)} 条，覆盖修订 {revisions_seen}")
print(f"   写完成后当前修订为 {final_rev}；响应落在 {len(revisions_seen)} 个完整修订视图上"
      f"（提交前 / 提交后均可，但每条响应内部一致）")
assert formal_responses, "未收集到正式查询响应"

# ------------------------------------------------------------------ 推演查询压测
print("== 推演：创建固定基线的推演（基线修订含 QB-1/QB-2） ==")
status, proj = http("POST", "/projections", {"label": f"并发一致性推演 {TAG}"})
assert status == 201, (status, proj)
pid = proj["id"]
base_rev = proj["base_revision"]

# proj_truth[prevision] = 该推演修订下的生效断言（基线 + 推演内净变更）
base_assertions = dict(truth[base_rev])
proj_truth = {0: dict(base_assertions)}
proj_active = dict(base_assertions)

proj_responses = []
stop_p = threading.Event()


def proj_writer():
    cur = 0
    for k in range(N):
        eid = f"{TAG}-PA-{k:04d}"  # 'PA' < 'QB'：推演内新增时最优路径改道
        cur = projection_batch(pid, cur, [
            {"op": "assert", "evidence_id": eid, "stratum_a": X2, "stratum_b": X3, "relation": EARLIER}
        ])
        proj_active[eid] = Assertion(eid, X2, X3, EARLIER)
        proj_truth[cur] = dict(proj_active)
        cur = projection_batch(pid, cur, [{"op": "retract", "evidence_id": eid}])
        del proj_active[eid]
        proj_truth[cur] = dict(proj_active)
    return cur


def proj_reader():
    while not stop_p.is_set():
        status, payload = http("GET", f"/projections/{pid}/relationship?a={X1}&b={X3}")
        if status == 200:
            proj_responses.append(payload)


print(f"== 推演查询：1 写线程提交 {2 * N} 个推演批次，4 读线程并发 ==")
with ThreadPoolExecutor(max_workers=5) as pool:
    writer = pool.submit(proj_writer)
    readers = [pool.submit(proj_reader) for _ in range(4)]
    final_prevision = writer.result()
    stop_p.set()
    for f in readers:
        f.result()

for resp in proj_responses:
    pr = resp["projection_revision"]
    check(pr in proj_truth, f"推演查询返回未知推演修订号 {pr}（混版）", resp)
    if pr not in proj_truth:
        continue
    exp_rel, exp_path = expected_view(proj_truth[pr], X1, X3)
    check(resp["status"] == "open", "推演查询：状态与修订号不是同一视图（应为 open）", resp)
    check(resp["base_revision"] == base_rev, "推演查询：base_revision 与创建时不一致", resp)
    check(resp["relation"] == exp_rel, "推演查询：推演修订号与关系结论不符（旧修订配新结论）",
          {"response": resp, "expected_relation": exp_rel})
    check(list(resp["evidence_path"] or []) == exp_path,
          "推演查询：推演修订号与最优证据路径不符（旧修订配新路径）",
          {"response": resp, "expected_path": exp_path})
    check([d["evidence_id"] for d in resp["path_detail"]] == (resp["evidence_path"] or []),
          "推演查询：evidence_path 与 path_detail 不是同一视图", resp)
    bad = [d["evidence_id"] for d in resp["path_detail"] if d["status"] != "active"]
    check(not bad, "推演查询：最优路径中出现已撤回证据（增撤状态混版）", {"response": resp, "bad": bad})
    expected_scope = {e: ("projection" if e.startswith(f"{TAG}-PA-") else "base") for e in (resp["evidence_path"] or [])}
    wrong_scope = [d["evidence_id"] for d in resp["path_detail"] if d["scope"] != expected_scope[d["evidence_id"]]]
    check(not wrong_scope, "推演查询：逐步证据 scope 标注与该冻结视图不符",
          {"response": resp, "wrong": wrong_scope})

previsions_seen = sorted({r["projection_revision"] for r in proj_responses})
print(f"   收集读响应 {len(proj_responses)} 条，覆盖推演修订 {previsions_seen}")
print(f"   推演当前修订 {final_prevision}；每条响应的修订号 / 状态 / 结论 / 路径 / 明细整体一致")
assert proj_responses, "未收集到推演查询响应"

# 正式状态在整个推演压测期间不变
formal_now = http("GET", f"/relationship?a={X1}&b={X3}")[1]
check(formal_now["revision"] == final_rev, "推演批次不应改变正式修订号", formal_now)

# ------------------------------------------------------------------ 已关闭推演冻结视图
print("== 放弃推演后，冻结视图仍可查询（HTTP 200，status=discarded，修订停在终值） ==")
status, _ = http("DELETE", f"/projections/{pid}")
assert status == 200, (status, _)
status, frozen = http("GET", f"/projections/{pid}/relationship?a={X1}&b={X3}")
check(status == 200 and frozen["status"] == "discarded", "已放弃推演应仍可查冻结视图", (status, frozen))
check(frozen["projection_revision"] == final_prevision, "冻结视图的推演修订号应为终值", frozen)
exp_rel, exp_path = expected_view(proj_truth[final_prevision], X1, X3)
check(frozen["relation"] == exp_rel and list(frozen["evidence_path"] or []) == exp_path,
      "冻结视图结论 / 路径应为关闭时终态", (frozen, exp_rel, exp_path))

# ------------------------------------------------------------------ 错误约定不变
print("== 错误约定：未知地层 400 INVALID_STRATUM；未知推演 404 PROJECTION_NOT_FOUND ==")
for path in (f"/relationship?a={X1}&b={TAG}-NOPE",
             f"/relationship?a={TAG}-NOPE&b={X2}",
             f"/projections/{pid}/relationship?a={X1}&b={TAG}-NOPE"):
    status, payload = http("GET", path)
    check(status == 400 and payload["error"]["code"] == "INVALID_STRATUM",
          f"未知地层应 400 INVALID_STRATUM: {path}", (status, payload))

status, payload = http("GET", f"/projections/999999999/relationship?a={X1}&b={X2}")
check(status == 404 and payload["error"]["code"] == "PROJECTION_NOT_FOUND",
      "未知推演应 404 PROJECTION_NOT_FOUND", (status, payload))

# ------------------------------------------------------------------ 汇总
print()
if failures:
    print(f"失败 {len(failures)} 项：")
    for msg, ctx in failures:
        print(" -", msg)
        print("   ", json.dumps(ctx, ensure_ascii=False, default=str)[:500])
    sys.exit(1)
print(f"全部一致性断言通过：正式 {len(formal_responses)} 条 + 推演 {len(proj_responses)} 条"
      f" 并发响应均对应单一完整视图；已关闭推演冻结视图与错误约定符合预期。")
PY
