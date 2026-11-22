"""桥吊加急名额的端到端验证：班次上限、推荐名单、名单作废、并发不超发、幂等重试、完工不退。

直接运行：cd backend && python tests/test_expedite.py
（用 SQLite 起真实 FastAPI 应用；晚到的分级记录通过直接写库模拟 worker 写回。）
"""

import os
import pathlib
import sys
import threading

DB_PATH = "/tmp/expedite_test.db"
pathlib.Path(DB_PATH).unlink(missing_ok=True)
os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
os.environ["SEED_DEMO"] = "true"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import event, select  # noqa: E402

from src.database import SessionLocal, engine  # noqa: E402
from src.main import app  # noqa: E402
from src.models import DelayCase, DispatchAction, ExpediteGrant, VesselCall  # noqa: E402


@event.listens_for(engine, "begin")
def _begin_immediate(conn):
    # SQLite 测试库用 BEGIN IMMEDIATE 让并发写串行化，贴近 PostgreSQL 行锁语义
    if conn.dialect.name == "sqlite":
        conn.exec_driver_sql("BEGIN IMMEDIATE")


PASS = []


def ok(name, cond, extra=""):
    assert cond, f"FAILED: {name} {extra}"
    PASS.append(name)
    print(f"  PASS {name}")


def login(client, username, password):
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def make_classified(case_no, critical, major=0.0):
    """模拟 worker 写回的晚到分级记录。"""
    db = SessionLocal()
    try:
        vessel = db.execute(select(VesselCall).where(VesselCall.call_no == "CALL-HAI-20261121")).scalar_one()
        label = "critical" if critical >= major else "major"
        case = DelayCase(
            vessel_id=vessel.id, case_no=case_no, status="classified", reason="晚到的分级记录",
            final_level=label,
            model_output={"label": label, "confidence": max(critical, major),
                          "probabilities": {"critical": critical, "major": major, "watch": 0.01, "normal": 0.0}},
        )
        db.add(case)
        db.commit()
        return case.id
    finally:
        db.close()


def grant_count():
    db = SessionLocal()
    try:
        return len(db.execute(select(ExpediteGrant)).scalars().all())
    finally:
        db.close()


def action_count():
    db = SessionLocal()
    try:
        return len(db.execute(select(DispatchAction).where(DispatchAction.action_type == "桥吊加急")).scalars().all())
    finally:
        db.close()


def run(client):
    sup = login(client, "supervisor", "supervisor123")
    op = login(client, "operator", "operator123")
    view = login(client, "viewer", "viewer123")

    print("-- 看板与上限 --")
    board = client.get("/api/expedite/board", headers=sup).json()
    ok("初始未设上限", board["quota_limit"] is None)
    ok("初始推荐名单为空", board["recommended"] == [])
    ok("已分级案件在等待区", [w["case_no"] for w in board["waiting"]] == ["CASE-HAI-001"])
    ok("严重延误概率按等级折算", board["waiting"][0]["severe_probability"] == 0.75)

    ok("观察员不能设上限", client.put("/api/expedite/quota", json={"quota_limit": 1}, headers=view).status_code == 403)
    ok("操作员不能设上限", client.put("/api/expedite/quota", json={"quota_limit": 1}, headers=op).status_code == 403)
    ok("上限不能为负", client.put("/api/expedite/quota", json={"quota_limit": -1}, headers=sup).status_code == 422)
    r = client.post("/api/expedite/confirm", json={"idempotency_key": "early-key-1", "list_version": "x" * 16}, headers=sup)
    ok("未设上限不能确认", r.status_code == 409 and "上限" in r.json()["detail"])

    board = client.put("/api/expedite/quota", json={"quota_limit": 1}, headers=sup).json()
    ok("主管填本班上限", board["quota_limit"] == 1 and board["remaining"] == 1)
    ok("推荐名单取剩余名额", [r["case_no"] for r in board["recommended"]] == ["CASE-HAI-001"])
    v1 = board["list_version"]

    print("-- 确认派工 / 幂等 / 名单作废 --")
    before_actions = action_count()
    r = client.post("/api/expedite/confirm", json={"idempotency_key": "batch-key-1", "list_version": v1}, headers=sup)
    ok("确认推荐名单", r.status_code == 200 and not r.json()["replayed"], r.text)
    granted = r.json()["granted"]
    ok("名单案件拿到名额", [g["case_no"] for g in granted] == ["CASE-HAI-001"])
    ok("确认即派工", action_count() == before_actions + 1)

    r = client.post("/api/expedite/confirm", json={"idempotency_key": "batch-key-1", "list_version": v1}, headers=sup)
    ok("中断重试返回首次结果", r.status_code == 200 and r.json()["replayed"] and r.json()["batch_id"] is not None)
    ok("重试不重复派工", action_count() == before_actions + 1 and grant_count() == 1)

    r = client.post("/api/expedite/confirm", json={"idempotency_key": "batch-key-2", "list_version": v1}, headers=sup)
    ok("旧名单确认被拒绝", r.status_code == 409 and r.json()["detail"]["code"] == "LIST_STALE")

    board = client.get("/api/expedite/board", headers=sup).json()
    ok("名额已用 1", board["used"] == 1 and board["remaining"] == 0)
    ok("拿到名额的进入已发名单", [g["case_no"] for g in board["grants"]] == ["CASE-HAI-001"])

    print("-- 手工派单占名额、完工不退 --")
    r = client.post("/api/expedite/manual", json={"case_id": 2, "idempotency_key": "manual-key-1"}, headers=sup)
    ok("名额满了手工单也被拒", r.status_code == 409 and "用完" in r.json()["detail"])
    ok("上限不能低于已发", client.put("/api/expedite/quota", json={"quota_limit": 0}, headers=sup).status_code == 409)

    client.put("/api/expedite/quota", json={"quota_limit": 2}, headers=sup)
    r = client.post("/api/expedite/manual", json={"case_id": 2, "idempotency_key": "manual-key-1"}, headers=sup)
    ok("手工派单成功", r.status_code == 200 and r.json()["grant"]["source"] == "manual", r.text)
    manual_action = r.json()["grant"]["action_id"]
    r = client.post("/api/expedite/manual", json={"case_id": 2, "idempotency_key": "manual-key-1"}, headers=sup)
    ok("手工单重试不重复派工", r.json()["replayed"] and grant_count() == 2)
    r = client.post("/api/expedite/manual", json={"case_id": 2, "idempotency_key": "manual-key-2"}, headers=sup)
    ok("同一案件不重复占名额", r.status_code == 409)

    client.post(f"/api/actions/{manual_action}/complete", headers=op)
    board = client.get("/api/expedite/board", headers=sup).json()
    ok("完工不退名额", board["used"] == 2 and board["remaining"] == 0)
    ok("已发名单能看到完工状态", board["grants"][1]["action_status"] == "completed")

    print("-- 晚到分级记录挤掉原名单 --")
    client.put("/api/expedite/quota", json={"quota_limit": 3}, headers=sup)
    make_classified("CASE-LATE-001", critical=0.95)
    board = client.get("/api/expedite/board", headers=sup).json()
    ok("晚到记录进入推荐名单", [r["case_no"] for r in board["recommended"]] == ["CASE-LATE-001"])
    v2 = board["list_version"]

    make_classified("CASE-LATE-002", critical=0.99)
    board = client.get("/api/expedite/board", headers=sup).json()
    ok("更严重的记录挤掉原名单", [r["case_no"] for r in board["recommended"]] == ["CASE-LATE-002"])
    ok("被挤掉的案件还在等", [w["case_no"] for w in board["waiting"]] == ["CASE-LATE-001"])
    v3 = board["list_version"]
    ok("名单指纹随名单变化", len({v1, v2, v3}) == 3)

    r = client.post("/api/expedite/confirm", json={"idempotency_key": "batch-key-3", "list_version": v2}, headers=sup)
    ok("被挤掉的原名单作废", r.status_code == 409 and r.json()["detail"]["code"] == "LIST_STALE")
    r = client.post("/api/expedite/confirm", json={"idempotency_key": "batch-key-3", "list_version": v3}, headers=sup)
    ok("看过新名单后确认成功", r.status_code == 200 and [g["case_no"] for g in r.json()["granted"]] == ["CASE-LATE-002"])

    print("-- 两位主管同时提交不超发 --")
    client.put("/api/expedite/quota", json={"quota_limit": 5}, headers=sup)
    make_classified("CASE-LATE-003", critical=0.5)
    make_classified("CASE-LATE-004", critical=0.4)
    board = client.get("/api/expedite/board", headers=sup).json()
    ok("剩余 2 个名额全进推荐", [r["case_no"] for r in board["recommended"]] == ["CASE-LATE-001", "CASE-LATE-003"], board["recommended"])
    v4 = board["list_version"]
    expected = len(board["recommended"])

    results = {}

    def confirm_as(name, key):
        headers = login(client, "supervisor", "supervisor123")
        r = client.post("/api/expedite/confirm", json={"idempotency_key": key, "list_version": v4}, headers=headers)
        results[name] = r.status_code

    threads = [threading.Thread(target=confirm_as, args=("A", "race-key-A")), threading.Thread(target=confirm_as, args=("B", "race-key-B"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok("并发确认只有一人成功", sorted(results.values()) == [200, 409], results)
    ok("并发不超发", grant_count() == 3 + expected and action_count() == 3 + expected)

    same_key = {}

    def confirm_same(name):
        headers = login(client, "supervisor", "supervisor123")
        r = client.post("/api/expedite/confirm", json={"idempotency_key": "race-key-same", "list_version": v4}, headers=headers)
        same_key[name] = (r.status_code, r.json().get("batch_id"))

    # 同键并发：名单已过期，两者都应得到同一个 409 或同一批结果，且不产生新批次
    before = grant_count()
    threads = [threading.Thread(target=confirm_same, args=("A",)), threading.Thread(target=confirm_same, args=("B",))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok("同键并发不产生重复派工", grant_count() == before)

    board = client.get("/api/expedite/board", headers=view).json()
    ok("观察员能看名单不能操作", board["can_manage"] is False)
    ok("调度页能看到拿到名额与还在等", len(board["grants"]) == 3 + expected and
       [w["case_no"] for w in board["waiting"]] == ["CASE-LATE-004"], board["waiting"])


def main():
    with TestClient(app) as client:
        run(client)
    print(f"\n全部 {len(PASS)} 项检查通过")


def test_expedite_flow():
    main()


if __name__ == "__main__":
    main()
