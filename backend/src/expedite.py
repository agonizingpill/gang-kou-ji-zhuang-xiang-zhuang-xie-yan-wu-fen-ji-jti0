"""桥吊组加急单名额：班次上限、模型推荐名单、确认派工与手工派单。

规则要点：
- 每班（白班 06:00-18:00 / 夜班 18:00-06:00，按 UTC+8）一份加急名额，主管在调度页面填上限。
- 推荐名单按模型给出的严重延误概率（P(major)+P(critical)）从高到低取剩余名额个。
- 手工派的加急单同样占本班名额；名额按已发 grant 计数，完工不退。
- 名单指纹 list_version 随名单内容变化：晚到的分级记录挤掉原名单上的案件时，
  原名单作废，确认请求被拒绝，主管看过新名单再确认。
- 所有占名额的写操作都先锁本班班次行（PostgreSQL FOR UPDATE），两位主管同时提交也不超发。
- 确认与手工派单都带幂等键：中断后重试返回首次结果，不重复派工。
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.orm import Session

from .models import AuditLog, DelayCase, DispatchAction, ExpediteBatch, ExpediteGrant, ExpediteShift, User
from .security import current_user, get_db, require_roles

router = APIRouter(prefix="/api/expedite", tags=["桥吊加急"])

SHIFT_TZ = timezone(timedelta(hours=8), "CST")
SHIFT_DAY_START_HOUR = 6
SHIFT_HOURS = 12
# 模型没有输出 probabilities 时按等级折算的严重延误概率
SEVERE_FALLBACK = {"critical": 0.9, "major": 0.75, "watch": 0.25, "normal": 0.05}
ELIGIBLE_STATUS = {"classified", "dispatched"}
CASE_CLOSED_STATUS = {"closed", "rejected"}


def current_shift(now: datetime | None = None) -> dict:
    """按 UTC+8 把一天切成 06:00/18:00 两个班。"""
    moment = (now or datetime.now(timezone.utc)).astimezone(SHIFT_TZ)
    day = moment.date()
    if SHIFT_DAY_START_HOUR <= moment.hour < SHIFT_DAY_START_HOUR + SHIFT_HOURS:
        start = datetime(day.year, day.month, day.day, SHIFT_DAY_START_HOUR, tzinfo=SHIFT_TZ)
        label = "白班"
    elif moment.hour >= SHIFT_DAY_START_HOUR + SHIFT_HOURS:
        start = datetime(day.year, day.month, day.day, SHIFT_DAY_START_HOUR + SHIFT_HOURS, tzinfo=SHIFT_TZ)
        label = "夜班"
    else:
        prev = day - timedelta(days=1)
        start = datetime(prev.year, prev.month, prev.day, SHIFT_DAY_START_HOUR + SHIFT_HOURS, tzinfo=SHIFT_TZ)
        label = "夜班"
    end = start + timedelta(hours=SHIFT_HOURS)
    code = f"{start:%Y%m%d}-{'DAY' if label == '白班' else 'NIGHT'}"
    return {"code": code, "label": label, "start": start, "end": end}


def severe_probability(model_output: dict | None) -> float:
    """严重延误概率 = P(major) + P(critical)；缺概率明细时按等级折算。"""
    output = model_output or {}
    probs = output.get("probabilities")
    if isinstance(probs, dict) and probs:
        return round(float(probs.get("major") or 0) + float(probs.get("critical") or 0), 4)
    return SEVERE_FALLBACK.get(output.get("label"), 0.0)


def list_version(shift_code: str, quota_limit: int | None, used: int, recommended: list) -> str:
    """名单指纹：班次、上限、已用名额与推荐案件（含概率）任一变化即作废旧名单。"""
    payload = json.dumps(
        {
            "shift": shift_code,
            "quota": quota_limit,
            "used": used,
            "rec": [[row["case_id"], row["severe_probability"]] for row in recommended],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def log(db: Session, user: User, action: str, entity: str, entity_id=None, detail=""):
    db.add(AuditLog(actor=user.username, role=user.role, action=action, entity=entity, entity_id=entity_id, detail=detail))


def lock_shift(db: Session, shift_code: str) -> ExpediteShift:
    """取出本班班次行并加行锁；没有则先建行。所有占名额的写操作都走这里串行化。"""
    dialect = db.get_bind().dialect.name
    if dialect == "postgresql":
        db.execute(pg_dialect.insert(ExpediteShift).values(shift_code=shift_code).on_conflict_do_nothing(index_elements=["shift_code"]))
    elif dialect == "sqlite":
        db.execute(sqlite_dialect.insert(ExpediteShift).values(shift_code=shift_code).on_conflict_do_nothing(index_elements=["shift_code"]))
    elif not db.execute(select(ExpediteShift).where(ExpediteShift.shift_code == shift_code)).scalar_one_or_none():
        db.add(ExpediteShift(shift_code=shift_code))
    db.flush()
    stmt = select(ExpediteShift).where(ExpediteShift.shift_code == shift_code)
    if dialect == "postgresql":
        stmt = stmt.with_for_update()
    return db.execute(stmt).scalar_one()


def grant_dict(db: Session, grant: ExpediteGrant) -> dict:
    case = db.get(DelayCase, grant.case_id)
    action = db.get(DispatchAction, grant.action_id) if grant.action_id else None
    return {
        "grant_id": grant.id,
        "case_id": grant.case_id,
        "case_no": case.case_no if case else "",
        "vessel": case.vessel.call_no if case and case.vessel else "",
        "source": grant.source,
        "severe_probability": grant.severe_probability,
        "granted_by": grant.granted_by,
        "created_at": grant.created_at.isoformat() if grant.created_at else None,
        "action_id": grant.action_id,
        "action_status": action.status if action else None,
    }


def candidate_dict(case: DelayCase) -> dict:
    return {
        "case_id": case.id,
        "case_no": case.case_no,
        "vessel": case.vessel.call_no if case.vessel else "",
        "vessel_name": case.vessel.vessel_name if case.vessel else "",
        "final_level": case.final_level,
        "severe_probability": severe_probability(case.model_output),
    }


def build_board(db: Session, shift: ExpediteShift | None, shift_info: dict, user: User) -> dict:
    grants = []
    used = 0
    if shift is not None:
        rows = db.execute(
            select(ExpediteGrant).where(ExpediteGrant.shift_id == shift.id).order_by(ExpediteGrant.id)
        ).scalars().all()
        used = len(rows)
        grants = [grant_dict(db, g) for g in rows]
    quota = shift.quota_limit if shift is not None else None
    # 一个案件终身只占一次加急名额，跨班也不重复
    granted_case_ids = set(db.execute(select(ExpediteGrant.case_id)).scalars().all())
    candidates = []
    cases = db.execute(select(DelayCase).where(DelayCase.status.in_(ELIGIBLE_STATUS)).order_by(DelayCase.id)).scalars().all()
    for case in cases:
        if case.id in granted_case_ids or not case.model_output:
            continue
        candidates.append(candidate_dict(case))
    candidates.sort(key=lambda row: (-row["severe_probability"], row["case_id"]))
    remaining = max(0, quota - used) if quota is not None else 0
    recommended = candidates[:remaining]
    waiting = candidates[remaining:]
    return {
        "shift": {
            "code": shift_info["code"],
            "label": shift_info["label"],
            "start": shift_info["start"].isoformat(),
            "end": shift_info["end"].isoformat(),
        },
        "quota_limit": quota,
        "used": used,
        "remaining": remaining if quota is not None else None,
        "list_version": list_version(shift_info["code"], quota, used, recommended),
        "recommended": recommended,
        "waiting": waiting,
        "grants": grants,
        "can_manage": user.role == "supervisor",
    }


def find_shift(db: Session, shift_code: str) -> ExpediteShift | None:
    return db.execute(select(ExpediteShift).where(ExpediteShift.shift_code == shift_code)).scalar_one_or_none()


def create_expedite_action(db: Session, case: DelayCase, shift_info: dict, instruction: str) -> DispatchAction:
    action = DispatchAction(
        case_id=case.id,
        action_type="桥吊加急",
        assignee="桥吊组",
        due_at=shift_info["end"],
        status="queued",
        instruction=instruction,
    )
    case.status = "dispatched"
    case.updated_at = datetime.now(timezone.utc)
    db.add(action)
    db.flush()
    return action


def batch_result(db: Session, batch: ExpediteBatch, replayed: bool) -> dict:
    grants = db.execute(select(ExpediteGrant).where(ExpediteGrant.batch_id == batch.id).order_by(ExpediteGrant.id)).scalars().all()
    return {
        "batch_id": batch.id,
        "replayed": replayed,
        "confirmed_by": batch.confirmed_by,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
        "granted": [grant_dict(db, g) for g in grants],
    }


def find_batch_by_key(db: Session, key: str) -> ExpediteBatch | None:
    return db.execute(select(ExpediteBatch).where(ExpediteBatch.idempotency_key == key)).scalar_one_or_none()


class QuotaIn(BaseModel):
    quota_limit: int = Field(ge=0, le=99)


class ConfirmIn(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=64)
    list_version: str = Field(min_length=8, max_length=64)


class ManualIn(BaseModel):
    case_id: int
    idempotency_key: str = Field(min_length=8, max_length=64)
    note: str = Field(default="", max_length=200)


@router.get("/board")
def board(db: Session = Depends(get_db), user: User = Depends(current_user)):
    shift_info = current_shift()
    return build_board(db, find_shift(db, shift_info["code"]), shift_info, user)


@router.put("/quota")
def set_quota(payload: QuotaIn, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    shift_info = current_shift()
    shift = lock_shift(db, shift_info["code"])
    used = db.scalar(select(func.count(ExpediteGrant.id)).where(ExpediteGrant.shift_id == shift.id)) or 0
    if payload.quota_limit < used:
        raise HTTPException(status_code=409, detail=f"本班已发出 {used} 个加急名额，上限不能低于该值")
    shift.quota_limit = payload.quota_limit
    shift.updated_by = user.username
    shift.updated_at = datetime.now(timezone.utc)
    log(db, user, "set_expedite_quota", "expedite_shift", shift.id, f"{shift_info['code']} 上限={payload.quota_limit}")
    db.commit()
    return build_board(db, shift, shift_info, user)


@router.post("/confirm")
def confirm(payload: ConfirmIn, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    # 幂等回放：确认中断后重试，直接返回首次派工结果
    existing = find_batch_by_key(db, payload.idempotency_key)
    if existing:
        return batch_result(db, existing, replayed=True)
    shift_info = current_shift()
    shift = lock_shift(db, shift_info["code"])
    # 锁内复查：同键并发的另一个请求可能已经完成
    existing = find_batch_by_key(db, payload.idempotency_key)
    if existing:
        return batch_result(db, existing, replayed=True)
    if shift.quota_limit is None:
        raise HTTPException(status_code=409, detail="请先在调度页面填写本班加急上限")
    current = build_board(db, shift, shift_info, user)
    if current["list_version"] != payload.list_version:
        raise HTTPException(
            status_code=409,
            detail={"code": "LIST_STALE", "message": "推荐名单已作废：分级记录或名额有变化，请核对新名单后重新确认"},
        )
    if not current["recommended"]:
        raise HTTPException(status_code=409, detail="当前推荐名单为空，没有可确认的加急单")
    batch = ExpediteBatch(
        shift_id=shift.id,
        idempotency_key=payload.idempotency_key,
        list_version=payload.list_version,
        case_ids=[row["case_id"] for row in current["recommended"]],
        confirmed_by=user.username,
    )
    db.add(batch)
    db.flush()
    for row in current["recommended"]:
        case = db.get(DelayCase, row["case_id"])
        action = create_expedite_action(
            db, case, shift_info, f"推荐名单确认加急（严重延误概率 {row['severe_probability']:.0%}），桥吊组优先作业"
        )
        db.add(
            ExpediteGrant(
                shift_id=shift.id,
                case_id=case.id,
                batch_id=batch.id,
                action_id=action.id,
                source="recommend",
                severe_probability=row["severe_probability"],
                idempotency_key=f"{payload.idempotency_key}:{case.id}",
                granted_by=user.username,
            )
        )
    log(db, user, "confirm_expedite_batch", "expedite_batch", batch.id, f"{shift_info['code']} 确认 {len(current['recommended'])} 单")
    db.commit()
    return batch_result(db, batch, replayed=False)


@router.post("/manual")
def manual(payload: ManualIn, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    # 手工派单同样占本班名额，完工不退；幂等键防中断重试重复派工
    existing = db.execute(select(ExpediteGrant).where(ExpediteGrant.idempotency_key == payload.idempotency_key)).scalar_one_or_none()
    if existing:
        return {"grant": grant_dict(db, existing), "replayed": True}
    shift_info = current_shift()
    shift = lock_shift(db, shift_info["code"])
    existing = db.execute(select(ExpediteGrant).where(ExpediteGrant.idempotency_key == payload.idempotency_key)).scalar_one_or_none()
    if existing:
        return {"grant": grant_dict(db, existing), "replayed": True}
    case = db.get(DelayCase, payload.case_id)
    if not case:
        raise HTTPException(status_code=404, detail="延误案件不存在")
    if case.status in CASE_CLOSED_STATUS:
        raise HTTPException(status_code=409, detail="案件已关闭，不能派加急单")
    if db.execute(select(ExpediteGrant).where(ExpediteGrant.case_id == case.id)).scalar_one_or_none():
        raise HTTPException(status_code=409, detail="该案件已持有加急名额")
    if shift.quota_limit is None:
        raise HTTPException(status_code=409, detail="请先在调度页面填写本班加急上限")
    used = db.scalar(select(func.count(ExpediteGrant.id)).where(ExpediteGrant.shift_id == shift.id)) or 0
    if used >= shift.quota_limit:
        raise HTTPException(status_code=409, detail="本班加急名额已用完")
    action = create_expedite_action(db, case, shift_info, payload.note.strip() or "手工加急：桥吊组优先作业")
    grant = ExpediteGrant(
        shift_id=shift.id,
        case_id=case.id,
        action_id=action.id,
        source="manual",
        severe_probability=severe_probability(case.model_output),
        idempotency_key=payload.idempotency_key,
        granted_by=user.username,
    )
    db.add(grant)
    log(db, user, "manual_expedite_grant", "delay_case", case.id, f"{shift_info['code']} 手工加急 {case.case_no}")
    db.commit()
    return {"grant": grant_dict(db, grant), "replayed": False}
