"""
langfuse_to_oracle.py

Langfuse(v3.194.1)의 observation 데이터를 추출하여 Oracle DB의 TRX_TOKEN_DET
테이블에 적재하는 1회성 배치 스크립트.

설계 근거: design.md 각 섹션을 참고. 코드 내 주석에 해당 섹션 번호를 표기했다.

사용법:
    python langfuse_to_oracle.py --from 2026-08-30T00:00:00 --to 2026-08-30T06:00:00

    --from / --to 는 KST(한국시간, UTC+9) 기준의 naive datetime(ISO 8601)이다.
    프로그램은 이 범위를 .env의 WINDOW_MINUTES(기본 10분) 단위로 분할하여
    윈도우별로 순차 추출·적재한다. (design.md §5.1)

필요 환경변수(.env, design.md §8):
    LANGFUSE_HOST, LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY
    ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD
    WINDOW_MINUTES (선택, 기본값 10)

필요 패키지: langfuse, oracledb, python-dotenv, tzdata(Windows 필수) — README.md 참고
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv

try:
    import oracledb
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "python-oracledb가 설치되어 있지 않습니다. `pip install oracledb`로 설치하세요."
    ) from exc

try:
    from langfuse import Langfuse
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "langfuse SDK가 설치되어 있지 않습니다. `pip install langfuse`로 설치하세요."
    ) from exc


# ============================================================================
# 상수 (design.md §4.1, §5.2)
# ============================================================================
NODE_NM = "AXgenticCode"          # 요구사항 고정값
USER_ID_FIXED = "TBD"             # 요구사항 고정값
TAG_FILTER = "project:1"          # observation(trace) tags 필터
OBSERVATION_TYPE = "GENERATION"   # 토큰 사용량이 존재하는 타입만 대상

try:
    KST = ZoneInfo("Asia/Seoul")
    UTC = ZoneInfo("UTC")
except ZoneInfoNotFoundError as exc:  # pragma: no cover
    # Windows는 OS에 IANA 시간대 데이터베이스가 내장되어 있지 않아 발생한다.
    raise SystemExit(
        "시간대(Asia/Seoul) 정보를 찾을 수 없습니다. "
        "Windows에서는 `pip install tzdata`로 시간대 데이터베이스를 설치해야 합니다."
    ) from exc

QUERY_CTN_MAX_BYTES = 4000        # TRX_TOKEN_DET.QUERY_CTN VARCHAR2(4000)
ERR_CTN_MAX_BYTES = 1000          # TRX_TOKEN_DET.ERR_CTN VARCHAR2(1000)

PAGE_LIMIT = 100
MAX_API_RETRIES = 3
RETRY_BACKOFF_BASE_SEC = 2

LOG = logging.getLogger("langfuse_to_oracle")


# ============================================================================
# 설정 (design.md §8)
# ============================================================================
@dataclass
class Config:
    langfuse_host: str
    langfuse_public_key: str
    langfuse_secret_key: str
    oracle_dsn: str
    oracle_user: str
    oracle_password: str
    window_minutes: int


def load_config() -> Config:
    load_dotenv()

    def _require(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise SystemExit(f"필수 환경변수 {name}가 .env에 설정되어 있지 않습니다.")
        return value

    window_minutes_raw = os.getenv("WINDOW_MINUTES", "10")
    try:
        window_minutes = int(window_minutes_raw)
        if window_minutes <= 0:
            raise ValueError
    except ValueError as exc:
        raise SystemExit(
            f"WINDOW_MINUTES는 1 이상의 정수여야 합니다 (현재 값: {window_minutes_raw!r})"
        ) from exc

    return Config(
        langfuse_host=_require("LANGFUSE_HOST"),
        langfuse_public_key=_require("LANGFUSE_PUBLIC_KEY"),
        langfuse_secret_key=_require("LANGFUSE_SECRET_KEY"),
        oracle_dsn=_require("ORACLE_DSN"),
        oracle_user=_require("ORACLE_USER"),
        oracle_password=_require("ORACLE_PASSWORD"),
        window_minutes=window_minutes,
    )


# ============================================================================
# 로깅 (design.md §11)
# ============================================================================
def setup_logging() -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{datetime.now(KST):%Y%m%d}.log"

    logger = logging.getLogger("langfuse_to_oracle")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    return logger


# ============================================================================
# 시간 유틸 (design.md §4.5, §5.1)
# ============================================================================
def parse_kst_datetime(value: str) -> datetime:
    """CLI로 입력받은 naive datetime 문자열을 KST-aware datetime으로 변환한다."""
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"'{value}'는 올바른 ISO 8601 형식이 아닙니다 (예: 2026-08-30T00:00:00)"
        ) from exc
    if dt.tzinfo is not None:
        raise argparse.ArgumentTypeError(
            f"'{value}'에 타임존 정보를 포함하지 마세요. KST 기준의 naive datetime만 입력하세요."
        )
    return dt.replace(tzinfo=KST)


def to_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC)


def to_kst(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(KST)


def split_into_windows(
    start: datetime, end: datetime, minutes: int
) -> list[tuple[datetime, datetime]]:
    """[start, end) 구간을 minutes 단위 반개구간 윈도우 리스트로 분할한다. (design.md §5.1)"""
    if start >= end:
        raise ValueError("--from은 --to보다 이전이어야 합니다.")
    windows: list[tuple[datetime, datetime]] = []
    cur = start
    step = timedelta(minutes=minutes)
    while cur < end:
        nxt = min(cur + step, end)
        windows.append((cur, nxt))
        cur = nxt
    return windows


# ============================================================================
# 공통 유틸
# ============================================================================
def _get(obj: Any, name: str) -> Any:
    """SDK 응답 객체(Pydantic 모델) / dict 양쪽 모두에서 안전하게 값을 꺼낸다."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def truncate_utf8(text: Optional[str], max_bytes: int) -> Optional[str]:
    """UTF-8 인코딩 byte 길이가 max_bytes를 넘지 않도록 안전하게 자른다. (design.md §4.2)"""
    if text is None:
        return None
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def parse_iso(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


# ============================================================================
# QUERY_CTN 파싱 (design.md §4.2)
# ============================================================================
def extract_query_ctn(raw_input: Any) -> Optional[str]:
    """observation.input에서 사용자 질의 텍스트를 추출한다."""
    if raw_input is None:
        return None

    if isinstance(raw_input, str):
        text = raw_input
    elif isinstance(raw_input, list):
        user_messages = [m for m in raw_input if isinstance(m, dict) and m.get("role") == "user"]
        if user_messages:
            content = user_messages[-1].get("content")
            text = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
        else:
            text = json.dumps(raw_input, ensure_ascii=False)
    else:
        try:
            text = json.dumps(raw_input, ensure_ascii=False)
        except TypeError:
            text = str(raw_input)

    return truncate_utf8(text, QUERY_CTN_MAX_BYTES)


# ============================================================================
# usage(토큰) 추출 (design.md §3.2 오픈 이슈 1)
#   v1 usage.{input,output,total} 형태와 레거시 usage.{promptTokens,...} 형태를
#   모두 지원한다. 실제 응답 형태는 배포 환경에서 1건 확인 후 필요 시 조정한다.
# ============================================================================
def extract_usage(observation: Any) -> tuple[int, int, int]:
    usage = _get(observation, "usage")

    input_tokens = _get(usage, "input")
    if input_tokens is None:
        input_tokens = _get(usage, "prompt_tokens")
    if input_tokens is None:
        input_tokens = _get(usage, "promptTokens")

    output_tokens = _get(usage, "output")
    if output_tokens is None:
        output_tokens = _get(usage, "completion_tokens")
    if output_tokens is None:
        output_tokens = _get(usage, "completionTokens")

    total_tokens = _get(usage, "total")
    if total_tokens is None:
        total_tokens = _get(usage, "total_tokens")
    if total_tokens is None:
        total_tokens = _get(usage, "totalTokens")

    input_tokens = int(input_tokens) if input_tokens is not None else 0
    output_tokens = int(output_tokens) if output_tokens is not None else 0
    total_tokens = int(total_tokens) if total_tokens is not None else (input_tokens + output_tokens)

    return input_tokens, output_tokens, total_tokens


# ============================================================================
# observation → TRX_TOKEN_DET row 변환 (design.md §3.2, §4)
# ============================================================================
class TransformSkip(Exception):
    """해당 observation을 이번 배치에서 제외해야 함을 나타낸다 (예: endTime 없음)."""


def transform_observation(observation: Any) -> dict:
    observation_id = _get(observation, "id")
    trace_id = _get(observation, "trace_id") or _get(observation, "traceId")

    if not observation_id or not trace_id:
        raise TransformSkip("observation_id 또는 trace_id가 없어 PK를 구성할 수 없습니다.")

    start_time = parse_iso(_get(observation, "start_time") or _get(observation, "startTime"))
    end_time = parse_iso(_get(observation, "end_time") or _get(observation, "endTime"))

    if end_time is None:
        # design.md §4.3: endTime이 없는 진행 중 observation은 이번 윈도우에서 제외한다.
        # (체크포인트/재처리 큐가 없으므로 "이연"은 채택하지 않음 — §13 오픈 이슈)
        raise TransformSkip(f"endTime이 없어 제외됨 (observation_id={observation_id})")

    if start_time is None:
        raise TransformSkip(f"startTime이 없어 제외됨 (observation_id={observation_id})")

    latency_ms = int((end_time - start_time).total_seconds() * 1000)  # design.md §4.3
    call_tm = to_kst(start_time).replace(tzinfo=None)  # design.md §4.5, Oracle TIMESTAMP는 tz-naive 바인딩

    input_tokens, output_tokens, total_tokens = extract_usage(observation)

    model_nm = _get(observation, "model")
    query_ctn = extract_query_ctn(_get(observation, "input"))

    level = (_get(observation, "level") or "DEFAULT")
    level = level.upper() if isinstance(level, str) else "DEFAULT"
    status_message = _get(observation, "status_message") or _get(observation, "statusMessage")

    if level == "ERROR":
        stat_cd = "ERROR"
        err_ctn = truncate_utf8(status_message, ERR_CTN_MAX_BYTES) if status_message else None
    else:
        stat_cd = "OK"
        err_ctn = None

    return {
        "trace_id": trace_id,
        "observation_id": observation_id,
        "node_nm": NODE_NM,
        "model_nm": model_nm,
        "user_id": USER_ID_FIXED,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "latency_ms": latency_ms,
        "call_tm": call_tm,
        "query_ctn": query_ctn,
        "stat_cd": stat_cd,
        "err_ctn": err_ctn,
    }


def safe_transform(observation: Any) -> Optional[dict]:
    """레코드 단위 오류 처리. (design.md §7)

    - endTime 없음 등 정상적으로 스킵해야 하는 경우: None 반환, INFO 로그.
    - 예상치 못한 파싱 오류: 가능하면 STAT_CD='ERROR' + ERR_CTN에
      '[ETL_PARSE_ERROR]' 접두사를 붙여 적재(가시성 확보). PK(trace_id,
      observation_id)조차 구성 불가능하면 스킵한다.
    """
    try:
        return transform_observation(observation)
    except TransformSkip as exc:
        LOG.info("SKIP: %s", exc)
        return None
    except Exception as exc:  # noqa: BLE001 - 의도적으로 광범위하게 포착
        observation_id = _get(observation, "id")
        trace_id = _get(observation, "trace_id") or _get(observation, "traceId")
        if not observation_id or not trace_id:
            LOG.error("파싱 실패 + PK 구성 불가로 스킵: %s", exc)
            return None
        LOG.warning("파싱 오류(observation_id=%s): %s", observation_id, exc)
        return {
            "trace_id": trace_id,
            "observation_id": observation_id,
            "node_nm": NODE_NM,
            "model_nm": _get(observation, "model"),
            "user_id": USER_ID_FIXED,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "latency_ms": None,
            "call_tm": None,
            "query_ctn": None,
            "stat_cd": "ERROR",
            "err_ctn": truncate_utf8(f"[ETL_PARSE_ERROR] {exc}", ERR_CTN_MAX_BYTES),
        }


# ============================================================================
# Langfuse API 조회 (design.md §5.2, §5.3)
# ============================================================================
def with_retries(description: str, func, *args, **kwargs):
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = RETRY_BACKOFF_BASE_SEC ** attempt
            LOG.warning(
                "%s 실패 (%d/%d회): %s — %d초 후 재시도",
                description, attempt, MAX_API_RETRIES, exc, wait,
            )
            time.sleep(wait)
    LOG.error("%s 최종 실패: %s", description, last_exc)
    raise last_exc  # type: ignore[misc]


def fetch_trace_ids(client: Langfuse, window_start_utc: datetime, window_end_utc: datetime) -> list[str]:
    """'project:1' 태그가 포함된 trace id 목록을 조회한다. (design.md §5.2 1단계)

    NOTE: `tags` 파라미터명/문법은 design.md §13 오픈 이슈로 남아있다.
    실제 SDK 버전에서 시그니처가 다르면 이 함수만 수정하면 된다.
    """
    trace_ids: list[str] = []
    page = 1
    while True:
        response = with_retries(
            f"traces 조회(page={page})",
            client.api.trace.list,
            tags=[TAG_FILTER],
            from_timestamp=window_start_utc,
            to_timestamp=window_end_utc,
            page=page,
            limit=PAGE_LIMIT,
        )
        data = _get(response, "data") or []
        trace_ids.extend(tid for t in data if (tid := _get(t, "id")))

        meta = _get(response, "meta")
        total_pages = _get(meta, "total_pages") or _get(meta, "totalPages") or 1
        if page >= total_pages or not data:
            break
        page += 1
    return trace_ids


def _get_observations_v1_client(client: Langfuse) -> Any:
    """v1 Observations API 클라이언트를 SDK 버전에 상관없이 찾아 반환한다.

    langfuse SDK 4.x부터 v1 엔드포인트가 `client.api.legacy.observations_v1`로
    이동했다(`client.api.observations`는 v2로 바뀌어 `page` 대신 `cursor`
    기반 페이지네이션을 사용하므로 그대로 쓰면 `unexpected keyword argument
    'page'` 오류가 난다). 4.x 이전 SDK에서는 `client.api.observations` 자체가
    v1이었으므로 그 경로로 폴백한다.
    """
    legacy = getattr(client.api, "legacy", None)
    observations_v1 = getattr(legacy, "observations_v1", None) if legacy else None
    if observations_v1 is not None:
        return observations_v1
    return client.api.observations  # langfuse SDK < 4.0 (v1이 기본 경로였음)


def fetch_observations(
    client: Langfuse, trace_id: str, window_start_utc: datetime, window_end_utc: datetime
) -> list[Any]:
    """지정 trace의 GENERATION observation을 윈도우 시간 범위 내에서 조회한다. (design.md §5.2 2단계)

    2단계 조회에도 윈도우 시간(fromStartTime/toStartTime)을 반드시 함께 지정한다 —
    그렇지 않으면 trace에 속한 다른 윈도우 시간대의 observation까지 함께 적재되어
    윈도우별 반개구간 처리 원칙이 깨진다. (design.md §5.2 경고 참고)
    """
    observations_client = _get_observations_v1_client(client)
    observations: list[Any] = []
    page = 1
    while True:
        response = with_retries(
            f"observations 조회(trace_id={trace_id}, page={page})",
            observations_client.get_many,
            trace_id=trace_id,
            type=OBSERVATION_TYPE,
            from_start_time=window_start_utc,
            to_start_time=window_end_utc,
            page=page,
            limit=PAGE_LIMIT,
        )
        data = _get(response, "data") or []
        observations.extend(data)

        meta = _get(response, "meta")
        total_pages = _get(meta, "total_pages") or _get(meta, "totalPages") or 1
        if page >= total_pages or not data:
            break
        page += 1
    return observations


# ============================================================================
# Oracle 적재 (design.md §6)
# ============================================================================
MERGE_SQL = """
MERGE INTO TRX_TOKEN_DET tgt
USING (
    SELECT :trace_id AS trace_id, :observation_id AS observation_id,
           :node_nm AS node_nm, :model_nm AS model_nm, :user_id AS user_id,
           :input_tokens AS input_tokens, :output_tokens AS output_tokens,
           :total_tokens AS total_tokens, :latency_ms AS latency_ms,
           :call_tm AS call_tm, :query_ctn AS query_ctn,
           :stat_cd AS stat_cd, :err_ctn AS err_ctn
    FROM dual
) src
ON (tgt.TRACE_ID = src.trace_id AND tgt.OBSERVATION_ID = src.observation_id)
WHEN MATCHED THEN UPDATE SET
    tgt.NODE_NM = src.node_nm, tgt.MODEL_NM = src.model_nm, tgt.USER_ID = src.user_id,
    tgt.INPUT_TOKENS = src.input_tokens, tgt.OUTPUT_TOKENS = src.output_tokens,
    tgt.TOTAL_TOKENS = src.total_tokens, tgt.LATENCY_MS = src.latency_ms,
    tgt.CALL_TM = src.call_tm, tgt.QUERY_CTN = src.query_ctn,
    tgt.STAT_CD = src.stat_cd, tgt.ERR_CTN = src.err_ctn
WHEN NOT MATCHED THEN INSERT
    (TRACE_ID, OBSERVATION_ID, NODE_NM, MODEL_NM, USER_ID, INPUT_TOKENS, OUTPUT_TOKENS,
     TOTAL_TOKENS, LATENCY_MS, CALL_TM, REG_DT, QUERY_CTN, STAT_CD, ERR_CTN)
    VALUES
    (src.trace_id, src.observation_id, src.node_nm, src.model_nm, src.user_id,
     src.input_tokens, src.output_tokens, src.total_tokens, src.latency_ms,
     src.call_tm, SYSTIMESTAMP, src.query_ctn, src.stat_cd, src.err_ctn)
"""


def merge_rows(connection: "oracledb.Connection", rows: list[dict]) -> None:
    """rows를 MERGE로 일괄 upsert하고 커밋한다."""
    if not rows:
        return
    with connection.cursor() as cursor:
        cursor.executemany(MERGE_SQL, rows)
    connection.commit()


# ============================================================================
# 윈도우 단위 처리 (design.md §5.1, §6, §7)
# ============================================================================
def process_window(
    client: Langfuse,
    connection: "oracledb.Connection",
    window_start_kst: datetime,
    window_end_kst: datetime,
) -> tuple[int, int]:
    window_start_utc = to_utc(window_start_kst)
    window_end_utc = to_utc(window_end_kst)

    LOG.info("윈도우 처리 시작: %s ~ %s (KST)", window_start_kst, window_end_kst)

    trace_ids = fetch_trace_ids(client, window_start_utc, window_end_utc)
    LOG.info("  tags='%s' 매칭 trace 수: %d", TAG_FILTER, len(trace_ids))

    rows: list[dict] = []
    skipped = 0
    for trace_id in trace_ids:
        observations = fetch_observations(client, trace_id, window_start_utc, window_end_utc)
        for observation in observations:
            row = safe_transform(observation)
            if row is None:
                skipped += 1
            else:
                rows.append(row)

    merge_rows(connection, rows)
    LOG.info("  적재 완료: %d건 (스킵 %d건)", len(rows), skipped)
    return len(rows), skipped


# ============================================================================
# CLI / main (design.md §5.1, §10)
# ============================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Langfuse observation을 추출하여 Oracle TRX_TOKEN_DET에 적재합니다."
    )
    parser.add_argument(
        "--from",
        dest="from_time",  # 'from'은 파이썬 예약어이므로 dest를 별도 지정 (design.md §5.1 유의사항)
        required=True,
        type=parse_kst_datetime,
        help="추출 시작 시각, KST 기준 naive ISO 8601 (예: 2026-08-30T00:00:00)",
    )
    parser.add_argument(
        "--to",
        dest="to_time",
        required=True,
        type=parse_kst_datetime,
        help="추출 종료 시각, KST 기준 naive ISO 8601 (예: 2026-08-30T06:00:00)",
    )
    return parser


def main() -> int:
    global LOG
    LOG = setup_logging()

    args = build_arg_parser().parse_args()
    config = load_config()

    try:
        windows = split_into_windows(args.from_time, args.to_time, config.window_minutes)
    except ValueError as exc:
        LOG.error(str(exc))
        return 1

    LOG.info(
        "배치 시작: from=%s to=%s (KST), WINDOW_MINUTES=%d, 총 %d개 윈도우",
        args.from_time, args.to_time, config.window_minutes, len(windows),
    )

    client = Langfuse(
        public_key=config.langfuse_public_key,
        secret_key=config.langfuse_secret_key,
        host=config.langfuse_host,
    )

    connection = oracledb.connect(
        user=config.oracle_user,
        password=config.oracle_password,
        dsn=config.oracle_dsn,
    )

    total_loaded = 0
    total_skipped = 0
    try:
        for idx, (window_start, window_end) in enumerate(windows, start=1):
            LOG.info("[%d/%d] 윈도우 처리 중", idx, len(windows))
            try:
                loaded, skipped = process_window(client, connection, window_start, window_end)
            except Exception:
                LOG.exception(
                    "윈도우 처리 중 치명적 오류로 배치를 중단합니다 (window=%s~%s). "
                    "동일한 --from/--to로 재실행하면 MERGE에 의해 안전하게 재처리됩니다.",
                    window_start, window_end,
                )
                return 1
            total_loaded += loaded
            total_skipped += skipped
    finally:
        connection.close()

    LOG.info("배치 종료: 총 적재 %d건, 스킵 %d건", total_loaded, total_skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
