# ExtractObservation2Oracle 설계 문서

## 1. 개요

Langfuse(v3.194.1)에 적재된 observation 데이터를 주기적으로 추출하여, Oracle DB의 `TRX_TOKEN_DET` 테이블에 적재하는 Python 배치 프로그램을 설계한다.

## 2. Langfuse 연동 정보

| 항목 | 내용 |
|---|---|
| Langfuse 버전 | 3.194.1 |
| 추출 대상 API | Traces API + Observations API **v1** (둘 다 legacy, §5.2 참고) |
| 인증 | `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY` (Basic Auth), `LANGFUSE_HOST` |
| 호출 방식 | Langfuse Python SDK v1 API 2종을 조합: `langfuse.api.trace.list(...)`(tags 필터용, §5.2 1단계), `langfuse.api.observations.get_many(...)`(§5.2 2단계) |
| 조회 대상 type | `GENERATION` (토큰 사용량이 존재하는 observation만 대상) |
| 필드 선택 | v1은 `fields` 파라미터가 없으며, 응답에 전체 필드가 항상 포함된다(그룹 선택 불가). |
| 주요 조회 파라미터 (traces) | `tags`, `fromTimestamp`, `toTimestamp`, `page`, `limit` (§5.2 1단계) |
| 주요 조회 파라미터 (observations) | `traceId`, `type`, `fromStartTime`, `toStartTime`, `page`, `limit` (§5.2 2단계) |
| tags 필터 | `'project:1'` 태그가 포함된 항목만 추출 — trace 단위로 필터링 후 해당 trace의 observation을 조회 (§5.2 참고) |

> **참고 (v1 사용 시 유의사항)**: v1 Observations API는 Langfuse Cloud 기준으로는 deprecated(2026-11-16까지 서빙)이나, 본 프로젝트는 **자체 호스팅(설치형) v3.194.1**을 사용하므로 해당 서빙 종료 일정과 무관하게 v1을 계속 사용한다. 다만 향후 Langfuse 버전을 업그레이드할 경우 v1 엔드포인트가 해당 버전에서도 유지되는지는 업그레이드 시점에 릴리스 노트로 확인이 필요하다.

## 3. 대상 테이블

### 3.1 테이블 정의 (기 생성됨 — 구조 확정)

`TRX_TOKEN_DET`은 이미 DB에 생성되어 있는 테이블이다. 아래 DDL은 **실제 운영 테이블 구조를 기록한 스냅샷**이며, 본 프로젝트 범위에서는 컬럼 추가/변경/타입 조정을 하지 않고 이 구조를 그대로 대상으로 코드를 작성한다. (테이블을 신규 생성하는 스크립트가 아니므로 `CREATE TABLE`을 실행하지 않는다.)

```sql
CREATE TABLE TRX_TOKEN_DET (
    TRACE_ID       VARCHAR2(50),
    NODE_NM        VARCHAR2(50),
    MODEL_NM       VARCHAR2(100),
    USER_ID        VARCHAR2(50),
    INPUT_TOKENS   NUMBER,
    OUTPUT_TOKENS  NUMBER,
    TOTAL_TOKENS   NUMBER,
    LATENCY_MS     NUMBER,
    CALL_TM        TIMESTAMP,
    REG_DT         TIMESTAMP DEFAULT SYSTIMESTAMP,
    QUERY_CTN      VARCHAR2(4000),
    STAT_CD        VARCHAR2(10),
    ERR_CTN        VARCHAR2(1000),
    OBSERVATION_ID VARCHAR2(50),
    CONSTRAINT PK_TRX_TOKEN_DET PRIMARY KEY (TRACE_ID, OBSERVATION_ID)
);
```

> 위 컬럼 타입/길이는 기 생성된 테이블의 확정값이며, 변경 대상이 아니다.
>
> ⚠️ Oracle `VARCHAR2(n)`은 기본적으로 **byte 길이** 기준이다(`NLS_LENGTH_SEMANTICS` 또는 `VARCHAR2(n CHAR)` 미지정 시). 한글 등 멀티바이트 문자(AL32UTF8 기준 한글 1자=3byte)가 섞인 `QUERY_CTN`(4000byte), `ERR_CTN`(1000byte)을 각각의 문자 수 기준으로 자르면 byte 초과로 `ORA-12899`가 발생할 수 있으므로, 테이블 구조 변경 없이 **Python 쪽에서 UTF-8 인코딩 byte 길이가 컬럼 한도를 넘지 않도록 안전하게 truncate**하는 로직으로 대응한다(§4.2, §4.4 참고).

### 3.2 컬럼 매핑

| 컬럼명 | 매핑 소스 (Langfuse observation) | 매핑 규칙 |
|---|---|---|
| TRACE_ID | `traceId` | 그대로 매핑 |
| NODE_NM | (고정값) | `'AXgenticCode'` 고정 문자열 |
| MODEL_NM | `model` | 그대로 매핑 |
| USER_ID | (고정값) | `'TBD'` 고정 문자열 |
| INPUT_TOKENS | `usage.input` (레거시 필드명일 경우 `usage.promptTokens`) | 정수 변환, 없으면 `0` |
| OUTPUT_TOKENS | `usage.output` (레거시 `usage.completionTokens`) | 정수 변환, 없으면 `0` |
| TOTAL_TOKENS | `usage.total` (레거시 `usage.totalTokens`) | 정수 변환, 없으면 `0`. 값이 없을 경우 `INPUT_TOKENS + OUTPUT_TOKENS`로 보정 |
| LATENCY_MS | `endTime - startTime` (밀리초 환산) | §4.3 참고 — API `latency` 필드를 직접 쓰지 않고 직접 계산 |
| CALL_TM | `startTime` | Langfuse는 UTC로 응답하므로, **UTC → KST(Asia/Seoul, UTC+9)로 변환**하여 저장 (§4.5 참고) |
| REG_DT | (DB 처리) | Python에서 값 전달하지 않고, MERGE 문의 INSERT 절에서만 `SYSTIMESTAMP` 사용 (DB 서버 타임존이 KST라는 전제, §13 오픈 이슈) |
| QUERY_CTN | `input` | §4.2 파싱 규칙 참고 (사용자 질의 텍스트만 추출), `VARCHAR2(4000)` byte 한도 truncate |
| STAT_CD | `level` | `level == 'ERROR'` → `'ERROR'`, 그 외 → `'OK'` |
| ERR_CTN | `statusMessage` | `STAT_CD='ERROR'`일 때만 값 저장, 정상 시 `NULL`. `VARCHAR2(1000)` byte 한도 truncate (§4.4) |
| OBSERVATION_ID | `id` | observation 고유 id, 그대로 매핑. `TRACE_ID`와 함께 PK 구성 |

> ⚠️ **오픈 이슈**: v1 응답의 `usage` 객체는 `{input, output, total, unit}` 형태와 레거시 `{promptTokens, completionTokens, totalTokens}` 형태가 혼재할 수 있다. 구현 착수 시 실제 API 응답을 1건 조회하여 필드명을 확정하고, `transformer.py`에서 두 형태 모두를 안전하게 처리하는 로직(예: 우선순위를 두고 순차 조회)을 둔다.

## 4. 데이터 변환 로직

### 4.1 NODE_NM / USER_ID 고정값
요구사항대로 상수로 정의하여 사용 (`config.py` 또는 `constants.py`에 정의).

### 4.2 QUERY_CTN (사용자 입력값) 파싱

Langfuse `input` 필드는 다음과 같이 다양한 형태로 저장될 수 있다.
- 단순 문자열
- Chat 메시지 배열 (`[{"role": "user", "content": "..."}, ...]`)
- 임의의 JSON 객체

파싱 규칙:
1. `input`이 문자열이면 그대로 사용.
2. `input`이 리스트(messages 형태)이면 `role == "user"`인 마지막 메시지의 `content`를 사용.
3. 위 조건에 해당하지 않으면 JSON 문자열로 직렬화하여 저장(파싱 실패 시 원본을 문자열로 저장하고 로그 남김).
4. `QUERY_CTN`은 `VARCHAR2(4000)`(byte 길이 기준)이므로, UTF-8 인코딩 기준 byte 길이가 4000을 초과하지 않도록 안전하게 truncate한다(멀티바이트 문자 중간 절단 방지를 위해 인코딩 후 자르고 재디코딩, 또는 문자 단위로 줄여가며 byte 길이를 검사).

```python
def truncate_utf8(text: str, max_bytes: int = 4000) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore")
```

### 4.3 LATENCY_MS 계산

Langfuse observation 응답의 `latency` 필드는 통합 버전/시점에 따라 초 단위와 밀리초 단위가 혼재되는 알려진 이슈가 있다(langfuse/langfuse#11051). 따라서 API의 `latency` 값을 그대로 사용하지 않고, **`startTime`, `endTime`으로 직접 계산**한다.

```python
latency_ms = int((end_time - start_time).total_seconds() * 1000)
```

`endTime`이 없는 경우(진행 중 observation)는 **해당 윈도우에서 제외**한다. §5.1에서 체크포인트 기반 증분 처리를 폐기하고 명시적 `--from`/`--to` 입력 방식으로 전환했기 때문에, 별도의 "다음 배치로 이연"할 재처리 큐나 상태 저장소가 존재하지 않는다. 따라서 진행 중인 observation은 이후 동일 시간 범위를 다시 조회(재실행)하지 않는 한 자동으로는 재처리되지 않으며, 이는 §13 오픈 이슈로 별도 관리한다.

### 4.4 STAT_CD / ERR_CTN 판단

- Langfuse `level`: `DEBUG`, `DEFAULT`, `WARNING`, `ERROR` 중 하나.
- `level == 'ERROR'` → `STAT_CD = 'ERROR'`, `ERR_CTN = statusMessage`
- 그 외 → `STAT_CD = 'OK'`, `ERR_CTN = NULL`
- `ERR_CTN`은 `VARCHAR2(1000)`(byte 길이 기준)이므로 §4.2의 `truncate_utf8()`을 `max_bytes=1000`으로 동일하게 적용하여 저장한다.
- 이 섹션의 `STAT_CD='ERROR'`는 **Langfuse가 보고한 원본 LLM 호출 오류**를 의미한다. 본 파이프라인 자체의 파싱/변환 실패로 인한 `STAT_CD='ERROR'`(§7)와는 `ERR_CTN`의 `[ETL_PARSE_ERROR]` 접두사 유무로 구분한다.

### 4.5 타임존 처리 (CALL_TM)

Langfuse API는 시각 정보를 UTC(ISO 8601, 예: `2026-08-30T00:00:00.000Z`)로 반환한다. `CALL_TM`(및 §5.1의 윈도우 계산용 `startTime` 비교)은 **UTC → KST(Asia/Seoul, UTC+9)로 변환한 값**을 저장한다.

```python
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

def to_kst(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    return dt_utc.astimezone(KST)
```

- `LATENCY_MS`(§4.3) 계산은 `endTime - startTime` 차이값이므로 타임존 변환 여부와 무관하게 동일한 결과를 준다(두 값 모두 같은 기준으로 변환하거나, 변환 전 UTC 상태로 계산해도 무방). 단, 코드 일관성을 위해 변환 후 값으로 계산한다.
- `REG_DT`는 Oracle `SYSTIMESTAMP`(DB 서버의 세션 타임존 기준)를 그대로 사용하므로, DB 서버의 타임존이 KST로 설정되어 있는지 사전 확인이 필요하다(§13 오픈 이슈).
- §5.1의 `--from`/`--to` CLI 입력값은 **KST 기준**으로 입력받고, Langfuse API 호출 시에는 UTC로 변환하여 `fromStartTime`/`toStartTime`(또는 `fromTimestamp`/`toTimestamp`) 파라미터에 사용한다.

## 5. 데이터 추출 전략

### 5.1 입력 시간 범위 및 윈도우 분할

배치 실행 시 처리할 전체 시간 범위를 **CLI 인자 `--from`, `--to`로 입력**받는다. **입력값은 한국시간(KST, Asia/Seoul, UTC+9) 기준의 naive datetime**이다 — 예: `--from 2026-08-30T00:00:00`은 `2026-08-30 00:00:00 KST`를 의미하며 UTC가 아니다. 프로그램은 내부적으로 이 범위를 **윈도우 단위**(크기는 `.env`의 `WINDOW_MINUTES`, 기본 10분, §8)로 분할하여 윈도우별로 순차 추출·적재한다.

> ⚠️ **구현 유의사항**: `from`은 Python 예약어이므로 argparse로 `--from` 플래그를 받을 때 속성명을 그대로 `from`으로 쓸 수 없다(`args.from`은 문법 오류). `add_argument("--from", dest="from_time", ...)`, `add_argument("--to", dest="to_time", ...)`처럼 `dest`를 명시적으로 지정한다.

- 파싱 시 입력값에 `ZoneInfo("Asia/Seoul")`을 명시적으로 부여해 KST-aware datetime으로 만든 뒤 윈도우 분할을 수행하고, Langfuse API(`fromStartTime`/`toStartTime`, `fromTimestamp`/`toTimestamp`) 호출 직전에만 UTC로 변환한다(§4.5). DB 적재 값(`CALL_TM`)은 KST로 저장하므로 재변환이 필요 없다.
- 윈도우 크기는 `config.py`가 `.env`의 `WINDOW_MINUTES`를 읽어 결정하며, 코드에 상수로 고정하지 않는다.

```python
def split_into_windows(start_time: datetime, end_time: datetime, minutes: int) -> list[tuple[datetime, datetime]]:
    windows = []
    cur = start_time
    while cur < end_time:
        nxt = min(cur + timedelta(minutes=minutes), end_time)
        windows.append((cur, nxt))
        cur = nxt
    return windows

# 호출부 예: split_into_windows(from_time, to_time, config.WINDOW_MINUTES)
```

- 각 윈도우는 `[window_start, window_end)` 반개구간으로 처리하여 경계 시각 중복을 방지한다.
- 윈도우 단위로 추출 → 변환 → 적재 → 커밋을 수행한다(§6). 특정 윈도우 처리 중 실패해도 이미 커밋된 이전 윈도우는 영향받지 않으며, 실패한 윈도우부터 재실행 가능하다.
- `OBSERVATION_ID` + `TRACE_ID` 복합 PK와 §6의 MERGE 적재 방식 덕분에, 동일 윈도우를 재실행하더라도 중복 적재 없이 멱등적으로 처리된다(이전 설계의 "체크포인트 기반 증분 처리"는 본 방식으로 대체됨).
- CLI 예시: `python -m src.main --from 2026-08-30T00:00:00 --to 2026-08-30T06:00:00` (두 값 모두 KST 기준)

### 5.2 tags 필터링 ('project:1')

observation의 tags는 Langfuse 데이터 모델상 **observation에 설정된 tags가 상위 trace로 집계**되는 구조이며, v1 Observations API 응답(`GET /api/public/observations`)에는 tags 필드가 직접 포함되지 않을 수 있다. 따라서 다음 2단계 방식으로 필터링한다.

1. **Trace 조회**: `GET /api/public/traces`를 `tags=["project:1"]`, 윈도우의 시간 범위(trace 타임스탬프 기준 파라미터, 예: `fromTimestamp`/`toTimestamp`)로 호출하여, 해당 윈도우 내 `'project:1'` 태그가 포함된 trace ID 목록을 페이지네이션으로 전체 수집한다.
2. **Observation 조회**: 수집된 각 `traceId`에 대해 `GET /api/public/observations`를 `traceId=<id>`, `type=GENERATION`, **그리고 1단계와 동일한 윈도우의 `fromStartTime`/`toStartTime`**을 함께 지정하여 조회한다.

> ⚠️ **시간 필터를 observation 조회에도 반드시 적용하는 이유**: trace 하나가 여러 10분 윈도우에 걸쳐 지속되며 여러 generation을 가질 수 있다. 1단계에서 trace는 자신의 타임스탬프가 속한 단일 윈도우에서만 매칭되므로, 2단계에서 `traceId`만으로 조회하면 **그 trace에 속한, 현재 윈도우 밖의 observation까지 함께 적재**되어 §5.1이 정한 윈도우별 반개구간 처리 원칙이 깨진다. 따라서 2단계 조회에도 동일한 `fromStartTime`/`toStartTime`을 반드시 지정해 해당 윈도우에서 시작된 observation만 가져온다.

> ⚠️ **오픈 이슈**: v1 `traces` API의 `tags` 파라미터명/문법과, v1 `observations` API가 `filter`(JSON 기반 advanced filtering) 파라미터로 tags를 직접 지원하는지 여부는 실제 API 레퍼런스(`api.reference.langfuse.com`)로 최종 확인이 필요하다. 만약 v1 observations가 tags 직접 필터를 지원한다면 1단계(trace 조회)를 생략하고 단일 호출로 단순화할 수 있다(§13 오픈 이슈 3).

### 5.3 페이지네이션
- `get_many(..., page=N, limit=100)` 형태로 순차 조회, 응답의 `meta.totalPages`(또는 동등 필드)를 기준으로 종료. Trace 조회, Observation 조회 각각에 대해 동일하게 적용.

## 6. DB 적재 전략

- 드라이버: `python-oracledb` (thin mode) 사용.
- 배치 처리: `cursor.executemany()`로 다건 일괄 MERGE(upsert).
- `TRACE_ID` + `OBSERVATION_ID` 복합 PK가 존재하므로, 동일 윈도우 재처리 시 PK 충돌(`ORA-00001`)을 피하기 위해 단순 `INSERT` 대신 **`MERGE`**를 사용하여 이미 적재된 행은 갱신, 없으면 신규 삽입한다(§5.1 멱등성 보장).
- `REG_DT`는 바인딩 값으로 넘기지 않고 SQL 문 자체에서 `SYSTIMESTAMP`를 사용(신규 INSERT 시에만 설정, 기존 행 UPDATE 시 `REG_DT`는 갱신하지 않음).
- 커밋 단위: 윈도우(`WINDOW_MINUTES`) 단위 커밋. 커밋 실패 시 해당 윈도우 전체 롤백 후 재시도.

```sql
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
```

## 7. 오류 처리 전략

| 오류 유형 | 처리 방식 |
|---|---|
| Langfuse API 인증/네트워크 오류 | 배치 전체 실패 처리, 재시도(exponential backoff, 예: 3회), 최종 실패 시 로그/알림 |
| Langfuse API 응답 파싱 오류(레코드 단위) | 해당 레코드는 스킵하지 않고 `STAT_CD='ERROR'`, `ERR_CTN`에 `[ETL_PARSE_ERROR] <오류 메시지>` 형식으로 담아 적재(가시성 확보), 원본 페이로드는 로그에 별도 기록. **접두사로 Langfuse 자체 오류(§4.4, `level='ERROR'`의 `statusMessage`)와 구분** — 접두사가 없으면 Langfuse가 보고한 원본 오류, 있으면 본 파이프라인의 변환 실패를 의미 |
| Oracle DB 연결/적재 오류 | 윈도우 단위 롤백 후 재시도, 최종 실패 시 해당 윈도우를 실패로 로깅하고 종료(동일 `--from`/`--to`로 재실행 시 MERGE에 의해 안전하게 재처리) |

## 8. 설정 관리

Langfuse 접속 정보와 Oracle 접속 정보는 **`.env` 파일로 관리**한다(`python-dotenv`로 로드, `.env`는 `.gitignore`에 추가하여 커밋 제외, `.env.example`만 저장소에 포함).

`.env` 예시:
```
LANGFUSE_HOST=https://<host>
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...

ORACLE_DSN=<host>:<port>/<service_name>
ORACLE_USER=...
ORACLE_PASSWORD=...

WINDOW_MINUTES=10
```

- **윈도우 크기(`WINDOW_MINUTES`)는 `.env`로 관리**한다(기본값 10, §5.1). 코드에 하드코딩하지 않고 `config.py`에서 로드.
- 처리할 시간 범위는 실행마다 달라지므로 `.env`가 아닌 **CLI 인자(`--from`/`--to`)**로 받는다(§5.1, §9).

## 9. 프로젝트 구조

```
ExtractObservation2Oracle/
├── readme.md
├── design.md
├── requirements.txt
├── .env.example
├── sql/
│   └── table_reference.sql  # 기 생성된 TRX_TOKEN_DET 구조 기록용(참고 전용, 실행 안 함)
├── src/
│   ├── config.py           # .env 로드(dotenv): 접속정보 + WINDOW_MINUTES
│   ├── langfuse_client.py  # Langfuse traces/observations 조회, 페이지네이션, tags 필터 (§5.2)
│   ├── windowing.py        # 시간 범위를 WINDOW_MINUTES 단위로 분할 (§5.1)
│   ├── transformer.py      # observation → DB row 매핑/파싱 (§4)
│   ├── db.py                # Oracle 연결 및 MERGE 적재 (§6)
│   └── main.py              # CLI 진입점(--from, --to), 윈도우 반복 처리
├── tests/
│   ├── test_transformer.py
│   └── test_windowing.py
└── logs/
```

## 10. 실행/스케줄링

- 실행 예: `python -m src.main --from 2026-08-30T00:00:00 --to 2026-08-30T06:00:00` (KST 기준)
- 입력받은 범위는 내부에서 `.env`의 `WINDOW_MINUTES`(기본 10분) 단위로 분할되어 순차 처리된다(§5.1, §8).
- 주기 실행이 필요하면 외부 스케줄러(Windows 작업 스케줄러 / cron)에서 직전 실행 종료 시각~현재 시각을 `--from`/`--to`로 계산하여 호출. 프로그램 자체는 상시 실행 데몬이 아닌 1회성 배치로 설계.

## 11. 로깅

- 표준 `logging` 모듈 사용, 파일 핸들러로 `logs/` 하위에 일자별 로그 저장.
- 배치 시작/종료, 윈도우별 조회 건수, 적재 성공/실패 건수를 INFO 레벨로 기록.
- 레코드 단위 파싱/변환 오류는 WARNING, API/DB 오류는 ERROR로 기록.

## 12. 테스트 전략

- `transformer.py`의 매핑/파싱 함수(§4.2~4.4)는 다양한 `input` 형태, `level` 값, 토큰 usage 누락 케이스, `QUERY_CTN` byte 길이 초과(한글 포함) 케이스에 대한 단위 테스트 작성.
- `windowing.py`의 10분 분할 함수는 경계값(정확히 10분 배수, 나머지가 있는 경우, 범위가 10분 미만인 경우) 테스트.
- Oracle 연동은 로컬/테스트 DB 또는 mock cursor로 MERGE `executemany` 호출 및 재실행 시 중복 미발생(멱등성) 검증.

## 13. 오픈 이슈 (구현 착수 전 확인 필요)

> `TRX_TOKEN_DET`은 이미 생성되어 있으므로 테이블 구조(컬럼/타입/길이/PK) 자체는 확정 사항이며 변경 대상 오픈 이슈에서 제외한다. 아래는 Langfuse API 연동 및 운영 환경 확인이 필요한 항목만 남긴다.

1. `usage` 관련 실제 응답 필드명 확정 (§3.2 비고).
2. (자체 호스팅으로 v1 유지 확정, 해소됨) 다만 향후 Langfuse 버전 업그레이드 시에는 릴리스 노트에서 v1 엔드포인트 제거 여부를 확인한다.
3. `'project:1'` tags 필터링을 위한 정확한 API 파라미터(§5.2) — v1 `traces` API의 `tags` 파라미터 문법, v1 `observations` API의 `filter`(advanced JSON filtering) 지원 여부 및 tags 직접 필터 가능 여부를 실제 API 레퍼런스로 확인 후, 가능하다면 2단계(trace→observation) 조회를 단일 호출로 단순화.
4. Oracle DB 서버의 세션 타임존이 KST(Asia/Seoul)로 설정되어 있는지 운영 환경에서 확인 필요(§4.5) — `REG_DT`는 `SYSTIMESTAMP`를 그대로 사용하므로, DB 서버 타임존이 KST가 아닐 경우 `SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul'` 등 SQL 표현식으로 보정하는 방안을 코드에서 대비한다(테이블 구조 변경 없이 INSERT/MERGE 쪽 SQL만 조정).
