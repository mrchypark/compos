# 05. 저장소와 배포의 현재 계약

- 상태: EGM 기반 역명세 초안. 새로운 설계나 구현 변경을 제안하지 않는다.
- 기준일: 2026-10-31. 커밋된 과거 문서가 아니라 작성 시점의 작업 트리 소스를 기준으로 한다.
- 범위: SQLite·내장 Rhiza 저장 경계, 22개 테이블, 스키마 초기화·구형 DB 거부, 백업·논리 이동, 객체 저장소 설정, Rust Operator·CRD·배포·CI.
- 근거 수준: **소스**는 구현 또는 선언을 읽은 결과이고, **테스트 근거**는 테스트가 검사하도록 작성된 내용을 뜻한다. 이 작업에서는 테스트·빌드·배포를 실행하지 않았다. 과거 배포 보고서의 성공 여부를 현재 코드의 검증 결과로 옮기지 않는다.

## 1. 목적과 경계

이 문서는 현재 Compos의 상태가 어디에 저장되며, 어떤 경계에서 원자적으로 바뀌고, 어떤 조건에서 재시작·이동·배포가 거부되는지 설명한다. EGM 템플릿의 목적·경계·사용 흐름·기술 계약·검증 항목을 현재 구현 설명에 맞춰 구성했다. 대안 선택이나 구현 계획은 추가하지 않는다.

Go 서버는 `-rhiza-data`가 비어 있으면 로컬 SQLite를 열고, 값이 있으면 프로세스 안에 Rhiza 노드를 연다. `OpenRhiza`는 별도의 공개 DB 리스너를 열지 않는다. 애플리케이션 HTTP 서버와 Rhiza peer 주소는 서로 다른 설정이다. 주 모듈의 Rhiza 의존성은 `v0.19.0`이며, 레거시 export 도구만 별도 모듈에서 `v0.12.0`을 사용한다. [서버 선택](../../cmd/server/main.go#L25), [내장 노드](../../internal/store/rhiza.go#L17), [주 모듈](../../go.mod#L9), [export 모듈](../../tools/rhiza-v012-export/go.mod#L5)

Store는 SQL 실행·일관성·상태 조회를 제공한다. 사용자 인증과 도메인 명령 권한은 Kernel/Server에서 검사한다. 파일·객체 저장소 접근 권한을 가지고 직접 Store나 import 도구를 호출하는 행위를 HTTP 사용자 권한 검사와 동일시할 수 없다. [Store 쓰기 경계](../../internal/store/tx.go#L18), [Kernel 인증·권한](../../internal/kernel/executor.go#L59), [import 진입점](../../cmd/storage-import/main.go#L19)

## 2. 엔터티와 상태

### 2.1 Store의 상태와 수명

`Store`는 `kind`, 경로, cluster ID와 SQLite `*sql.DB` 또는 Rhiza 핸들을 보유한다. SQLite의 `Kind()`는 `sqlite`, 기본 cluster ID는 `sqlite-local`이다. Rhiza의 `Kind()`는 `rhiza`, 경로는 `Config.DataDir`, cluster ID는 설정값이다. `DB()`를 제공하지만 Rhiza 생성 경로는 `db` 필드를 채우지 않는다. 따라서 Rhiza에 대해 `DB()`가 SQLite 핸들을 반환한다고 기대하면 안 된다. [구조·생성](../../internal/store/store.go#L34), [Rhiza 생성](../../internal/store/rhiza.go#L23), [조회 API](../../internal/store/store.go#L209)

SQLite `Ready()`는 항상 참이고, Rhiza는 하위 노드의 `Ready()`를 전달한다. `Health(ctx)`는 readiness와 로컬 `PRAGMA foreign_keys=1`을 검사한다. SQLite에서는 추가로 ping을 수행한다. Rhiza health가 성공했다는 사실만으로 새 선형화 읽기나 새 쓰기가 성공했다고 판단하지 않는다. `Optimize`는 SQLite에서만 `PRAGMA optimize`를 실행한다. 서버는 24시간마다 이를 호출하며, SQLite 종료 시에는 최대 10초의 context로 optimize를 시도하고 close 오류와 합친다. Rhiza 종료는 `cluster.Close`에 위임한다. [수명·health](../../internal/store/store.go#L211), [주기 실행](../../cmd/server/main.go#L70)

### 2.2 현재 22개 테이블의 분류별 전체 목록

아래 목록은 `schema.sql`에 선언된 **22개의 애플리케이션 테이블**이다. Rhiza 내부 `_rhiza_*` 및 SQLite 내부 테이블은 포함하지 않는다. 각 행의 링크는 해당 DDL의 시작점이며, 전체 열·FK·CHECK·인덱스의 정확한 정의는 연결된 소스에 있다. 새 DB의 22개 테이블과 논리 import 대상 21개의 차이는 `schema_metadata`이다. [import 순서](../../internal/store/logical_import.go#L69)

| 분류 | 테이블 | 식별자·주요 제약과 저장 상태 |
| --- | --- | --- |
| 스키마 1 | [`schema_metadata`](../../internal/store/schema.sql#L3) | PK `id=1`; `identity`, `applied_at`을 저장한다. |
| 공간 2 | [`workspaces`](../../internal/store/schema.sql#L9) | PK `id`, 전역 UNIQUE `key`; `version>=1`과 생성·수정 시각을 저장한다. |
| 객체·관계 3 | [`object_types`](../../internal/store/schema.sql#L18) | PK `id`, UNIQUE `(workspace_id,key)` 및 `(workspace_id,id)`; 필드·관계 JSON 객체, schema version, archive, 수정 version을 저장한다. |
| 객체·관계 4 | [`object_type_schema_revisions`](../../internal/store/schema.sql#L38) | PK `(workspace_id,object_type_id,schema_version)`; schema JSON·digest를 저장하고 UPDATE/DELETE를 금지한다. |
| 객체·관계 5 | [`object_instances`](../../internal/store/schema.sql#L59) | PK `id`; type 및 schema revision에 복합 FK가 있다. payload는 JSON 객체이며 state·archive·version을 저장한다. |
| 객체·관계 6 | [`relation_definitions`](../../internal/store/schema.sql#L99) | PK `id`, UNIQUE `(workspace_id,key)`; 양 끝 type에 복합 FK가 있다. cardinality는 `one_to_one/one_to_many/many_to_one/many_to_many`, required는 0/1이다. |
| 객체·관계 7 | [`object_relations`](../../internal/store/schema.sql#L119) | PK `id`; 공간·정의·from·to 조합이 UNIQUE이고 정의와 양 끝 객체에 복합 FK가 있다. |
| 객체·관계 8 | [`relation_cardinality_slots`](../../internal/store/schema.sql#L141) | PK `id`; `(workspace_id,relation_definition_id,side,object_instance_id)`가 UNIQUE이다. side는 `from/to`이며 INSERT 트리거가 cardinality와의 조합을 검사한다. |
| 객체·관계 9 | [`object_field_index`](../../internal/store/schema.sql#L169) | PK `(workspace_id,object_instance_id,field_name)`; kind는 `string/number/boolean/null`; 문자열·수치·불리언 부분 인덱스를 둔다. |
| 생성 필드 10 | [`field_sequence_counters`](../../internal/store/schema.sql#L186) | PK `(workspace_id,object_type_id,field_name,scope_key)`; counter는 0 이상의 정수이다. |
| 생성 필드 11 | [`generated_field_values`](../../internal/store/schema.sql#L196) | PK `(workspace_id,object_type_id,field_name,generated_value)`; 객체별 필드도 UNIQUE이다. |
| 반응형 실행 12 | [`reactive_rules`](../../internal/store/schema.sql#L209) | PK `id`, 공간별 `rule_key` UNIQUE; status는 `draft/active/disabled/retired`, circuit breaker는 `closed/open`이다. 현재 revision·cursor·version을 저장한다. |
| 반응형 실행 13 | [`reactive_rule_revisions`](../../internal/store/schema.sql#L232) | PK `id`; rule·definition hash 조합 UNIQUE와 supersedes FK가 있다. 정의 JSON들과 `owner_principal` 정책을 저장한다. publication 시각의 최초 설정을 제외한 UPDATE와 모든 DELETE를 금지한다. |
| 반응형 실행 14 | [`adapter_inbound_events`](../../internal/store/schema.sql#L283) | PK `id`, UNIQUE `(workspace_id,adapter_id,inbound_event_id)`; payload·hash와 `appended/reduced/rejected` 상태를 저장한다. |
| 반응형 실행 15 | [`reactive_rule_evaluations`](../../internal/store/schema.sql#L297) | PK `id`; predicate는 `matched/skipped/failed`, dispatch는 `pending/dispatched/applied/rejected/retryable_failed/dead_lettered`이다. revision/action이 존재할 때 이벤트별 action 중복을 부분 UNIQUE로 막는다. |
| 명세·기록 16 | [`spec_revisions`](../../internal/store/schema.sql#L328) | PK `id`; 공간별 `(family_id,revision)` 및 `(kind,digest)`가 UNIQUE이다. kind는 `task/function/workflow/participant/event/ontology`이고 UPDATE/DELETE를 금지한다. |
| 명세·기록 17 | [`command_ledger`](../../internal/store/schema.sql#L351) | PK `command_id`, UNIQUE `(workspace_id,authenticated_principal_id,idempotency_key)`; actor·target·precondition·causation·payload·outcome·receipt를 저장한다. |
| 명세·기록 18 | [`journal_records`](../../internal/store/schema.sql#L374) | PK `id`, UNIQUE `(workspace_id,stream_id,stream_sequence)`; command/spec FK와 양의 sequence가 있다. participant kind/id는 함께 NULL이거나 함께 존재해야 하며 UPDATE/DELETE를 금지한다. |
| 명세·기록 19 | [`activities`](../../internal/store/schema.sql#L413) | PK `id`; command에 복합 FK가 있고 aggregate·event·JSON payload를 저장한다. |
| 전달·투영 20 | [`outbox_events`](../../internal/store/schema.sql#L427) | PK `id`; command FK와 JSON 객체 payload를 저장한다. status는 `pending/published/failed`이다. |
| 전달·투영 21 | [`projection_events`](../../internal/store/schema.sql#L441) | PK `id`, UNIQUE `(workspace_id,projection_sequence)`; aggregate version·payload hash·JSON과 nullable snapshot/signature/key ID를 저장한다. |
| 공간 생성 22 | [`workspace_creation_requests`](../../internal/store/schema.sql#L460) | PK `(authenticated_principal_id,idempotency_key)`; workspace도 UNIQUE이다. 요청 hash·payload·result를 저장한다. |

### 2.3 DDL이 보장하는 범위와 남아 있는 과거 값

모든 22개 테이블은 STRICT이다. JSON CHECK는 열에 명시된 유효성·종류를 검사하며 시각 GLOB은 완전한 달력 검증이 아니다. 자체 auth_sessions와 identity 테이블은 없다. [DDL](../../internal/store/schema.sql#L1)

객체 INSERT 및 workspace/type/schema pin 변경 UPDATE는 현재 type schema version과 pin이 같아야 한다. 과거 pin은 revision FK로 유지한다. cardinality slot은 정의의 방향·카디널리티를 검사한다. principal–Person binding/claim 테이블과 관련 트리거는 없다. [DDL](../../internal/store/schema.sql#L59)

Knowledge 전용 테이블은 현재 DDL에 없다. 그러나 `journal_records.kind`에는 `knowledge.proposed`, `knowledge.proposal.accepted`, `knowledge.proposal.rejected`가 남아 있고 `spec_revisions.kind=ontology`도 유지된다. 이는 과거 기록을 담는 저장 계약이며 제거된 명령·API가 현재 제공된다는 근거가 아니다. ledger outcome의 허용값도 저장 가능한 값의 집합일 뿐, 모든 실패가 반드시 ledger에 기록된다는 뜻은 아니다. [허용 kind](../../internal/store/schema.sql#L328), [ledger·journal](../../internal/store/schema.sql#L351)

## 3. 저장 API와 원자성

### 3.1 SQLite 열기와 쓰기

`Open(ctx, Options)`는 빈 Path를 `sqlite path is required`로 거부한다. `MaxOpenConns<=0`은 8, `BusyTimeout<=0`은 10초, 빈 synchronous mode는 `NORMAL`로 대체한다. synchronous는 정확히 `NORMAL` 또는 `FULL`만 허용한다. 일반 경로는 절대 경로로 바꾸고 `file:` URI는 파싱한다. DSN은 `_txlock=immediate`, `_timefmt=rfc3339`, busy timeout, FK 활성화, WAL, synchronous PRAGMA를 추가한다. FTS5를 등록하고 ping·초기화·planner optimize를 순서대로 수행하며 실패하면 DB를 닫는다. [Open](../../internal/store/store.go#L44), [DSN](../../internal/store/store.go#L88)

`Transact(ctx, requestID, fn)`는 SQLite에서 serializable SQL 트랜잭션을 시작한다. callback 오류는 commit하지 않고 rollback하며 성공하면 commit한다. 이 경로는 `requestID`를 저장하거나 자체 중복 제거에 쓰지 않는다. `ExecAffected(ctx, requestID, query, args...)`는 단일 문장을 이 트랜잭션으로 실행하고 RowsAffected를 반환한다. `Exec`은 영향 행 수를 버리고 오류만 반환한다. [쓰기 API](../../internal/store/tx.go#L28)

### 3.2 Rhiza 쓰기와 멱등성

`Transact`는 Rhiza에서 callback을 실행하지 않고 `ErrInteractiveTransactionUnsupported`를 반환한다. `ApplyStatements(ctx, requestID, []rhiza.SQLStatement)`는 Rhiza 전용이며, SQLite에서는 `replicated SQL statements require Rhiza storage`로 실패한다. Rhiza `Execute` 응답의 status가 `committed`일 때만 성공한다. precondition/execution 코드에는 각각 `ErrMutationPreconditionFailed`, `ErrMutationExecutionFailed`를 감싸며, 나머지 거부 응답은 status와 code를 포함한 오류가 된다. `ExecAffected`의 단일 문장 경로는 committed 여부와 RowsAffected를 반환하지만 위 두 sentinel로 분류하지 않는다. [API](../../internal/store/tx.go#L28)

request ID가 비어 있으면 UUID를 만들고, 길이가 64바이트 이하이면 그대로 쓰며, 초과하면 SHA-256 hex로 바꾼다. 이것은 Rhiza 요청 식별자의 정규화이다. 도메인 명령의 중복 처리는 ledger의 공간·인증 principal·멱등 키와 명령 내용 비교가 맡는다. Kernel은 재시도마다 새 Rhiza request ID를 만들고 ledger guard를 같은 배치에 넣는다. 따라서 transport request ID와 사용자 명령 멱등 키는 서로 다른 경계이다. [ID 정규화](../../internal/store/rhiza.go#L104), [ledger guard](../../internal/kernel/rhiza.go#L250), [commit·중복 조회](../../internal/kernel/rhiza.go#L499)

### 3.3 도메인 변경과 감사 기록의 원자성

SQLite Kernel은 트랜잭션 전에 외부 승인을 확인한다. 트랜잭션 경로는 중복 확인 → 공간 존재·외부 actor 승인·precondition 확인 → 도메인 변경 → projection sequence 할당 → receipt 및 기록 저장을 하나의 트랜잭션으로 묶는다. Rhiza 경로는 상태 guard, 변경, ledger, journal, secondary effects, activity, pending outbox, projection을 하나의 statement batch로 제출한다. Rhiza commit 경로는 64개 문장을 초과하면 `validation_failed`로 거부하며 원자적 명령을 여러 배치로 나누지 않는다. [SQLite 실행](../../internal/kernel/executor.go#L59), [Rhiza commit](../../internal/kernel/rhiza.go#L499)

Rhiza 배치 실패 후에는 ledger를 선형화 읽기로 다시 조회해 이미 적용된 동일 명령이면 `duplicate` receipt로 수렴한다. 같은 멱등 키로 command ID·type·payload hash·target·preconditions·causation·requested actor 중 하나라도 다르면 `idempotency_conflict`이다. 중복이 없을 때 mutation precondition 실패는 `precondition_failed`, SQL 제약 실패는 `validation_failed`로 바뀐다. 나머지 오류는 반환한다. commit 이후 receipt 읽기가 실패할 수 있으므로 호출 오류만으로 미적용을 단정할 수 없다. [commit 후 처리](../../internal/kernel/rhiza.go#L536), [정확한 비교 필드](../../internal/kernel/rhiza.go#L554)

원자성은 SQL 상태와 전달할 이벤트의 **저장**까지이다. 외부 서비스가 outbox를 소비하고 실행을 끝내는 행위까지 같은 트랜잭션에 포함되지는 않는다. Store 자체는 principal을 받지 않으며, Kernel의 주입된 외부 AuthorizeCommand가 사용자 명령 권한의 경계다. authority 부재는 거부이며 network adapter는 아직 없다. [권한](../../internal/kernel/authority.go#L24), [저장되는 outbox](../../internal/kernel/rhiza.go#L527)

## 4. 읽기 일관성과 관측 계약

| API | SQLite | Rhiza |
| --- | --- | --- |
| `ReadRow(ctx, consistency, query, args...) Row` | `QueryRowContext`; Version은 0 값이다. | `Query`에 consistency를 전달한다. 첫 행을 반환하고 없으면 Scan에서 `sql.ErrNoRows`를 반환한다. |
| `ReadRows(ctx, consistency, query, args...) (Rows,error)` | `*sql.Rows`를 감싼다. | 응답 rows를 메모리 iterator로 감싸며 AppliedSlot·ConsensusTip을 보존한다. |
| `ReadSnapshot(ctx, consistency, fn) (ReadVersion,error)` | 하나의 read-only SQL 트랜잭션 안에서 callback을 실행한다. | callback의 각 Query 응답 AppliedSlot이 같아야 한다. slot이 바뀌면 callback 전체를 최대 8회 시도한다. |

허용 consistency는 `local`과 `linearizable`뿐이며 다른 문자열은 `unsupported read consistency` 오류이다. SQLite에서는 허용값을 검사하되 두 값 모두 로컬 SQL 경로를 사용한다. Rhiza에서는 두 값을 하위 Query에 전달한다. Snapshot은 같은 AppliedSlot을 요구하고 ConsensusTip은 관측한 최댓값으로 갱신한다. slot 변화 이외의 callback 오류는 즉시 반환하고, 8회 모두 바뀌면 `read snapshot changed during 8 attempts`로 실패한다. callback 결과를 누적하는 호출자는 재실행을 고려해야 한다. 이것은 Rhiza에 장시간 고정한 서버 측 read transaction을 여는 API가 아니다. [읽기 전체 계약](../../internal/store/read.go#L14), [snapshot 재시도](../../internal/store/read.go#L48), [slot 비교](../../internal/store/read.go#L148)

`GET /api/v1/health`는 Store health 오류에 HTTP 503과 `storage_unavailable`, domain `storage`, retryable=true를 반환한다. 성공하면 HTTP 200으로 `status=ok`와 `storage.kind`, `cluster_id`, `ready`를 반환한다. Operator가 사용하는 readiness probe도 이 경로이다. [HTTP 응답](../../internal/server/server.go#L113), [probe 생성](../../crates/compos-operator/src/main.rs#L352)

## 5. Greenfield 스키마 초기화와 구형 DB 거부

현재 identity는 `compos-go-sqlite-external-authority-v5`다. SQLite와 Rhiza는 identity가 다르면 거부한다. v3/v4를 자동으로 변경하거나 인증 테이블을 DROP하는 호환 마이그레이션은 없다. 기존 DB나 라이브 데이터를 이번 문서 작업에서 변경하지 않았다. [상수·SQLite 검사](../../internal/store/store.go#L19), [Rhiza 검사](../../internal/store/rhiza.go#L47)

빈 SQLite는 DDL을 트랜잭션으로 적용하고 schema_metadata에 identity를 기록한다. 사용자 테이블이 있지만 metadata 또는 identity 행이 없으면 legacy/partial DB로 거부한다. 현재 identity는 호환성 표식이며 전체 DDL의 해시 검증은 아니다. [초기화](../../internal/store/store.go#L113)

빈 Rhiza는 DDL을 최대 64 statement의 Migration들로 나누고 마지막 배치에서 metadata와 identity를 공개한다. 이 초기 생성용 Migration을 구형 인증 상태의 호환 이관으로 해석하지 않는다. 다중 멤버 초기화는 지정된 readiness/quorum/durability/commit-unknown 오류를 재시도하지만 identity 불일치는 지원하지 않는다. [초기화](../../internal/store/rhiza.go#L17)

여섯 자체 인증 테이블과 Knowledge 전용 테이블은 새 DDL에 없다. 공유 Journal의 과거 Knowledge kind와 일반 Ontology spec은 유지한다. 이전 zoom_out 보고서는 작성 시점의 이력이며 현재 스키마나 migration 완료 근거로 사용하지 않는다.

## 6. 백업·논리 export/import

### 6.1 로컬 SQLite 온라인 백업

`Backup(ctx,destination)`은 빈 destination을 거부하고 SQLite driver connection의 `Backup("main",destination)`으로 위임한다. 임의 SQL dump를 만드는 API가 아니다. Rhiza에서는 `embedded Rhiza owns HA checkpoints; direct SQLite backup is disabled`로 거부한다. 서버의 `-backup <destination>`은 DB를 연 뒤 백업하고 HTTP 서버 설정 전에 종료한다. 따라서 백업 전에 초기화나 구형 identity 거부 전환이 먼저 일어날 수 있다. HTTP 인증을 거치는 백업 endpoint는 이 경로에 없다. 백업 파일 overwrite·권한·원격 checkpoint 보관 정책은 이 래퍼가 추가로 정의하지 않는다. [백업 API](../../internal/store/store.go#L258), [서버 분기](../../cmd/server/main.go#L87)

### 6.2 레거시 v0.12 export 도구

`tools/rhiza-v012-export`는 v0.12 object-store read replica로 인증된 archive를 읽는다. source voter에 참가하거나 voter 자격 증명을 받지 않는다. 결과는 `compos-logical-v1` JSON이며 source 정보, 테이블별 columns·rows, 제외 테이블의 이름·행 수를 포함한다. [replica 구성](../../tools/rhiza-v012-export/main.go#L110), [출력 작성](../../tools/rhiza-v012-export/main.go#L158)

| 플래그 | 환경변수·기본값 또는 제한 |
| --- | --- |
| `-data-dir`, `-output` | `COMPOS_RHIZA_EXPORT_DATA_DIR`, `COMPOS_RHIZA_EXPORT_OUTPUT`; 전용 로컬 replica 경로와 새 결과 파일 경로이다. |
| `-object-provider` | `COMPOS_RHIZA_OBJECT_PROVIDER`, 기본 `gcs`; 허용값은 `gcs/filesystem`뿐이다. |
| `-object-bucket`, `-object-prefix`, `-object-dir` | 각각 `COMPOS_RHIZA_OBJECT_BUCKET/PREFIX/DIR`; gcs는 bucket, filesystem은 object-dir가 필요하다. |
| `-object-endpoint`, `-object-region`, `-object-insecure` | 대응하는 `COMPOS_RHIZA_OBJECT_*`에서 읽는다. |
| `-cluster-id`, `-node-id` | `COMPOS_RHIZA_CLUSTER_ID/NODE_ID`, 기본 `compos/compos-0`이다. |
| `-page-size` | `COMPOS_RHIZA_EXPORT_PAGE_SIZE`, 기본 500; 허용 범위 1~5000이다. |

[플래그](../../tools/rhiza-v012-export/main.go#L92), [설정 검증](../../tools/rhiza-v012-export/main.go#L535)

처음 Sync 후 replica가 ready이고 `LagSlots=0`, `AppliedSlot=SourceTip`이어야 한다. 각 query의 AppliedSlot을 고정 기준과 비교하고 마지막 query 및 최종 Sync 뒤에도 동일 slot/tip인지 확인한다. 원본이 진행하면 최대 3회 다시 export하며 다른 오류는 즉시 실패한다. `schema_metadata`, 폐기된 `object_type_templates`, Knowledge 3개 테이블, 제거된 자체 인증 6개 테이블, `_rhiza_*`, `sqlite_*`를 export 대상에서 제외한다. 이 도구는 21개 업무 테이블을 다루며 자체 인증 상태를 복사하지 않는다. 복사 대상은 애플리케이션 상태이며 합의·voter 상태가 아니다. [일관성](../../tools/rhiza-v012-export/main.go#L135), [최종 확인](../../tools/rhiza-v012-export/main.go#L252), [제외 규칙](../../tools/rhiza-v012-export/main.go#L476)

출력 디렉터리는 group/world 접근 권한이 없어야 하며 기존 output은 거부한다. 임시 파일에 쓰고 sync·close한 뒤 파일 mode 0600을 설정하고 hard link로 output을 배타 생성한다. 기존 결과를 덮어쓰는 rename이 아니다. link 성공 뒤 임시 파일 제거에 실패하면 output 제거를 시도한다. stderr 요약에는 SHA-256·테이블 수·행 수·제외 테이블 수가 들어간다. [출력 게시](../../tools/rhiza-v012-export/main.go#L495), [요약](../../tools/rhiza-v012-export/main.go#L78)

### 6.3 import 입력과 CLI 계약

`compos-storage-import`의 플래그는 `-input`, `-verify-only`, `-rhiza-data`, `-cluster-id`, `-node-id`이다. input/data/cluster/node는 각각 `COMPOS_LOGICAL_IMPORT_PATH`, `COMPOS_RHIZA_DATA_DIR`, `COMPOS_RHIZA_CLUSTER_ID`, `COMPOS_RHIZA_NODE_ID`를 기본값으로 쓴다. 필수값 누락이나 identity의 `[A-Za-z0-9_-]` 이외 문자는 거부한다. 입력은 1~268435456바이트여야 하며, JSON 뒤에 다른 값이 오면 거부한다. JSON 숫자는 `UseNumber`로 읽어 정수는 int64, 소수·지수 표현은 float64로 변환하며 배열·객체를 scalar cell로 허용하지 않는다. [CLI](../../cmd/storage-import/main.go#L19), [decode·scalar](../../internal/store/logical_import.go#L80)

입력 format은 `compos-logical-v1`, source Rhiza version은 정확히 `v0.12.0`, source schema identity는 정확히 `compos-go-sqlite-world-spec-journal-reactive-agent-v2`여야 한다. 현재 v5를 임의로 export해 다시 import하는 범용 round-trip 계약이 아니다. 21개 대상 테이블이 모두 정확히 한 번 있어야 하며 빈/중복 열, 행 너비 불일치, 지원하지 않는 테이블을 거부한다. `excluded_tables`에는 source `schema_metadata` 1행과 `object_type_templates` 선언이 필수이다. Knowledge·자체 인증 테이블 및 내부 테이블의 제외 선언 허용 범위는 validator에 따른다. 자체 인증 상태를 target으로 이관하지 않는다. [입력 검증](../../internal/store/logical_import.go#L102)

CLI는 provider·bucket·비어 있지 않은 prefix를 요구하고 source/target prefix의 `.`·`..` segment를 거부한다. target cluster ID와 node ID는 각각 source와 달라야 한다. 일반 import는 target 로컬 data directory를 mode 0700으로 생성하고 비어 있는지 확인한다. `-verify-only`는 이 디렉터리 공백 검사를 건너뛰되 Rhiza 저장소를 실제로 열기 때문에 스키마 열기 부작용까지 없는 순수 파일 비교 명령은 아니다. target object prefix가 source prefix와 반드시 다른지 또는 원격 prefix가 비어 있는지는 이 CLI가 검사하지 않는다. [대상 검증](../../cmd/storage-import/main.go#L44), [디렉터리 검사](../../cmd/storage-import/main.go#L101)

### 6.4 import의 원자성·재시도·검증

`ImportLogical`은 Rhiza target만 허용한다. SHA-256 문자열의 길이·hex를 검사하고 target catalog와 열 이름·순서, PK 중복을 비교한다. 쓰기 전에 21개 target 테이블이 모두 비어 있는지 검사한다. `schema_metadata`는 target 초기화가 만든 v5 값을 유지한다. source 파일 hash는 CLI decode가 계산하며, 라이브러리 API는 전달받은 hash의 형식만 확인한다. [사전 검증](../../internal/store/logical_import.go#L215), [catalog·공백 검사](../../internal/store/logical_import.go#L482), [hash 검증](../../internal/store/logical_import.go#L609)

테이블 순서는 FK 의존성에 맞춘 고정 목록이다. linked reactive revision은 supersedes 선행 순서로 정렬하며 없는 부모나 순환을 거부한다. 과거 schema pin을 가진 객체는 type version을 잠시 pin으로 변경 → 객체 INSERT → 현재 version 복원이라는 3문장을 같은 그룹에 넣고 영향 행 수를 검사한다. 정상 런타임의 schema pin 트리거를 해제하지 않는다. [pin 그룹](../../internal/store/logical_import.go#L308), [revision 정렬](../../internal/store/logical_import.go#L559)

모든 그룹을 먼저 만들고 `rhiza.ValidateExecuteRequest`가 허용하는 문장 수·복제 mutation 크기에 맞춰 배치를 사전 검증한다. 각 request ID는 `imp-<파일 SHA 앞 48자>-<배치 번호>`이다. 적용 중 `ErrCommitUnknown`에만 같은 배치를 1초 간격, 총 최대 3회 시도하며 그 밖의 오류는 즉시 반환한다. 배치 하나는 원자적이지만 **전체 import는 여러 배치에 걸치므로 실패 시 이미 적용한 앞 배치가 남을 수 있다**. 처음부터 다시 호출하면 비어 있지 않은 target 검사에서 거부될 수 있어, 배치 재시도와 전체 명령 재개를 같은 보장으로 취급하지 않는다. [배치 구성·적용](../../internal/store/logical_import.go#L243), [재시도](../../internal/store/logical_import.go#L286)

완료 후 `VerifyLogicalImport`는 한 번의 `ReadSnapshot(ReadLinearizable)` 안에서 21개 테이블의 모든 행·scalar를 정렬 비교한다. 행 수 또는 값이 다르면 실패한다. 결과 stdout JSON은 `file_sha256`, 테이블별 `name/rows/digest`, 전체 `digest`이다. 테이블 digest는 정렬한 각 행의 JSON과 개행을 SHA-256에 넣고 `sha256:`을 붙인다. 전체 digest는 테이블 요약 JSON의 SHA-256이다. 이 검증은 이미 import했고 외부 쓰기가 멈춘 target에 적합하며 원격 합의 이력·voter 자격·외부 검색 색인의 이전을 검증하지 않는다. [전체 행 검증](../../internal/store/logical_import.go#L421), [digest](../../internal/store/logical_import.go#L651)

## 7. 객체 저장소와 peer 설정

다음은 Compos가 `rhiza.Config`에 전달하는 계약이다. provider별 SDK 내부 동작이나 외부 Rhiza 저장소 구현은 이 프로젝트 밖이므로 여기서 추가로 추정하지 않는다.

| 서버 설정 | 값·효과 |
| --- | --- |
| `-bind` / `COMPOS_BIND` | 애플리케이션 HTTP bind이며 코드 기본값은 `127.0.0.1:3000`이다. server 이미지의 환경변수 기본값은 `0.0.0.0:7878`이다. |
| `-database` / `COMPOS_SQLITE_PATH` | 기본 `compos.sqlite`; Rhiza data가 비어 있을 때 사용한다. |
| `-rhiza-data` / `COMPOS_RHIZA_DATA_DIR` | 비어 있지 않으면 내장 Rhiza를 선택한다. |
| `COMPOS_RHIZA_CLUSTER_ID`, `COMPOS_RHIZA_NODE_ID` | 기본 `compos`, `node-1`이다. |
| `COMPOS_RHIZA_PEER_ADDR`, `COMPOS_RHIZA_MEMBERS` | peer bind 기본 `127.0.0.1:0`; members는 JSON `[]rhiza.Member`이다. |
| `COMPOS_RHIZA_PEER_TOKEN`, `COMPOS_RHIZA_PEER_TOKENS` | scalar token 또는 node ID별 token JSON map이다. 둘을 함께 지정하면 거부한다. |
| `COMPOS_RHIZA_ADMIN_TOKEN` | 하위 Rhiza Config로 전달한다. HTTP 사용자 token과 별개이다. |
| `COMPOS_RHIZA_OBJECT_PROVIDER`, `DIR`, `BUCKET`, `PREFIX` | 마지막 세 이름은 동일한 `COMPOS_RHIZA_OBJECT_` 접두사를 사용한다. provider·로컬 객체 경로·bucket·prefix를 전달한다. |
| `COMPOS_RHIZA_OBJECT_ENDPOINT` | 선두 `http://`, `https://`를 제거해 전달한다. TLS 여부는 별도 INSECURE 값에 달려 있다. |
| `COMPOS_RHIZA_OBJECT_REGION`, `ACCESS_KEY`, `SECRET_KEY`, `SESSION_TOKEN` | 동일한 접두사 아래에서 region·자격 증명을 전달한다. |
| `COMPOS_RHIZA_OBJECT_INSECURE` | `strconv.ParseBool`; 기본 false이며 파싱 실패는 시작 실패이다. |
| `COMPOS_RHIZA_OBJECT_DURABILITY` | 객체 설정이 없으면 기본 `async`; provider/dir/endpoint/bucket 중 하나라도 있으면 기본 `before-ack`이다. 멤버가 2개 이상이면 `before-ack` 이외 값으로 시작할 수 없다. |

[서버 설정 전달](../../cmd/server/main.go#L23)

per-node token map을 사용하면 현재 node와 모든 member의 token이 필요하다. 각 member public key는 `rhiza.PeerPublicKey(clusterID,nodeID,token)`으로 도출하며 이미 지정한 public key가 다르면 거부한다. map이 없으면 members와 scalar token을 그대로 하위 계층에 전달한다. 이 함수 자체가 모든 단일 token 구성의 안전성을 검증하는 것은 아니다. [peer 자격 증명](../../cmd/server/main.go#L147)

import CLI는 별도로 `COMPOS_RHIZA_OBJECT_SERVICE_ACCOUNT`를 `ObjStoreServiceAccount`에 넣고 durability를 항상 `before-ack`으로 설정한다. 현재 서버 Config 구성에는 이 service-account 환경변수를 명시적으로 전달하는 코드가 없다. 이 차이를 provider 인증이 동일하다는 가정으로 덮지 않는다. [import Config](../../cmd/storage-import/main.go#L68), [서버 Config](../../cmd/server/main.go#L55)

## 8. Rust Operator와 CRD 계약

### 8.1 실행 경계와 공통 동작

Rust workspace 멤버는 `crates/compos-operator` 하나이며 edition 2024, 최소 Rust 1.96을 선언한다. Operator는 기본 Kubernetes client로 접속하고 `COMPOS_OPERATOR_NAMESPACE`의 namespace만 감시한다. 기본 namespace는 `compos`이다. `--print-cluster-crd`와 `--print-search-syncer-crd`는 각각 CRD JSON을 출력하고 client 생성 전에 종료한다. 두 controller는 CR 자체 watch로 실행되며 코드에서 별도 자식 리소스 watch나 finalizer를 등록하지 않는다. [workspace](../../Cargo.toml#L1), [entry point](../../crates/compos-operator/src/main.rs#L138)

자식 리소스는 CR ownerReference를 붙이고 server-side apply의 field manager `compos-operator`, force=true로 반영한다. 반복 reconcile은 이 apply를 다시 실행한다. 적용 순서 전체가 Kubernetes 트랜잭션은 아니므로 중간 실패 시 앞서 생성한 자식이 남을 수 있다. API 오류는 error policy에 따라 30초 뒤 재시도한다. CR 삭제 시 외부 객체 bucket/prefix를 정리하는 코드나 복구 절차를 실행하는 finalizer는 이 소스에 없다. [apply](../../crates/compos-operator/src/main.rs#L365), [error policy](../../crates/compos-operator/src/main.rs#L217), [ownerReference](../../crates/compos-operator/src/main.rs#L276)

### 8.2 ComposCluster

API는 `compos.mrchypark.dev/v1alpha1`, namespaced kind `ComposCluster`, plural `clusters`, shortname `cc`이며 status subresource를 둔다. 저장된 CRD 파일은 확장자가 yaml이지만 내용은 JSON이다. [Rust 정의](../../crates/compos-operator/src/main.rs#L21), [CRD](../../deploy/charts/operator/crds/clusters.compos.mrchypark.dev.yaml#L1)

| spec 필드 | 필수·기본값 |
| --- | --- |
| `server.image` | 필수이다. |
| `server.envSecret`, `server.port` | `compos-env`, 7878이다. |
| `server.rhiza.clusterId`, `peerPort`, `adminTokenSecretRef` | `compos`, 8100, `compos-env`이다. |
| `replicas`, `imagePullPolicy` | 3, `IfNotPresent`이다. |
| `objectStore.provider`, `endpoint`, `bucket` | `s3`, `rustfs:9000`, `compos-rhiza`이다. |
| `objectStore.prefix`, `region`, `insecure`, `secretRef` | `compos`, `us-east-1`, true, `compos-env`이다. |

[타입·default](../../crates/compos-operator/src/main.rs#L31), [기본 함수](../../crates/compos-operator/src/main.rs#L421)

reconcile 검증은 replicas가 3 이상 홀수인지, HTTP/peer port가 양수인지, image·cluster ID·object endpoint·bucket이 공백이 아닌지 검사한다. port 상한, provider enum, prefix 공백·중복, secret 존재·key 내용은 이 검증 함수가 검사하지 않는다. CRD가 모든 runtime 검증을 admission 단계에서 시행한다고 해석하지 않는다. 실패하면 `phase=Invalid`, `lastError`, `observedGeneration`을 기록하고 기존 나머지 status를 보존하며 30초 후 재시도한다. [검증](../../crates/compos-operator/src/main.rs#L229)

유효하면 `<name>-headless` Service → `<name>` HTTP Service → `<name>` StatefulSet을 apply한다. headless Service는 `publishNotReadyAddresses=true`, HTTP와 UDP peer 포트를 제공한다. member ID는 `<name>-<ordinal>`, host는 `<name>-<ordinal>.<name>-headless`이며 `url/log_url`은 HTTP server port, `peer_url`은 `quic://...:<peerPort>`이다. private token map은 membership JSON에 넣지 않고 server envSecret으로 제공할 수 있다. [member·서비스 생성](../../crates/compos-operator/src/main.rs#L261)

StatefulSet은 `podManagementPolicy=Parallel`, data volume은 PVC가 아닌 `emptyDir`, Rhiza data 경로는 `/data/rhiza`이다. node ID는 pod metadata.name에서 받는다. object 설정을 env로 넣고 durability는 `before-ack`으로 고정한다. `server.envSecret`은 envFrom, admin token은 해당 Secret의 `COMPOS_RHIZA_ADMIN_TOKEN`, 객체 key/secret은 `objectStore.secretRef`의 `COMPOS_RHIZA_OBJECT_ACCESS_KEY/SECRET_KEY`에서 가져온다. 별도 initContainer나 객체 bucket 생성은 이 생성 함수에 없다. [StatefulSet](../../crates/compos-operator/src/main.rs#L312)

readiness는 `/api/v1/health`, 초기 지연·주기 각각 5초이다. status의 `readyReplicas`가 spec.replicas와 같으면 `Running`, 아니면 `Deploying`이며 정상 reconcile은 10초 후 반복한다. `Running` 판정은 실제 명령·복제·객체 archive 복구를 실행해 보는 검사가 아니다. server.port를 바꿔도 생성 env에 `COMPOS_BIND`를 추가하지 않으므로 실제 서버 bind는 이미지 기본값이나 envSecret 설정과 맞아야 한다. [status](../../crates/compos-operator/src/main.rs#L185), [생성 env·probe](../../crates/compos-operator/src/main.rs#L335)

### 8.3 ComposSearchSyncer

같은 group/version의 namespaced kind `ComposSearchSyncer`, plural `searchsyncers`, shortname `css`이다. spec은 다음 필드를 제공한다. [타입 정의](../../crates/compos-operator/src/main.rs#L466), [CRD](../../deploy/charts/operator/crds/searchsyncers.compos.mrchypark.dev.yaml#L1)

| spec 필드 | 필수·기본값 |
| --- | --- |
| `syncer.image` | 필수이다. |
| `compos.endpoint`, `compos.workspaceId` | 필수이다. |
| `compos.authSecretRef` | 선택 `{name,key}`; 있으면 `COMPOS_API_TOKEN`으로 주입한다. |
| `meilisearch.image`, `replicas`, `ephemeralStorage` | `meilisearch/meilisearch:v1.12`, 1, `512Mi`이다. |
| `meilisearch.masterKeySecretRef`, `resources` | 선택 Secret key 참조 및 임의 JSON 리소스 설정이다. |
| `index.uid`, `primaryKey` | `compos-objects`, `id`이다. |
| `sync.intervalSeconds`, `batchSize` | 5, 100이다. |

검증은 compos endpoint/workspace, 두 image가 공백이 아닌지, Meilisearch replicas가 정확히 1인지, interval·batch가 양수인지 확인한다. explicit master-key ref는 name·key가 비어 있으면 거부한다. Secret은 immutable=true이고 참조 key가 있어야 한다. explicit Secret이 없으면 Invalid이다. 관리 Secret이 이미 있으면 현재 CR UID의 ownerReference가 있어야 한다. 관리 Secret이 아직 없으면 32바이트 난수의 64자리 hex master key를 생성해 `<name>-meilisearch`의 `MASTER_KEY`로 저장한다. 기존 관리 Secret을 reconcile마다 회전하지 않는다. [Secret 검증](../../crates/compos-operator/src/main.rs#L630), [생성·적용](../../crates/compos-operator/src/main.rs#L795), [spec 검증](../../crates/compos-operator/src/main.rs#L961)

자식은 `<name>-meilisearch` Deployment·Service, `<name>-syncer` Deployment 및 필요할 때 관리 Secret이다. Meilisearch는 production mode·analytics 비활성화·master key를 설정하고 7700 `/health` readiness를 둔다. `ephemeralStorage`는 `/tmp`에 붙인 emptyDir의 sizeLimit이다. DB data 경로를 그 volume에 연결하거나 PVC를 설정하는 코드는 없다. syncer는 1 replica, compos endpoint/workspace 및 index·interval·batch·Meilisearch 자격 증명을 env로 받으며 requests 50m/64Mi, limits 200m/128Mi이다. syncer container readiness probe는 생성하지 않는다. [Meilisearch 리소스](../../crates/compos-operator/src/main.rs#L687), [syncer 리소스](../../crates/compos-operator/src/main.rs#L749)

두 Deployment에 ready replica가 하나 이상이면 `Running`, Meilisearch만 ready이면 `SyncerPending`, 아니면 `Deploying`이다. `Running`일 때만 lastError를 지운다. Invalid 때도 이전 readiness 필드를 보존할 수 있다. 정상·Invalid reconcile은 30초 간격이며 status는 `observedGeneration`, `meilisearchReady`, `syncerReady`, `phase`, `lastError`를 제공한다. 검색 index의 내용·최신성·rebuild 완료를 직접 확인하는 판정은 아니다. [상태 전환](../../crates/compos-operator/src/main.rs#L891)

## 9. 배포 산출물과 CI

### 9.1 이미지·Helm·예제 매니페스트

Dockerfile은 Go 1.27.0 빌더로 server·storage-import·CLI·search-syncer를 만들고 Rust 1.96.1 빌더로 operator를 만든다. server·CLI·operator 이미지는 UID/GID 10001 비root 사용자를 만들며 search-syncer는 distroless nonroot를 쓴다. server 기본 HTTP는 `0.0.0.0:7878`, SQLite 경로는 `/data/compos.sqlite`이고 curl healthcheck를 둔다. `storage-import` target은 server 기반에 도구를 추가하지만 healthcheck를 끄고 API·scheduler 대신 import만 실행한다. [이미지 전체 계약](../../Dockerfile#L1)

Helm chart는 CRD 두 개, namespace 범위 Role/RoleBinding, ServiceAccount, Operator Deployment를 배포한다. Operator Deployment는 1 replica, Recreate 전략이며 release namespace를 `COMPOS_OPERATOR_NAMESPACE`로 넣는다. 기본 이미지는 `compos-operator:local`이다. Role에는 CR/status, Services·Secrets·Deployments·StatefulSets 등의 권한과 현재 reconcile 코드가 사용하지 않는 일부 리소스 권한도 있다. 객체 저장소 서버·bucket을 자동 제공하는 chart는 아니다. [Deployment](../../deploy/charts/operator/templates/deployment.yaml#L1), [values](../../deploy/charts/operator/values.yaml#L1), [Role](../../deploy/charts/operator/templates/role.yaml#L1)

`deploy/operator` 예제의 이미지 경로는 교체용 placeholder이다. `deploy/compos/vcluster-no-pvc-s3.yaml`은 별도로 RustFS·bucket init Job·3 replica Compos StatefulSet을 선언하며 RustFS와 Compos 모두 emptyDir를 사용한다. 이 예제의 headless 포트 이름·구성 및 `HQL_S3_*` initContainer env는 현재 Operator의 QUIC member 생성 계약과 다르므로 같은 실행 경로로 합쳐 설명하지 않는다. Secret envFrom에 의존하므로 이 파일만으로 현재 Rhiza 설정이 완성되었다고 판단할 수 없다. [Operator 예제](../../deploy/operator/compos-cluster.example.yaml#L1), [직접 배포 예제](../../deploy/compos/vcluster-no-pvc-s3.yaml#L1), [init·서버 env](../../deploy/compos/vcluster-no-pvc-s3.yaml#L150)

`deploy/tg900/compos-conductor.yaml`은 별도의 1 replica Deployment와 GCS provider·before-ack·emptyDir 구성을 선언한다. 파일에 있는 `registry.example.invalid` image reference는 실제 배포 가능한 registry 검증 근거가 아니다. Conductor는 외부 실행 서비스로 연결하는 매니페스트만 이 프로젝트에서 읽었으며 외부 프로젝트 구현은 조사하지 않았다. 이 파일과 과거 검증 보고서만으로 현재 v0.19·v5 배포가 동작한다고 주장하지 않는다. [선언된 서버 설정](../../deploy/tg900/compos-conductor.yaml#L29)

### 9.2 CI에 선언된 검사와 비활성 게시

`test compos`는 코드·도구·scripts·deploy·docs·Dockerfile·workflow 변경의 push/PR에 반응한다. shell syntax·ShellCheck, Helm lint/template, Docker Go 버전 정합성, `go test ./...`, user journey, `go vet`, 별도 legacy export 모듈 test/vet, Rust fmt/test/clippy와 두 generated CRD diff를 수행하도록 선언되어 있다. 별도 `rhiza-cluster` job은 클러스터 shell script를 실행한다. 이것은 workflow 정의이지 이 문서를 작성한 세션이나 최근 CI 실행의 성공 기록이 아니다. [트리거·검사](../../.github/workflows/test.yaml#L1)

이미지 workflow는 test workflow 완료 또는 수동 실행을 트리거로 선언하지만 docker job이 `if: ${{ false }}`여서 비활성이다. 내부 matrix는 server·CLI·operator·search-syncer 4개이며 Dockerfile의 storage-import target은 포함하지 않는다. 내부 로직은 변경 경로 gate, Google Artifact Registry login, linux/amd64 build/push를 선언한다. CLI publish 역시 job의 `if: ${{ false }}`로 비활성이다. 따라서 소스 변경이나 tag가 자동 배포·공개 release로 이어진다는 계약은 현재 없다. [이미지 게시](../../.github/workflows/build-images.yaml#L1), [CLI 게시](../../.github/workflows/publish-cli.yaml#L1)

비활성 CLI job의 선언상 산출물은 linux amd64/arm64, macOS amd64/arm64, Windows amd64의 tar.gz와 SHA256SUMS이며 `compos/vX.Y.Z` tag release를 작성한다. 릴리스 문구는 unsigned를 명시한다. 이 경로의 작성된 설정을 실제 배포·서명 검증 완료와 구별한다. [package·release 설정](../../.github/workflows/publish-cli.yaml#L100)

## 10. 주요 사용자 흐름과 실패 후 상태

| 전제 | 동작 | 결과·실패 경계 |
| --- | --- | --- |
| 빈 로컬 SQLite 경로 | 서버 시작 | PRAGMA·DDL·v5 identity를 적용한 뒤 HTTP를 시작한다. 초기화 오류이면 시작하지 않는다. |
| 구형 schema identity의 DB | SQLite/Rhiza로 개방 | identity 불일치로 거부한다. 자동 변환·인증 데이터 migration은 없다. |
| 외부 승인 principal과 유효한 명령 | Kernel Execute | 외부 승인을 중복 조회 전에 확인한다. SQL 도메인 변경·기록의 원자성과 외부 인가 시스템의 결정은 별도 경계다. |
| Rhiza snapshot 도중 적용 slot이 바뀜 | 복수 조회 | callback 전체를 다시 실행한다. 8회에도 안정되지 않으면 실패한다. |
| 새 Rhiza target과 유효한 v0.12/v2 export | storage-import | 사전 검증 후 여러 원자적 배치를 적용하고 모든 행을 비교한다. 중간 실패 시 부분 import가 남을 수 있다. |
| 유효한 ComposCluster CR | reconcile | Service 2개와 StatefulSet을 반복 apply한다. Kubernetes 부분 적용과 SQL commit은 서로 독립이다. |
| 유효한 SearchSyncer CR과 Secret | reconcile | 검색 자식 리소스를 apply한다. Running은 Deployment readiness 판정이며 데이터 정합성 검증은 아니다. |
| 3 voter 중 한 노드를 중단하고 상태를 보존함 | 나머지 두 노드 쓰기 후 재시작 | 테스트는 계속 쓰기·추격·복제 성공을 기대한다. 상태를 잃은 같은 voter의 새 경로 재시작은 `ErrVoterStateLost`를 기대한다. |

각 행의 근거는 앞 절의 실행 함수와 아래 테스트 지도에 연결한다. 마지막 행은 **테스트가 요구하는 동작**이며 이번 작업에서 클러스터를 구동한 결과가 아니다.

## 11. 테스트 근거, 실제 확인, 알려진 한계

### 11.1 테스트를 읽어서 확인한 계약

| 테스트 소스 | 작성된 검사 내용 | 증명하지 않는 범위 |
| --- | --- | --- |
| [`store_test.go`](../../internal/store/store_test.go#L11) | 초기 identity·connection FK, legacy 거부, JSON·시각 CHECK, 온라인 백업 재개방을 검사한다. | 실제 운영 DB의 integrity, crash·디스크 장애 후 복구를 증명하지 않는다. |
| [`rhiza_test.go`](../../internal/store/rhiza_test.go#L12) | 대화형 tx 거부, native exec/read, FK, request ID 길이, 전체 DDL 후 identity 공개, 불완전 metadata 거부를 검사한다. | 다중 노드 업그레이드의 전체 장애 조합을 증명하지 않는다. |
| [`read_test.go`](../../internal/store/read_test.go#L75) | SQLite 한 read transaction, Rhiza AppliedSlot 변경 거부를 검사한다. | 실제 장시간 혼합 부하에서 snapshot 성공률을 측정하지 않는다. |
| [`logical_import_test.go`](../../internal/store/logical_import_test.go#L14) | int64 정밀도·과거 schema pin·재개방 비교, 잘못된 입력·부분 target 거부, revision 순서, pin 복원 실패 시 그룹 rollback, commit-unknown 한정 재시도를 검사한다. | 실제 GCS/S3/Azure 전체를 통한 source→target 이동 검증은 아니다. |
| [`export main_test.go`](../../tools/rhiza-v012-export/main_test.go#L17) | filesystem read replica export, blob 거부, AppliedSlot 변경 시 결과 미게시를 검사한다. | 실제 GCS 자격 증명·원격 archive의 운영 상태를 검증하지 않는다. |
| [`rhiza_cluster_test.go`](../../internal/store/rhiza_cluster_test.go#L18) | 3 peer의 동시 UNIQUE 경쟁, 한 노드 종료 후 쓰기, voter 상태 유실 거부, 보존한 경로 재시작·추격·후속 복제를 검사한다. | Kubernetes pod 재생성, 전체 voter 상태 유실, 실클라우드·다중 provider 동등성을 검증하지 않는다. |
| [`operator tests.rs`](../../crates/compos-operator/src/tests.rs#L69) | CRD·기본값·홀수 replica 검증·생성 리소스의 emptyDir/ownerReference·Secret 조건·Invalid status를 검사한다. | 실 Kubernetes API 적용·네트워크·권한·rollout·데이터 복구를 실행하지 않는다. |

### 11.2 현재 소스에서 확인되는 검증 한계

- 클러스터 테스트는 `COMPOS_RHIZA_CLUSTER_TEST_ENDPOINT`가 없으면 skip한다. CI script는 digest로 고정한 MinIO 호환 컨테이너와 고유 네트워크를 만들고 해당 테스트만 실행하며 trap으로 제거를 시도한다. 현재 script가 사용하는 것은 Floci가 아니다. S3 호환 endpoint 검사 결과를 GCS·Azure SDK 계약 검사로 확대할 수 없다. [skip·SDK](../../internal/store/rhiza_cluster_test.go#L18), [script](../../scripts/e2e-rhiza-cluster.sh#L1)
- Operator의 emptyDir는 pod 교체 시 기존 voter 상태를 보존하는 계약이 아니다. 테스트는 새 DataDir로 같은 voter를 열면 fail-closed를 기대하고, 성공하는 restart는 보존한 기존 경로를 쓴다. 객체 저장소 before-ack 설정만으로 이 간극이 해결되었다고 주장할 수 없다. 자동 voter replacement나 복구 절차는 현재 Operator 코드에서 확인되지 않는다. [emptyDir](../../crates/compos-operator/src/main.rs#L329), [유실·재시작 검사](../../internal/store/rhiza_cluster_test.go#L134)
- runtime 서버·import·legacy exporter·Operator가 노출하는 객체 저장소 설정은 서로 다르다. 특히 legacy export는 gcs/filesystem만 허용하고 Operator는 endpoint/bucket과 S3 형태 key Secret을 요구한다. 하위 Rhiza의 provider 지원 전체와 현재 Compos 진입점의 지원 범위를 동일시하지 않는다.
- 여러 import 배치, 여러 Rhiza 초기 Migration, Kubernetes 여러 리소스 apply는 각각 전체 작업을 하나의 원자적 작업으로 만들지 않는다. 중간 상태의 가능성은 각 절에 명시한 경계대로 남는다.
- CRD 생성 결과 diff와 소스 테스트는 선언·렌더링 정합성을 검사한다. Operator status의 Running은 데이터 최신성이나 복구 능력의 실측 결과가 아니다. CI 설정도 최근 실행 결과를 대신하지 않는다.
- 현재 조사한 저장소 테스트와 CI에는 Chaos Mesh 실험으로 fault injection과 recovery를 검증하는 단계가 없다. 노드 핸들을 닫는 테스트를 네트워크 분할·디스크 장애·클라우드 장애에 대한 chaos 검증으로 부르지 않는다. [클러스터 검사 범위](../../internal/store/rhiza_cluster_test.go#L113), [CI](../../.github/workflows/test.yaml#L115)

### 11.3 이 문서 작성에서 실제 수행한 확인

작업 트리 상태, 현재 Go/Rust/SQL·Dockerfile·CRD·Helm·CI·테스트 소스를 읽고 테이블 수 및 문서 source link를 정적으로 확인했다. 테스트·빌드·컨테이너·클러스터·객체 저장소 호출·운영 배포는 실행하지 않았다. 따라서 이 문서에는 이번 세션에서 실행한 runtime 검증 통과 주장이 없다. 문서 이외의 프로덕션 파일이나 다른 작성자의 파일은 수정하지 않는다.

## 12. 소스 지도

| 계약 | 주 소스 |
| --- | --- |
| SQLite 수명·DSN·identity·백업 | [`internal/store/store.go`](../../internal/store/store.go#L19) |
| Rhiza 개방·초기 Migration·ID | [`internal/store/rhiza.go`](../../internal/store/rhiza.go#L17) |
| 쓰기·읽기·snapshot 경계 | [`tx.go`](../../internal/store/tx.go#L18), [`read.go`](../../internal/store/read.go#L14) |
| 22개 테이블의 완전한 DDL | [`schema.sql`](../../internal/store/schema.sql#L1) |
| schema identity 불일치 거부 | [`store.go`](../../internal/store/store.go#L113), [`rhiza.go`](../../internal/store/rhiza.go#L47) |
| Kernel 원자성·멱등성·권한 | [`executor.go`](../../internal/kernel/executor.go#L59), [`rhiza.go`](../../internal/kernel/rhiza.go#L250) |
| 논리 이동 형식·적용·검증 | [`logical_import.go`](../../internal/store/logical_import.go#L20), [`storage-import`](../../cmd/storage-import/main.go#L19), [`v0.12 exporter`](../../tools/rhiza-v012-export/main.go#L92) |
| 서버 설정·health | [`cmd/server`](../../cmd/server/main.go#L23), [`health`](../../internal/server/server.go#L113) |
| Operator·CRD 원본 타입 | [`main.rs`](../../crates/compos-operator/src/main.rs#L21) |
| 저장된 CRD·Helm·이미지 | [`Cluster CRD`](../../deploy/charts/operator/crds/clusters.compos.mrchypark.dev.yaml#L1), [`SearchSyncer CRD`](../../deploy/charts/operator/crds/searchsyncers.compos.mrchypark.dev.yaml#L1), [`Helm Deployment`](../../deploy/charts/operator/templates/deployment.yaml#L1), [`Dockerfile`](../../Dockerfile#L1) |
| CI·클러스터 검사 선언 | [`test workflow`](../../.github/workflows/test.yaml#L1), [`cluster script`](../../scripts/e2e-rhiza-cluster.sh#L1), [`image workflow`](../../.github/workflows/build-images.yaml#L1), [`CLI workflow`](../../.github/workflows/publish-cli.yaml#L1) |

*EGM 초안 구조를 현재 코드의 역명세에 맞춰 적용했다.*

현재 배포 매니페스트·Operator가 자식 리소스를 만든다는 사실은 Goauthy/Grant adapter 주입을 뜻하지 않는다. 기본 cmd/server는 nil authority이므로 GET health 외 API를 거부한다. Conductor·syncer의 서비스 인가 계약은 미구현이며 기존 token/Secret 선언만으로 operational 통합을 주장하지 않는다. [기본 서버](../../cmd/server/main.go#L93), [Authority](../../internal/server/authority.go#L24).

