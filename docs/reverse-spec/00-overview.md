# 00. 실행 구조와 공통 명령 계약

기준일: 2026-10-31. 현재 코드의 역명세 초안이다. 소스와 테스트 코드를 근거로 작성했으며 테스트·서버·배포를 실행하지 않았다. 정식 EGM Goldfish 리뷰 결과가 아니다.

## 목적과 경계

Compos의 공통 실행 경계, 명령의 성공·실패·중복 처리, 정본과 파생 데이터의 관계를 설명한다. 객체·정의·권한·자동화·저장의 세부 계약은 [목차](README.md)에 연결한다. 이름 변경이나 새 저장 모델을 결정하지 않는다.

## 런타임과 정본

| 구성 | 현재 역할 | 소스 |
|---|---|---|
| Go CLI | 외부 identity를 주입받아 HTTP 조회를 수행한다. Kernel 쓰기 의도는 로컬 bbolt 원장에 먼저 남긴다. | [CLI 시작](../../cmd/compos/commands.go#L36) |
| Go API 서버 | 인증·제어 경로, 읽기 API, Kernel 제출, Workflow/ReactiveRule 스케줄러, 선택적 Conductor dispatch를 제공한다. | [서버 생성](../../internal/server/server.go#L46), [구동 설정](../../cmd/server/main.go#L23) |
| Go Kernel | 명령 형식, 외부 authority의 actor 승인, 사전조건, 도메인 규칙을 검증하고 정본 변경을 커밋한다. | [Executor](../../internal/kernel/executor.go#L60) |
| 내장 Rhiza / SQLite | Rhiza 설정 시 프로세스 내 SQL 런타임을 연다. 그렇지 않으면 로컬 SQLite를 사용한다. 독립 DB 서버를 CLI가 띄우는 구조가 아니다. | [저장소 선택](../../cmd/server/main.go#L34) |
| Meilisearch + search-syncer | 객체 검색용 외부 파생 인덱스다. SQL 업무 정본을 대체하지 않는다. | [동기화](../../cmd/search-syncer/main.go#L140) |
| Rust Operator | Kubernetes 리소스를 조정한다. 업무 명령의 실행기는 아니다. | [reconcile](../../crates/compos-operator/src/main.rs#L185) |

업무 객체의 현재 값은 `object_instances`, 정의는 불변 `spec_revisions`, 실행 사실은 불변 `journal_records`에 저장한다. Work는 Task revision을 고정한 stream이며 동적 객체와 동일하지 않다. 객체 schema pin은 `values` 교체 시 최신 revision으로 바뀔 수 있다. Task의 `constraints`는 문자열 목록이고 조건식 실행기가 아니다. 상세는 [02장](02-definitions-work.md)에서 다룬다. [DDL](../../internal/store/schema.sql#L59), [정의](../../internal/store/schema.sql#L328), [Journal](../../internal/store/schema.sql#L374).

`command_ledger`는 서버의 명령 결과·중복 판정 정본이고 CLI 원장은 송신 의도·영수증 증거다. `activities`, `outbox_events`, `projection_events`는 각각 활동, 외부 전달, 조회 projection 동기화에 필요한 별도 기록이다. 모든 명령이 Journal 사실을 만드는 것은 아니다. `spec.publish`는 Journal을 추가하지 않는다. [effect 기록](../../internal/kernel/executor.go#L911), [정의 발행](../../internal/kernel/work.go#L27).

## 명령 envelope와 receipt

[Command 타입](../../internal/kernel/types.go#L31)의 필드는 `command_id`, `idempotency_key`, `workspace_id`, `requested_actor`, `command_type`, `target`, `preconditions`, `payload`, `causation`이다.

- `target`: `aggregate_type`, nullable `aggregate_id`. 생성 명령은 null 또는 빈 문자열 ID를 허용하고, ID 필수 명령은 비어 있지 않은 ID를 요구한다. registry metadata의 `must_be_null` 표기보다 실제 검증 조건이 넓다. [ID 검증](../../internal/kernel/registry.go#L128).
- `preconditions[]`: `aggregate_type`, `aggregate_id`, `expected_version`. 대상별 필요 조건은 registry와 도메인 검증에 따른다.
- `causation`: `source`, 선택적 `attempt_id`, `event_id`, `automation`. source 형식과 외부 authorizer가 승인한 actor·작업 범위를 벗어나면 거부된다.
- 명령 actor는 `user/service/automation`, 업무 participant는 `human/agent/automation`이다. 배정이나 Task의 Function 허용 목록은 actor 권한을 부여하지 않는다.

공개 제출은 `POST /api/v1/kernel/commands`이며 JSON 크기 상한은 8 MiB다. 성공은 HTTP 200, `command_id`, `outcome`, `aggregate`, `projection:{workspace_id,projection_sequence}`, 필요 시 `client_refs`를 반환한다. 내부 Receipt의 `aggregate_version` 같은 필드와 공개 JSON 모양을 동일시하지 않는다. [공개 핸들러](../../internal/server/server.go#L364), [타입](../../internal/kernel/types.go#L49).

## 실행 흐름·실패·멱등성

1. `ValidateCommand`로 형식·principal·내부 결과 causation을 확인하고 외부 `AuthorizeCommand`를 호출한다. 미설정·장애는 retryable external_authority_unavailable, 명시적 거부는 forbidden이며 중복 조회도 이 승인을 우회하지 않는다.
2. SQLite는 트랜잭션 안에서 중복 명령을 확인한다. 신규 변경은 workspace 존재·외부 actor 승인·버전 사전조건을 검사한다. 자체 membership/capability 상태는 없다.
3. 도메인 변경을 적용한 뒤 명령 원장, effect 기록, 필요한 Journal 사실을 같은 트랜잭션에 기록한다. `work.submit.changes`는 별도 원자적 변경 경로를 탄다.
4. Rhiza는 전용 실행 경로에서 guard와 변경·원장·Journal·effect를 SQL 배치로 묶는다. 64 statement 상한을 넘으면 분할 커밋하지 않고 거부한다.
5. 커밋 뒤 서버가 scheduler/dispatcher를 깨운다. 외부 실행 완료나 검색 반영까지 성공 receipt에 포함되는 것은 아니다.

근거: [SQLite 실행](../../internal/kernel/executor.go#L60), [Rhiza 실행](../../internal/kernel/rhiza.go#L18), [Rhiza 커밋](../../internal/kernel/rhiza.go#L499), [서버 wake](../../internal/server/server.go#L364).

멱등성 키의 범위는 `(workspace_id, authenticated_principal_id, idempotency_key)`다. 같은 키라도 command ID·명령 종류·payload hash·target·preconditions·causation·requested actor가 다르면 `idempotency_conflict`다. 같은 명령은 저장된 receipt를 `outcome=duplicate`로 반환한다. 키만 유지하고 attempt나 causation을 새로 만들면 동일 요청이 아니다. [중복 비교](../../internal/kernel/executor.go#L120), [UNIQUE](../../internal/store/schema.sql#L351).

중복 receipt 조회 전에 외부 명령 승인을 검사한다. SQLite와 Rhiza 모두 승인 이후 동일 명령이면 저장된 receipt를 반환한다. 신규 변경의 도메인 버전 사전조건은 중복 반환 시 다시 적용하지 않는다. HTTP 요청 승인도 그 앞에서 별도로 필요하다. [SQLite 순서](../../internal/kernel/executor.go#L75), [Rhiza 순서](../../internal/kernel/rhiza.go#L35), [권한 상세](03-auth-permissions.md).

| 실패 | HTTP / 의미 |
|---|---|
| `idempotency_conflict`, `precondition_failed` | 409 / 키 재사용 충돌 또는 상태 변경 |
| `forbidden` | 403 / 명시적 외부 거부 또는 actor 불일치 |
| `external_authority_unavailable` | 503 / 외부 승인 미설정·장애, retryable=true |
| `schema_mismatch` | 422 / 스키마 불일치 |
| `unsupported_storage` | 501 / 저장소 지원 범위 밖 |
| `quorum_unavailable` | 503 / 저장소 가용성 문제 |
| 기타 CommandError | 기본 400 |
| 그 외 오류 | 500, storage category |

오류 JSON은 `error:{code,category,retryable,message,details?}`다. HTTP 코드만으로 적용 여부를 단정하지 말아야 하며, 네트워크 응답 유실은 CLI 원장에서 재시도 가능한 모호한 상태로 다룬다. [오류 매핑](../../internal/server/server.go#L995), [CLI 분류](../../cmd/compos/http_outcome.go#L1).

## 보존 경계와 현재 한계

- Kernel은 업무 변경의 쓰기 경계다. 워크스페이스 provision·adapter 수신 증거·자동화 평가 기록까지 모든 SQL 쓰기가 Kernel 명령이라는 뜻은 아니다.
- Work 상세 조회는 Journal과 pinned spec을 사용하지만 `object_state_equals` 완료 판정은 현재 객체 상태를 읽는다. 과거 Journal만으로 모든 현재 판단을 대체할 수 없다. [완료 판정](../../internal/kernel/work.go#L1866).
- Knowledge 명령·읽기 API·그래프 projection은 제거된 현재 코드가 기준이다. Journal에 남은 과거 Knowledge kind는 신규 기능 지원 선언이 아니다. 일반 Ontology spec은 유지된다.
- source의 `canonical_linearizable` 같은 응답 표기를 실행 검증 결과로 취급하지 않는다. 읽기 일관성·배포 내구성의 실제 보장은 [05장](05-storage-deployment.md)의 구현 경계와 구분한다.

## 테스트 근거와 검증 공백

[객체 갱신 pin 테스트](../../internal/kernel/executor_test.go#L104), [receipt 검증 테스트](../../cmd/compos/receipt_validation_test.go#L13), [scope pin 테스트](../../cmd/compos/ledger_test.go#L159)는 의도된 계약을 설명하는 테스트 소스다. 이 문서 작업에서는 실행하지 않았다. 다중 노드 failover, 실제 IdP·Conductor·Meilisearch, 재시작 복구를 검증했다는 주장은 하지 않는다.

## 소스 지도

- `cmd/server/main.go`: 의존성 구성과 서버 수명.
- `internal/kernel/types.go`, `registry.go`, `executor.go`, `rhiza.go`: 공통 계약·검증·커밋.
- `internal/store/schema.sql`: 정본·기록·제약.
- `internal/server/server.go`: 공개 라우트, 제어 경로, 응답 모양.
- `cmd/compos/submit.go`, `internal/cli/ledger`: 클라이언트 의도·송신·확인 증거.
