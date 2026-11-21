# 05. 저장소와 배포

- 기준일: 2026-11-11. 현재 Go 서버와 저장소 소스를 기준으로 작성했다.
- 범위: SQLite·embedded Rhiza, canonical schema, backup·export, 객체 저장소, Docker·CI.

## 저장 모드

`-rhiza-data`/`COMPOS_RHIZA_DATA_DIR`가 비어 있으면 SQLite를 열고, 값이 있으면 embedded Rhiza를 연다. 별도 DB listener는 열지 않는다. 시작 로그는 다음 topology를 기록한다. [서버 선택](../../cmd/server/main.go#L34), [topology](../../cmd/server/main.go#L125)

| topology | 조건 |
|---|---|
| `standalone/sqlite` | Rhiza data directory 없음 |
| `standalone/rhiza` | Rhiza data directory, member 0 또는 1 |
| `ha/rhiza` | Rhiza data directory, member 2 이상 |

Rhiza HA는 `before-ack` object storage를 요구한다. Operator는 제거되었으므로 Kubernetes 배포는 직접 manifest 또는 별도 배포 도구가 담당한다.

## Canonical schema

현재 `schema.sql`은 **22개 애플리케이션 테이블**을 선언한다. schema identity는 `compos-go-sqlite-authless-v7`이다. [DDL](../../internal/store/schema.sql#L1), [identity](../../internal/store/store.go#L20)

| 구분 | 테이블 |
|---|---|
| schema·workspace | `schema_metadata`, `workspaces` |
| 객체 정의 | `object_types`, `object_type_schema_revisions` |
| 객체·관계 | `object_instances`, `relation_definitions`, `object_relations`, `relation_cardinality_slots` |
| 필드 보조 | `object_field_index`, `field_sequence_counters`, `generated_field_values` |
| 자동화 | `reactive_rules`, `reactive_rule_revisions`, `adapter_inbound_events`, `reactive_rule_evaluations` |
| 정의·실행 | `spec_revisions`, `command_ledger`, `journal_records`, `activities` |
| 전달·projection | `outbox_events`, `projection_events` |
| workspace 생성 | `workspace_creation_requests` |

다른 schema identity의 기존 DB는 거부한다. v1~v6에서 v7로 바꾸는 자동 migration은 제공하지 않는다. [SQLite identity 검사](../../internal/store/store.go#L150), [Rhiza identity 검사](../../internal/store/rhiza.go#L46)

## 원자성과 멱등성

SQLite Kernel은 command 중복 확인, workspace·precondition 확인, 도메인 변경, command ledger, activities, outbox와 projection event를 하나의 transaction으로 커밋한다. Rhiza는 같은 효과를 하나의 SQL statement batch로 제출하고 64문장을 넘으면 분할하지 않고 거부한다. [SQLite 실행](../../internal/kernel/executor.go#L59), [Rhiza commit](../../internal/kernel/rhiza.go#L499)

Command ledger의 멱등 범위는 `(workspace_id, principal_id, idempotency_key)`다. 같은 key에서 command ID, type, payload hash, target, preconditions, causation 또는 requested actor가 달라지면 `idempotency_conflict`; 완전히 같은 명령은 저장된 receipt를 `duplicate`로 반환한다. HTTP 응답 유실만으로 commit 여부를 단정하지 않는다.

## 읽기 경계

`ReadLocal`, `ReadLinearizable`, bounded snapshot은 Store 수명 동안 일관된 읽기 API를 제공한다. Rhiza에서 read replica 지연과 readiness를 실제 linearizable commit과 혼동하지 않는다. 응답의 `canonical_linearizable` 표기는 source 계약이며 별도 운영 검증 결과가 아니다. [Store 읽기](../../internal/store/read.go#L14)

검색 snapshot은 서버가 sequence와 객체를 여러 `ReadLocal` 호출로 읽으므로 한 시점의 원자적 전체 집합을 보장하지 않는다. [projection snapshot](../../internal/server/server.go#L855)

## Backup

- SQLite `Backup`은 driver online backup을 사용한다. `-backup <path>` 후 서버는 HTTP를 시작하지 않는다. Rhiza 직접 SQLite backup은 지원하지 않는다.
[Backup](../../internal/store/store.go#L258)

## Object store와 peer 설정

Rhiza는 `COMPOS_RHIZA_OBJECT_*` env로 GCS/filesystem 및 S3 호환 object store 설정을 받는다. provider 또는 object 설정이 있으면 durability 기본값은 `before-ack`이다. 다중 member에서 `async`는 시작 단계에서 거부된다. Peer token은 `COMPOS_RHIZA_PEER_TOKENS`로 member public key를 파생하며 `COMPOS_RHIZA_MEMBERS` JSON에 raw token을 넣지 않는다. [server config](../../cmd/server/main.go#L38), [peer derivation](../../cmd/server/main.go#L135)

## Docker와 CI

Dockerfile은 `server`, `search-syncer` build target만 제공한다. server는 Go 1.27.4 빌더와 Debian runtime을 사용한다. [Dockerfile](../../Dockerfile#L1)

`test compos`는 shell syntax·ShellCheck, Go test·vet, Rhiza 3-node script를 수행한다. server 이미지 검증과 선택적 게시는 별도 workflow가 담당한다. [test workflow](../../.github/workflows/test.yaml#L1), [server image workflow](../../.github/workflows/server-ghcr.yaml#L1)

## 검증 상태

Go 전체 테스트와 shell checks를 2026-11-11에 실행해 통과했다. 실제 HA cluster, cloud object storage, fresh-disk ACK recovery와 fencing은 이번 변경에서 검증하지 않았다.
