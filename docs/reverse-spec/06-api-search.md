# 06. HTTP API와 검색 계약

- 기준일: 2026-11-11. 현재 서버의 HTTP surface를 설명한다.
- CLI와 내장 웹 콘솔은 제거되었다. 서버 API 소비자는 외부 client 또는 통합 서비스다.

## API 목록

`W=/api/v1/workspaces/{workspace_id}`로 표기한다. 모든 route는 credential 없이 접근할 수 있다.

| Method·경로 | 용도 |
|---|---|
| GET `/api/v1/health` | storage kind, cluster, readiness |
| GET `/api/v1/search-syncer/workspace` | search-syncer가 사용할 workspace |
| POST `/api/v1/workspaces` | 빈 도메인 모델을 가진 workspace 생성 |
| GET `/api/v1/kernel/command-registry` | 공개 command 정의 |
| POST `/api/v1/kernel/commands` | Kernel command 제출 |
| GET `W/command-surface` | 현재 객체형·관계·명령 표면 |
| GET `W/definitions` | Task·Function·Workflow 정의 목록 |
| GET `W/definitions/{revision_id}` | 고정 정의 revision |
| GET `W/objects/{object_id}/relations` | 객체 관계 목록 |
| POST `W/adapter/inbound-events` | 외부 수신 증거 저장 |
| POST `/internal/v1/conductor/callbacks` | Conductor 실행 결과 수신 |
| GET `/v1/connector/objects/{object_key}/schema` | connector schema |
| GET `/v1/connector/objects/{object_key}/records/{record_id}` | connector record |
| GET `W/objects/{object_id}` | 객체 단건 조회 |
| GET `W/objects` | 일반 객체 목록 |
| GET `W/object-types/{object_type_id}/objects` | 제한된 ID scope와 페이지 조회 |
| POST `W/object-types/{object_type_id}/objects` | typed create |
| PATCH `W/object-types/{object_type_id}/objects/{object_id}` | typed update와 CAS |
| DELETE `W/object-types/{object_type_id}/objects/{object_id}` | typed archive와 CAS |
| POST `W/objects` | ID 묶음 batch read |
| GET `W/work` | 현재 `local` principal의 human 배정 |
| GET `W/work/{stream_id}` | Journal과 pinned definition 기반 Work 상세 |
| GET `W/events` | 도메인 Event 목록 |
| GET `W/command-ledger` | 서버 command ledger 목록 |
| GET `W/command-ledger/{command_id}` | command 결과 조회 |
| GET `W/object-types/{object_type_id}/schema-revisions` | 객체형 schema revision 목록 |
| GET `W/projection/events` | 검색 projection event |
| GET `W/projection/snapshot` | 검색용 객체 snapshot |
| POST `W/search` | Meilisearch 검색 proxy |

`POST /api/v1/kernel/commands`는 최대 8 MiB의 command envelope를 받고 성공 시 `command_id`, `outcome`, `aggregate`, `projection`을 반환한다. 오류는 `{error:{code,category,retryable,message,details?}}` 형식이다. [handler](../../internal/server/server.go#L375)

## Typed record scope

Typed 목록은 하나의 `filter`, `limit`, `offset`을 받는다.

```text
GET /api/v1/workspaces/{workspace_id}/object-types/{object_type_id}/objects
    ?filter=(id = "obj_a" || id = "obj_b")&limit=50&offset=0
```

ID equality 두 그룹까지 `&&` 교집합을 지원하고, 한 그룹은 199개 ID, 두 그룹 합계는 200개다. 필터 뒤 `updated_at DESC,id`로 정렬하고 `page.next_cursor`에 다음 숫자 offset 또는 null을 반환한다. [scope parser](../../internal/server/object_resources.go#L20)

## Command ledger와 idempotency

command ledger는 `(workspace_id, principal_id, idempotency_key)`로 중복을 판별한다. 같은 키에서 command ID, command type, payload hash, target, preconditions, causation 또는 requested actor가 달라지면 충돌한다. 동일 envelope는 저장된 receipt를 duplicate로 반환한다. [실행](../../internal/kernel/executor.go#L59)

## Search projection

Search syncer는 workspace snapshot과 projection event를 읽어 Meilisearch를 갱신한다. 서버 검색은 `q`, `filter`, `sort`, `limit`, `offset`, `attributesToRetrieve`를 받으며 workspace 필터를 강제한다. 과거 Knowledge 문서가 남은 인덱스에서는 `object_id`가 없는 hit을 제외하고 `returnedHits`를 반환한다. 엔진 전체 count와 facet는 반환하지 않는다. [search handler](../../internal/server/search.go#L29)

한국어 제목을 위해 syncer는 초성·자모·혼합 표현을 생성한다. 이 필드는 검색 인덱스에만 사용하며 객체 payload의 정본 값이 아니다. [검색 필드](../../cmd/search-syncer/main.go#L460)

## 테스트 근거

HTTP route, workspace CRUD, typed pagination, Work, projection, search와 인증 없는 접근은 `internal/server` 테스트에서 다룬다. 전체 `go test ./...`와 `scripts/e2e-user-journey.sh`를 2026-11-11에 실행해 통과했다. 실제 Meilisearch 운영 인스턴스와 외부 client 통합은 이 검증에 포함되지 않는다.
