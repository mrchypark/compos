# 06. CLI·HTTP API·웹 콘솔·검색 계약

기준일: 2026-10-31. 현재 코드에 대한 사실 대조 초안이다. 테스트는 근거 소스이며 이번 작성 중 실행하지 않았다. 정식 Goldfish 리뷰가 아니다.

## 목적·경계와 상태

사용자가 접하는 CLI, HTTP 라우트, 내장 로컬 웹 콘솔, 검색 projection을 연결한다. 도메인 payload의 세부 스키마·권한 정책·외부 callback 검증은 해당 장으로 연결한다. 로컬 `AttemptAppended → Submitted → Rejected/Committed` 증거와 서버 `applied/duplicate` 결과는 서로 다른 상태다. 네트워크 오류의 `Rejected` 기록은 서버의 미커밋 증명이 아니다. [원장 이벤트](../../internal/cli/ledger/journal.go#L30), [상태 투영](../../cmd/compos/forwarder_ledger.go#L52).

## HTTP API 전체 목록

아래 목록은 현재 [routes()](../../internal/server/server.go#L79)의 모든 등록이다. `W=/api/v1/workspaces/{workspace_id}`다. 정확한 GET health 외 모든 요청은 외부 Authority 승인을 요구한다. internal callback/connector와 syncer도 예외가 아니다. nil Authority는 503으로 거부한다. 자체 auth·actor-capabilities 라우트는 제거되었다. [Authority](../../internal/server/authority.go#L24), [03장](03-auth-permissions.md).

| Method·경로 | 계약·연결 장 |
|---|---|
| GET `/api/v1/health` | storage kind·cluster·ready와 health를 반환. 저장소 읽기 실패 시 503. Knowledge projection 상태 없음. |
| GET `/api/v1/search-syncer/workspace` | 외부 Authority 승인 후 설정된 데이터 workspace 발견. 전용 토큰 우회 없음. |
| POST `/api/v1/workspaces` | name·선택 key·idempotency key 기반 원자적 workspace provision. Kernel 명령과 구분. |
| GET `/api/v1/kernel/command-registry` | 공개 command definition 배열 `commands`. 내부 결과·기한 명령 제외. |
| POST `/api/v1/kernel/commands` | envelope 제출, 최대 8 MiB, 200 receipt 또는 구조화 오류. [00장](00-overview.md) |
| GET `W/command-surface` | 현재 객체형·관계·명령 표면. 동적 CLI와 웹 폼의 기준. |
| GET `W/definitions` | task/function/workflow 목록. kind·family_id·limit·offset. |
| GET `W/definitions/{revision_id}` | 선택한 definition 읽기. [02장](02-definitions-work.md) |
| GET `W/objects/{object_id}/relations` | 객체의 관계 목록. [01장](01-objects-relations.md) |
| POST `W/adapter/inbound-events` | adapter 수신 증거 저장. [04장](04-automation-integrations.md) |
| GET `W/objects/{object_id}` | 단일 객체 읽기. |
| GET `W/objects` | 객체 목록. 일반 조회와 아래 typed scope 조회를 구분. |
| GET `W/object-types/{object_type_id}/objects` | 정확히 하나의 `filter`를 요구하는 제한된 record scope 조회. |
| POST `W/object-types/{object_type_id}/objects` | typed create를 Kernel `object.create`로 변환. |
| PATCH `W/object-types/{object_type_id}/objects/{object_id}` | typed update와 expected version. |
| DELETE `W/object-types/{object_type_id}/objects/{object_id}` | typed archive와 expected version. hard delete 계약 아님. |
| POST `W/objects` | 객체 ID 묶음 조회용 batch 경로. HTTP POST만 보고 업무 쓰기로 분류하지 않음. |
| GET `W/work` | 현재 principal의 human 배정 중 route별 미제출·미종료 항목. 전체 Work 목록 아님. |
| GET `W/work/{stream_id}` | Journal facts와 pinned definition에 따른 Work 상세. |
| GET `W/events` | 도메인 Event 인스턴스 목록. Knowledge와 함께 제거되지 않음. |
| GET `W/command-ledger` | 서버 명령 원장 목록. CLI bbolt 원장과 다름. |
| GET `W/command-ledger/{command_id}` | 명령 증거·결과 읽기. |
| GET `W/object-types/{object_type_id}/schema-revisions` | 객체형 불변 revision 이력. |
| GET `W/projection/events` | `after`, `limit` 기반 projection event 순서 읽기. |
| GET `W/projection/snapshot` | schema_version=3, 객체 전용 snapshot. Knowledge 배열 없음. |
| POST `W/search` | workspace 범위를 강제한 Meilisearch 프록시. 아래 검색 절 참조. |
| POST `/internal/v1/conductor/callbacks` | 전용 callback token 검증, 증거 보관 후 내부 Kernel 결과 명령. |
| GET `/v1/connector/objects/{object_key}/schema` | 서비스 토큰으로 읽는 connector schema. |
| GET `/v1/connector/objects/{object_key}/records/{record_id}` | 서비스 토큰으로 읽는 connector record. |

Typed body는 `title`, `values`, `idempotency_key`, `expected_version`을 사용한다. create는 title과 idempotency_key, update는 idempotency_key·양수 expected_version·title 또는 values, archive는 idempotency_key·양수 expected_version을 요구한다. body 상한은 1 MiB이며 미등록 필드·중복 JSON key·뒤따르는 JSON 값을 거부한다. update/archive는 object type과 객체 소속을 확인하고 버전 precondition을 생성한다. command ID는 `[workspace,principal,idempotency_key]`를 JSON으로 직렬화해 hash한 값으로 만든다. [typed handler](../../internal/server/object_resources.go#L195), [명령 변환](../../internal/server/object_resources.go#L338). 인증·권한은 이 우회 API에서도 Kernel을 통과한다.

네 필드는 공통 decoder가 인식하므로 미사용 알려진 필드를 일괄 금지하지는 않는다. create의 expected_version, DELETE의 title·values는 처리에 사용하지 않는다. 자체 capability 관리 API는 없다.

## 전체 실행 명령 inventory

[definitions](../../internal/kernel/registry.go#L28)의 현재 27개 명령을 아래에 기록한다. `R`은 payload 필수 키이고, 추가 키·타입·enum·target·precondition의 정확한 기계 계약은 링크의 registry와 연결 장을 함께 읽는다. payload의 미등록 키는 허용하지 않는다. 공개 registry의 metadata는 도메인 전체 검사를 대체하지 않는다.

| 명령 | R / target aggregate | 추가 계약·담당 장 |
|---|---|---|
| `spec.publish` | family_id, revision, kind, spec / spec_revision | kind 6종, 불변 정의. [02](02-definitions-work.md) |
| `work.open` | task_revision_id, context / work_stream | deadline_at 선택, 생성 target ID null 또는 빈 문자열. |
| `work.route` | participant / work_stream | reason, workflow_step_id, workflow_route_id 선택. |
| `work.submit` | submission / work_stream | changes, workflow_step_id, workflow_route_id 선택. |
| `work.cancel` | reason / work_stream | 열린 실행 취소. |
| `function.invoke` | function_revision_id, input / work_stream | workflow_route_id 선택. |
| `event.emit` | event_revision_id, data / domain_event | subject 선택. |
| `object_type.define` | key, singular, plural, field_schema / object_type | state_machine_id 선택. [01](01-objects-relations.md) |
| `object_type.schema_evolve` | field_schema / object_type | object_type version precondition. |
| `relation_definition.define` | key, from_object_type_id, to_object_type_id, cardinality, required / relation_definition | object_type preconditions. |
| `object.create` | object_type_key, title / object_instance | values, relations 선택. |
| `object.update` | 없음 / object_instance | title, values; object_instance precondition. 도메인 검사는 별도. |
| `object.move` | to_state / object_instance | reason, force 선택, object_instance precondition. |
| `relation.link` | relation_definition_id, from_object_id, to_object_id / object_relation | relation_definition·object_instance preconditions. |
| `relation.unlink` | relation_definition_id / object_relation | object_relation precondition. |
| `reactive_rule.define` | automation_id, rule_key, name, requested_actor, trigger, predicates, action, idempotency, limits / reactive_rule | [04](04-automation-integrations.md) |
| `reactive_rule.publish_revision` | revision_id, activation_event_cursor / reactive_rule | reactive_rule precondition. |
| `reactive_rule.disable` | 없음 / reactive_rule | reason, circuit_breaker 선택, precondition. |
| `reactive_rule.retire` | 없음 / reactive_rule | reason 선택, precondition. |
| `object_type.delete` | 없음 / object_type | hard, reason 선택, precondition. archive 계약. |
| `object.delete` | 없음 / object_instance | hard, reason 선택, precondition. |
| `relation_definition.delete` | 없음 / relation_definition | hard, reason 선택, precondition. |
| `reactive_rule.delete` | 없음 / reactive_rule | hard, reason 선택, precondition. |
| `function.record_result` | invocation_id, callback_id, runtime_status, output, adapter_inbound_event_id, execution_id, provider_key, provider_run_ref, handler_version, attempt_count / work_stream | 내부 결과. error_class/error_detail 선택. |
| `agent.record_result` | 위 Function 결과와 동일 / work_stream | 내부 Agent 결과. |
| `workflow.complete` | invocation_id, runtime_status, output / work_stream | 내부 완료, error_detail 선택. |
| `work.record_deadline` | deadline_at / work_stream | scheduler 기한 사실. |

앞 23개가 공개 `Registry()`의 반환 대상이다. 뒤 4개는 registry에서 제외한다. 공개 submit handler는 Function 결과·Agent 결과·Workflow 완료 세 명령을 명시적으로 403 처리하지만 `work.record_deadline`을 같은 목록에서 막지는 않는다. 기한 명령은 Kernel에서 scheduler causation을 요구한다. 따라서 registry 비노출과 HTTP 차단을 같은 의미로 쓰지 않는다. [registry 필터](../../internal/kernel/registry.go#L82), [HTTP 차단](../../internal/server/server.go#L364), [기한 guard](../../internal/kernel/work.go#L956). ReactiveRule action 허용은 `object.create`, `object.move`, `relation.link` 세 명령이다. `knowledge.*` 및 과거 task/submission 제품 명령은 현재 명령 inventory에 없다.

## CLI 표면과 전송 흐름

[라우터](../../cmd/compos/commands.go#L71)의 고정 명령은 `help`, `version`, `workspace`, `web`, `define`, `object`, `object-type`, `relation`, `objects`, `ledger`, `search`다. registry의 모든 Kernel 명령이 전용 CLI 하위 명령을 갖는 것은 아니다. `setup`, `login`, `logout`, `whoami`는 로컬 계정 명령 제거 오류이며 `forwarder`, `task`, `submission`, `audit`도 오류다.

- `define` / `object-type define`, `object-type schema-evolve`: 동적 객체형 정의·진화.
- `object create|update|move|get`, `relation define|link|unlink`, `objects list`: 일반 객체·관계 CLI. 세부 flags는 [usage](../../cmd/compos/commands.go#L253), [mutations](../../cmd/compos/mutations.go#L1).
- `<plural-key> create|move|get|link|help`: 서버 command surface에서 단어를 먼저 확인한다. 예를 들어 `contracts create`는 `object.create`의 `object_type_key=contracts`를 구성한다. 미정의 단어는 명령 제출 전에 실패한다. [surface](../../cmd/compos/surface.go#L19).
- `workspace create`는 제어 API를 직접 호출한다. Kernel 업무 제출의 bbolt 의무와 혼동하지 않는다. [workspace](../../cmd/compos/workspace.go#L1).
- CLI identity는 주입된 identityProvider에서 server/principal/credential을 받는다. 파일 세션·COMPOS_SESSION_PATH·dev fallback은 없다. 실제 network adapter는 없으며 기본 CLI의 인증이 운영 가능하다고 주장하지 않는다. 요청 원장은 COMPOS_LEDGER_PATH 또는 플랫폼 기본 경로를 사용한다. [외부 identity](../../cmd/compos/auth.go#L9).

쓰기 흐름은 외부 identity 문맥·actor 검증 → server/principal/workspace와 원본 intent·hash를 `AppendAttempt` → `AppendSubmitted` → HTTP → receipt 검증 → `AppendCommitted`다. append 실패 시 HTTP를 보내지 않는다. 응답 성공 후 원장 committed 기록 실패는 경고이며 서버 커밋 취소가 아니다. 일시 실패는 최대 두 번, 50ms 간격으로 시도한다. [submit](../../cmd/compos/submit.go#L83), [송신](../../cmd/compos/submit.go#L179).

## bbolt 영속성과 재시도

bbolt는 업무 read replica가 아니라 append-only 요청 증거다. event와 조회 index를 저장하고 sync 실패를 반환한다. 손상 이벤트 발견 시 writable 원장은 quarantine되어 추가 append를 막는다. read-only reader는 원장을 생성하거나 quarantine 쓰기를 하지 않는다. [bbolt 구현](../../internal/cli/ledger/bbolt.go#L43), [append](../../internal/cli/ledger/bbolt.go#L85), [quarantine](../../internal/cli/ledger/bbolt.go#L168).

`ledger list`는 의도·서버·사용자·결과의 metadata를 보여준다. `ledger retry --journal PATH --attempt ID` 또는 `--limit N`은 미송신/모호한 요청을 선택한다. committed는 다시 보내지 않는다. 원장 원본과 hash를 검증하고 기존 command·causation을 그대로 재사용한다. 외부 identity 문맥의 server/principal/workspace가 pin과 달라지면 HTTP 전에 실패하며 `--server`로 다른 서버에 보낼 수 없다. [재시도](../../cmd/compos/forwarder_ledger.go#L247), [scope pin](../../cmd/compos/forwarder_ledger.go#L328), [intent 복원](../../cmd/compos/forwarder_submit.go#L91).

## 로컬 내장 웹 UI

`compos web`은 기본 `127.0.0.1:8787`에서 로컬 Go SSR 콘솔을 열며 서버 credential은 로컬 프로세스에서 사용한다. 기본 읽기 전용이고 `--enable-writes`일 때만 create/edit 폼을 연다. 현재 handler는 native SSR를 사용하며 저장된 HTML template 선택은 410이다. embed된 webdist 파일이 있다는 사실을 별도 업무 쓰기 API의 근거로 보지 않는다. [시작·embed](../../cmd/compos/web.go#L28), [handler](../../cmd/compos/web.go#L112).

GET `/`, `/objects/{noun}`, `/objects/{noun}/{id}`, 쓰기 활성 시 `/objects/{noun}/new`, `/objects/{noun}/{id}/edit`; POST는 `/objects/{noun}`와 `/objects/{noun}/{id}`다. 표면에 없는 noun·다른 객체형의 ID는 거부한다. host·remote loopback, origin/content-type, CSRF, native form mode와 지원 schema를 검사한다. [라우팅](../../cmd/compos/web_ssr.go#L54), [POST](../../cmd/compos/web_ssr.go#L286), [origin](../../cmd/compos/web_ssr.go#L430).

edit는 현재 전체 values를 읽고 제출 필드를 덮어쓴 뒤 기존 version을 precondition으로 보낸다. 숨겨진 값과 지원하지 않는 구조 값을 보존하지만 values 교체이므로 최신 schema 검증 대상이다. 쓰기는 동일 bbolt 제출 경로를 사용한다. receipt ID/version을 확인하고 정본 재조회까지 성공한 뒤 redirect한다. 커밋 후 확인 읽기 실패는 502이며 실패 화면이 미커밋을 의미하지 않는다. [edit·확인](../../cmd/compos/web_ssr.go#L335).

## 검색·projection 동기화

POST search body는 `q`, `filter`, `sort[]`, `limit`, `offset`, `attributesToRetrieve[]`다. limit 기본 20, 범위 1..100, offset은 음수 불가다. 사용자 filter와 별개 배열 원소로 workspace equality를 강제한다. endpoint 미설정/접속 실패는 503, upstream 거부/잘못된 JSON은 502다. [search](../../internal/server/search.go#L28).

현재 서버는 결과에서 `object_id`가 없는 문서를 제외한다. 속성 제한 요청에도 내부적으로 object_id를 추가해 판별하고, 사용자가 요청하지 않은 object_id는 응답에서 제거한다. hit에 `id`가 이미 있을 때만 canonical object ID로 바꾸며, 속성 요청에서 제외되어 없는 `id`를 새로 추가하지 않는다. 엔진 건수·facet가 과거 Knowledge를 포함할 수 있으므로 `estimatedTotalHits`, `totalHits`, `totalPages`, `facetDistribution`, `facetStats`, `semanticHitCount`를 제거하고 `returnedHits`를 준다. **후처리 필터라 페이지가 limit보다 짧거나 비어도 다음 엔진 페이지에 객체가 있을 수 있다.** 서버 필터링은 과거 문서의 실제 삭제와 다르다. [응답 정리](../../internal/server/search.go#L88).

syncer state는 `WorkspaceID`, `LastSequence`, `SnapshotReady`를 프로세스 메모리에 둔다. 시작·index 재생성·scope 변경 시 snapshot을 다시 읽는다. workspace를 고정하지 않으면 전용 discovery API로 매 cycle 확인한다. scope 변경 시 이전 workspace 문서를 지우고 state를 바꾼다. 이 동기화 알고리즘은 외부 Authority가 요청을 승인한다는 전제이며 현재 기본 서버나 서비스 토큰만으로 실행 가능하다는 주장이 아니다. snapshot은 해당 workspace 문서를 삭제한 뒤 객체만 다시 색인한다. 이 교체는 Meilisearch 전체의 원자적 전환이 아니므로 삭제와 추가 사이 빈 결과가 가능하다. [syncOnce](../../cmd/search-syncer/main.go#L140), [snapshot](../../cmd/search-syncer/main.go#L262), [삭제](../../cmd/search-syncer/main.go#L310).

원본 데이터 추출도 원자적 snapshot이 아니다. 서버는 projection sequence와 객체 목록을 별도의 `ReadLocal` 호출로 읽고, 객체별 상세도 따로 읽는다. 응답의 `canonical_linearizable` 표기만으로 해당 sequence 시점의 일관된 전체 객체 집합을 보장한다고 설명할 수 없다. 동시 변경 중 sequence와 객체 내용의 시점이 달라질 수 있다. [snapshot 생성과 객체 읽기](../../internal/server/server.go#L855).

이벤트는 성공 처리 후에만 cursor를 올린다. upstream 409면 snapshot 미준비 상태로 되돌린다. `object_instance` 외 aggregate는 색인하지 않고 cursor는 진행하므로 과거 Knowledge 이벤트가 재생을 막지 않는다. 객체 create/update/move/delete는 canonical outer event type을 기준으로 처리한다. Meilisearch task 완료 실패도 실패로 취급한다. [event loop](../../cmd/search-syncer/main.go#L321), [object 처리](../../cmd/search-syncer/main.go#L382).

### 한국어 검색 표현과 실제 검색 속성

`internal/korean`은 완성형 한글을 초성(예: `박찬엽` → `ㅂㅊㅇ`)과 호환 자모(예: `박` → `ㅂㅏㄱ`)로 변환하며 한글이 아닌 문자는 보존한다. `SearchFields`는 원문·초성·자모 표현과 공백으로 나눈 부분 표현을 중복 제거해 만든다. `MixedSearchFields`는 완성형 prefix, 초성 prefix, `박ㅊ`, `박ㅊㅇ`, `박찬ㅇ` 같은 혼합 표현을 추가한다. 공백이 있으면 각 단어를 따로 처리하고 전체 원문도 포함한다. [초성](../../internal/korean/korean.go#L101), [자모](../../internal/korean/korean.go#L117), [검색 표현](../../internal/korean/korean.go#L138), [혼합 표현](../../internal/korean/korean.go#L175).

syncer는 title이 비어 있지 않으면 `name_chosung`, `name_jamo`, `name_search`, `name_mixed`를 생성한다. 그러나 실제 `SearchableAttributes`는 **`name`, `name_search`, `name_mixed`, `title`, `type`**다. 독립 필드 `name_chosung`·`name_jamo`가 생성된다는 사실과 직접 검색 대상으로 등록된다는 주장은 다르다. 초성·자모 표현은 searchable인 `name_search` 배열에도 들어간다. [문서 생성](../../cmd/search-syncer/main.go#L460), [검색 속성 설정](../../cmd/search-syncer/main.go#L247).

패키지는 `NormalizeQuery`와 초성·혼합 판별 함수도 제공하지만 현재 CLI와 API 검색 경로는 이를 호출하지 않고 `q`를 그대로 전달한다. query를 여러 표현으로 확장해 재검색한다고 설명하지 않는다. 한국어 매칭은 생성된 검색 표현과 Meilisearch의 동작에 의존하며, 라이브 엔진의 모든 초성·자모 입력 결과를 이 소스 대조로 입증하지 않는다. [query helper](../../internal/korean/korean.go#L244), [혼합 판별](../../internal/korean/korean.go#L327), [CLI query 전달](../../cmd/compos/search.go#L37), [API query 전달](../../internal/server/search.go#L62).

## 테스트 근거·공백·소스 지도

아래는 테스트 **소스**가 담는 기대 동작이다. 이번 문서 작업에서 실행하지 않았다.

| 근거 | 확인 대상으로 삼은 계약 |
|---|---|
| [동적 surface 통합](../../cmd/compos/surface_integration_test.go#L19) | live surface로 noun 해석 |
| [receipt 검사](../../cmd/compos/receipt_validation_test.go#L13) | 성공 HTTP라도 잘못된 receipt 거부 |
| [retry scope](../../cmd/compos/ledger_test.go#L159) | 다른 server/principal/workspace 차단 |
| [bbolt 손상](../../internal/cli/ledger/bbolt_test.go#L145) | quarantine과 추가 쓰기 차단 |
| [SSR read-only](../../cmd/compos/web_ssr_test.go#L200), [필드 보존](../../cmd/compos/web_ssr_test.go#L273) | 폼 쓰기 제한과 overlay |
| [sync 실패 cursor](../../cmd/search-syncer/main_test.go#L217) | 실패한 이벤트 뒤로 cursor를 넘기지 않음 |
| [과거 이벤트](../../cmd/search-syncer/main_test.go#L335), [재시작 청소](../../cmd/search-syncer/main_test.go#L351) | Knowledge 무시·객체 snapshot 재구축 |
| [서버만 업그레이드](../../internal/server/search_test.go#L117) | 기존 Knowledge 검색 문서 제외와 결과 건수 정리 |
| [Meilisearch E2E](../../cmd/search-syncer/meilisearch_e2e_test.go#L24) | 실제 엔진 projection 테스트 경로 존재 |
| [초성·자모](../../internal/korean/korean_test.go#L9), [혼합 표현](../../internal/korean/korean_test.go#L72), [query helper](../../internal/korean/korean_test.go#L92) | 정규화 helper의 기대 출력. 현재 API가 query helper를 사용한다는 증거는 아님 |

HTTP inventory는 등록된 경로 전체를 포함하지만 각 도메인의 validation·state transition은 연결 장과 함께 읽어야 한다. 실제 IdP, 브라우저, 네트워크 응답 유실, Meilisearch task와 운영 rollout은 이 소스 대조에서 실행 검증하지 않았다. 주요 소스는 `cmd/compos`, `internal/cli/ledger`, `internal/server/{server,object_resources,search}.go`, `cmd/search-syncer/main.go`다.
