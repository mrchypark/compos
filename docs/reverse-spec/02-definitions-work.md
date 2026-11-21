# 02. 정의 revision과 Work 실행의 현재 계약

- 기준: 2026-10-31 현재 Compos 작업 트리. 승인된 Knowledge 제거가 반영된 코드를 설명한다.
- 성격: EGM 형식을 현재 구현 역명세에 맞춰 적용한 초안이다. 새 설계, 이름 변경, 구현 계획 또는 독립 Goldfish 검토 결과가 아니다.
- 증거: 운영 소스와 테스트 소스를 읽었다. 이 문서를 작성하면서 테스트·서버·외부 실행기를 실행하지 않았다. 아래의 테스트는 검증 의도를 보여 주는 소스 증거이며 이번 작업의 통과 결과가 아니다.
- 소유 범위: 이 파일만 작성한다. `design_draft.md`, 운영 코드와 다른 역명세 문서는 수정하지 않는다.

## 1. 목적과 경계

이 문서는 Task·Function·Workflow·AgentDefinition·Event·Ontology 정의의 발행, Work의 개설·배정·제출·취소, 실행 결과 처리와 조회 상태 복원을 설명한다. Task는 수행 계약이고 Work는 특정 Task revision을 참조하는 실행 stream이다. 일반 업무 객체는 별도의 동적 객체 저장소에 존재한다.

HTTP 고정 `local` attribution과 actor 기록의 공통 계약은 [00. 실행 구조](00-overview.md), ReactiveRule과 외부 이벤트 처리의 전체 계약은 [04. 자동화와 외부 연동](04-automation-integrations.md)을 함께 읽는다. 일반 객체·관계는 [01. 객체와 관계](01-objects-relations.md), 저장소·배포·복구는 [05. 저장소와 배포](05-storage-deployment.md), HTTP API는 [06. API·검색](06-api-search.md)의 범위다. 여기서는 이 실행 모델이 호출하는 경계만 설명한다. 사실 판단 근거는 아래에 연결한 현재 소스이며, 다른 문서의 더 넓은 설명을 구현 사실로 가져오지 않는다.

현재 구현을 다음처럼 일반화하면 안 된다.

- Task·Function 정의 자체를 Journal이나 `object_instances`에 저장한다고 설명하지 않는다.
- Work의 조회 상태를 복원하는 것과 업무 객체 전체를 이벤트로 복원하는 것을 동일시하지 않는다.
- Task의 Function 허용 목록, requested actor attribution, 현재 참여자 검사를 같은 통제로 취급하지 않는다.
- `constraints`, Agent의 policy 객체, timeout·retry 설정이 모두 Compos 안에서 실행되는 정책이라고 설명하지 않는다.
- 정의 발행을 포함한 모든 command가 Journal fact를 추가한다고 설명하지 않는다.

근거: [정의 저장](../../internal/kernel/work.go#L27), [실행 기록](../../internal/kernel/work.go#L1890), [객체 갱신](../../internal/kernel/executor.go#L495), [actor attribution](../../internal/kernel/actor.go#L5).

## 2. 엔터티와 상태

| 개념 | 식별과 저장 | 현재 의미 |
|---|---|---|
| Spec revision | `spec_revisions.id`, workspace, family, revision, kind | 불변 JSON 정의다. Task 전용 테이블이 아니다. |
| Task | `kind=task` revision | Work context·제출·완료 조건과 선택적인 정책·데이터 바인딩을 정한다. |
| Function | `kind=function` revision | 입력 계약과 실행 binding을 정한다. Work stream 안에서 호출한다. |
| Workflow | `kind=workflow` revision | route/invoke/branch/parallel로 구성한 고정 실행 절차다. |
| AgentDefinition | `kind=participant` revision | agent에게 배정할 때 쓰는 정의다. `agentdefinition`이라는 저장 kind는 없다. |
| Work | `work_stream` aggregate, Journal의 `stream_id` | `work.opened`에 Task revision과 context를 고정한다. 별도 mutable Work 행은 없다. |
| Invocation | Journal의 `correlation_id` | 한 Function·Agent·Workflow 요청과 결과를 연결한다. Work ID와 다르다. |
| Participant | Journal의 kind/id | `human`, `agent`, `automation`이다. agent의 ID는 participant spec revision을 참조한다. |
| 동적 객체 | `object_types`, `object_type_schema_revisions`, `object_instances`, `object_relations` | 현재 업무 데이터와 관계다. Work가 context와 binding으로 참조하고 command로 변경할 수 있다. |

근거: [정의 DDL](../../internal/store/schema.sql#L328), [Journal DDL](../../internal/store/schema.sql#L374), [Work 생성](../../internal/kernel/work.go#L918), [participant 파서](../../internal/kernel/work.go#L1800).

### 2.1 조회 상태는 저장 행이 아니라 계산 결과다

Work 상세는 `stream_sequence` 순으로 Journal을 읽어 assignments, handoffs, submissions, executions, 활성 invocation, 완료·취소·기한 정보를 계산한다. 응답의 `version`은 마지막으로 읽은 Journal sequence다. 하나의 command가 여러 fact를 추가할 수 있으므로 command 수와 Work version은 같지 않다. 이후 고정 Task revision을 읽어 `task.spec`을 응답에 넣는다.

표시 상태의 우선순위는 `cancelled` → `completed` → 활성 Function/Agent/Workflow가 있으면 `running` → `awaiting_review` → 배정되었으면 `assigned` → `open`이다. Function 실패 자체가 곧 Work의 별도 `failed` 상태가 되는 것은 아니다. 기한 도달도 별도 사실이며 그 자체로 완료·취소를 뜻하지 않는다.

병렬 route를 포함한 이력은 원본 facts에 남지만 상세 응답의 단일 `assignee`는 마지막으로 읽은 배정으로 덮어쓴다. 이를 모든 병렬 배정의 집합으로 해석하지 않는다.

현재 Journal의 실행 kind는 Work의 `opened/routed/submitted/satisfied/cancelled/deadline.reached`, Function의 `requested/succeeded/failed`, Agent의 `requested/succeeded/failed`, Workflow의 `started/completed/failed`, 그리고 `domain.event.emitted`다. 실제 저장 문자열에는 `work.`·`function.` 등의 접두사가 붙는다. 역사적 Knowledge proposal kind도 CHECK 목록에는 남아 있으나 활성 command가 이를 생성하는 기능은 제거되었다.

근거: [상세 복원](../../internal/server/server.go#L596), [상태 우선순위](../../internal/server/server.go#L747), [다음 sequence 계산](../../internal/kernel/work.go#L1789).

## 3. 정의 발행과 불변 revision

### 3.1 공통 발행 계약

`spec.publish`는 `target.aggregate_type=spec_revision`을 사용하고 기존 target ID를 요구하지 않는다. payload의 필수 필드는 `family_id`, 양의 정수 `revision`, `kind`, 객체 `spec`이다. 등록된 kind는 `task`, `function`, `workflow`, `participant`, `event`, `ontology` 여섯 개다.

발행은 kind별 검증, 참조 revision 검증, 실행 그래프 순환 검사를 거친 뒤 행을 INSERT한다. ID는 command ID에서 결정적으로 만든다. digest는 spec 본문만이 아니라 family/revision/kind/spec을 함께 정규화하여 계산한다. UPDATE·DELETE 트리거가 기존 정의 변경을 막는다. 새 revision을 발행해도 기존 Work와 invocation의 참조를 자동 교체하지 않는다.

DB는 `(workspace_id,family_id,revision)`을 유일하게 만든다. 따라서 같은 family/revision에 다른 kind를 넣을 수도 없다. 반면 현재 코드에는 revision이 반드시 직전 값+1이거나, 가장 큰 기존 값보다 커야 한다는 검사가 없다. `current_revision` 포인터나 spec 수정·삭제 command도 없다. 정렬상 큰 revision과 실행이 고정한 revision은 구분한다.

근거: [registry](../../internal/kernel/registry.go#L29), [발행](../../internal/kernel/work.go#L27), [참조 검사](../../internal/kernel/work.go#L661), [순환 검사](../../internal/kernel/work.go#L708), [유일성과 불변성](../../internal/store/schema.sql#L328), [Rhiza 발행](../../internal/kernel/rhiza_spec.go#L107).

### 3.2 공통 스키마 언어의 범위

입력·출력·Event payload 스키마는 프로젝트의 제한된 검증기를 사용한다. 허용 키워드는 `type`, `properties`, `required`, `items`, `additionalProperties`, `enum`, `minLength`, `maxLength`, `minimum`, `maximum`, `pattern`, `format`, `readOnly`, `state_machine`, `x-compos-generation`이다. `state_machine`은 field schema에만 허용된다. 임의의 JSON Schema 표준 전체나 `$ref`·조합 키워드 지원을 뜻하지 않는다.

등록 스키마 중첩은 깊이 32까지 검사한다. format은 `date`, `date-time`, `email`, `uri`, `regex:` 접두사의 정규식을 지원하며, `additionalProperties`는 boolean이다. 종류별 필수 object schema와 실제 데이터 검증은 별도다. `readOnly`나 생성 규칙의 효과는 각 객체 쓰기 경로의 검증과 함께 판단해야 한다.

근거: [지원 키워드와 등록 검증](../../internal/kernel/schema.go#L40), [command payload 검증](../../internal/kernel/registry.go#L117).

### 3.3 여섯 kind의 계약

아래 필드 목록은 kind별 허용 키다. 허용되지 않은 최상위 키는 `onlyKeys`가 거부한다.

| kind | 필수 계약 | 선택 필드와 검증 범위 |
|---|---|---|
| `task` | 비어 있지 않은 `name`, `goal`; object `input_schema`, `output_schema`; `completion` | `constraints`, `capability_policy`, `ontology_context`, `failure_policy`, `data_bindings` |
| `function` | `name`, object `input_schema`, 객체 `binding` | object `output_schema`, `emits`(Event revision ID 목록, 최대 64개) |
| `workflow` | `name`, 비어 있지 않은 `steps` 배열 | 최상위 추가 필드는 없다. step별 계약은 7절에 기술한다. |
| `participant` | `name`, `instructions`, `execution_policy` | `model_policy`, `context_policy`, `permissions`, `memory_policy`는 각각 최대 64키인 객체; `tools`는 Function revision ID 문자열 목록 최대 128개 |
| `event` | `name`, object `payload_schema` | 추가 필드는 없다. Event 정의와 발생 인스턴스는 다르다. |
| `ontology` | `name`, `concepts` 1~1,000개, `relations` 배열 최대 5,000개 | concept은 고유하고 비어 있지 않은 `id`와 선택 `description`; relation은 선언된 concept의 `source`, `target`, 비어 있지 않은 `predicate`, 선택 `cardinality` |

Ontology의 cardinality는 `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many`다. 이는 Ontology 정의의 의미 계약이며 곧바로 실제 object relation을 생성하지 않는다. Knowledge accept/propose/review 기능은 제거되었지만 일반 Ontology 발행과 Task의 Ontology 참조는 남아 있다.

근거: [kind별 검증](../../internal/kernel/work.go#L54), [AgentDefinition](../../internal/kernel/work.go#L232), [Ontology](../../internal/kernel/work.go#L611), [Knowledge 제거 후 보존 계약의 테스트 소스](../../internal/kernel/semantic_test.go#L40).

### 3.4 Task의 선택 정책과 완료 계약

- `constraints`: 최대 64개의 비어 있지 않은 문자열이다. 이 목록을 표현식으로 평가해 업무를 차단하는 실행기는 확인되지 않는다.
- `completion.kind=object_state_equals`: `state`가 필수이며 입력 스키마가 string `object_id`를 필수로 요구해야 한다. 완료 판정 시 현재 객체의 `state_key`를 읽는다.
- `completion.kind=submission_valid`: `state`를 받지 않는다. 제출 데이터는 적용되는 출력 스키마로 검증한다. 일반 제출과 Workflow route 제출의 완료 시점은 다르다.
- `capability_policy`: 키는 `allowed_function_revision_ids` 하나다. 제공하면 1~128개의 Function revision ID를 요구하고, 호출 시 포함 여부를 검사한다. 생략하면 이 허용 목록 검사는 생략하지만 참여자와 입력 검사는 남는다. 이 정책은 Task 실행 범위를 제한하며 사용자 인가 정책이 아니다.
- `ontology_context`: `ontology_revision_ids`, `concept_refs`를 받으며 각각 문자열 목록 최대 128개다. revision ID는 같은 workspace의 Ontology 정의를 참조해야 한다. concept 문자열 목록 자체를 Ontology concept ID에 대조하는 별도 검사는 참조 검사에서 수행하지 않는다.
- `failure_policy`: `on_function_failed`, `on_agent_failed`에 `action=route|cancel`을 둔다. route는 human participant를 요구한다. cancel은 participant를 받지 않는다. 선택 `reason`이 비어 있으면 실행 처리에서 기본 이유를 사용한다. 정책이 없으면 실패 사실만으로 Work를 자동 취소하거나 재배정하지 않는다.

근거: [Task 검증](../../internal/kernel/work.go#L54), [참조 검사](../../internal/kernel/work.go#L661), [Function 허용 목록](../../internal/kernel/work.go#L1179), [실패 정책 적용](../../internal/kernel/work.go#L1696), [완료 판단](../../internal/kernel/work.go#L1866).

### 3.5 참조·실행 정책

Task의 Function/Ontology 참조, participant의 tools, Function의 emits·composite Workflow 참조와 Workflow 내부 Function/agent 참조는 같은 workspace의 기대 kind revision이 존재해야 한다. Function↔Workflow 실행 참조는 순환 검사 대상이다. 모든 문자열 참조가 실세계 권한이나 실행 가능성을 보장하지는 않는다.

외부 실행 정책의 `timeout_seconds`는 선택 정수 1~86,400이다. 선택 `retry_policy`는 양의 정수 `max_attempts`와 선택 `backoff`를 받는다. backoff는 비어 있지 않은 문자열 또는 음수가 아닌 숫자다. 이 검증은 재시도 알고리즘 전체를 구현하거나 문자열의 의미를 해석하지 않는다.

credential pin은 connection ID와 양의 안전 정수 revision을 사용한다. `credential_revision`은 connection ID 없이 사용할 수 없으며 JSON 정수 리터럴 1~9,007,199,254,740,991이어야 한다. connection ID만 있고 revision이 없는 경우도 검증기는 허용한다. 이를 항상 credential이 완전히 고정된다고 설명하지 않는다.

근거: [참조와 순환 검사](../../internal/kernel/work.go#L661), [credential pin](../../internal/kernel/work.go#L346), [정수 리터럴 검사](../../internal/kernel/registry.go#L141), [timeout·retry 검증](../../internal/kernel/work.go#L808).

## 4. Command와 HTTP 계약

### 4.1 공통 envelope·고정 local attribution·멱등성

Kernel command 구조는 `command_id`, `idempotency_key`, `workspace_id`, 선택 `requested_actor`, `command_type`, `target`, `preconditions`, `payload`, `causation`이다. target은 `aggregate_type`과 선택 `aggregate_id`, precondition은 aggregate 종류·ID·expected version을 담는다. causation 구조에는 `source`, 선택 `attempt_id`, `event_id`, `automation`이 있다. 구조체에 존재한다는 사실과 모든 필드가 항상 필수라는 주장은 구분한다.

등록 command 종류, 필수 ID, target 종류·ID 요구, payload schema를 검사한다. Work command에는 registry가 요구하는 Work version precondition이 없다. 일반 precondition 검사도 object type/instance/relation, relation definition, reactive rule을 대상으로 하므로 `work_stream` precondition을 임의로 추가하면 지원되지 않는다. Work version은 Journal 순서·검사로 관리한다.

HTTP 서버는 인증 gate 없이 요청을 route로 전달하고 모든 요청을 고정 principal `local`로 기록한다. caller identity를 받지 않으며 workspace·command별 허용 정책은 없다. Kernel의 `requested_actor`는 비어 있지 않은 `type`과 `id`를 effective actor로 기록하며, actor type 제한이나 principal 일치 검사를 적용하지 않는다. ReactiveRule·Workflow도 같은 `Execute` 경로를 사용한다. Task binding·Work 배정이나 저장된 principal은 workspace 인가를 발급하지 않는다. [HTTP 처리](../../internal/server/server.go#L54), [actor 해석](../../internal/kernel/actor.go#L5), [실행 입구](../../internal/kernel/executor.go#L59)

멱등성 범위는 workspace/principal/idempotency key다. actor attribution을 확인한 뒤 중복을 조회하며 명령별 권한 승인은 없다. 기존 key가 있으면 command ID·종류·payload·target·preconditions·causation·requested actor를 비교한다. 같으면 저장 영수증을 `duplicate`로 반환하고 다르면 `idempotency_conflict`다. command ID만 또는 payload만 같으면 충분하다고 설명하지 않는다. command ledger는 적용된 command의 영수증을 저장하므로 모든 실패 시도를 저장하는 전면 감사 로그도 아니다.

근거: [타입](../../internal/kernel/types.go#L8), [등록 검증](../../internal/kernel/registry.go#L117), [실행·중복 경계](../../internal/kernel/executor.go#L59), [actor 검사](../../internal/kernel/actor.go#L5), [preconditions](../../internal/kernel/executor.go#L129), [Rhiza 중복 처리](../../internal/kernel/rhiza.go#L554).

### 4.2 공개 command

아래 표에서 `?`는 선택 필드다. `spec.publish`·`work.open`·`event.emit`은 target ID가 없고 나머지는 기존 Work stream ID를 target으로 받는다. 모든 payload는 등록된 필드만 받는다.

| command / target 종류 | payload 계약 | 효과 |
|---|---|---|
| `spec.publish` / `spec_revision` | `family_id`, `revision`, `kind`, `spec` | 정의 행을 생성한다. Journal fact는 생성하지 않는다. |
| `work.open` / `work_stream` | `task_revision_id`, object `context`, `deadline_at?` | `work.opened`를 추가한다. |
| `work.route` / `work_stream` | object `participant`, `reason?`, `workflow_step_id?`, `workflow_route_id?` | `work.routed`; agent 배정이면 `agent.requested`와 외부 발신도 만든다. |
| `work.submit` / `work_stream` | object `submission`, array `changes?`, `workflow_step_id?`, `workflow_route_id?` | `work.submitted`; 조건에 따라 `work.satisfied`를 추가한다. |
| `work.cancel` / `work_stream` | `reason` | 비어 있지 않은 이유로 `work.cancelled`를 추가한다. |
| `function.invoke` / `work_stream` | `function_revision_id`, object `input`, `workflow_route_id?` | binding에 따라 객체 변경, 외부 요청 또는 Workflow 시작을 수행한다. |
| `event.emit` / `domain_event` | `event_revision_id`, object `data`, object `subject?` | payload schema 검증 후 `domain.event.emitted`를 기록한다. |

work.route의 participant는 정확히 `kind`, `id`를 받는다. kind는 `human|agent|automation`, ID는 비어 있지 않은 문자열이다. agent 배정에서는 participant spec을 실제로 읽는다. human/automation ID가 단지 문자열 검증을 통과했다는 사실을 해당 참여자의 권한 부여로 해석하면 안 된다.

근거: [command registry](../../internal/kernel/registry.go#L29), [participant](../../internal/kernel/work.go#L1800), [Domain Event 처리](../../internal/kernel/semantic.go#L10).

### 4.3 내부 결과·기한 command와 공개 경계

내부용 command는 공개 command registry 응답에서 제외된다. 그러나 registry에서 숨기는 것과 공개 HTTP endpoint가 거부하는 것은 별도 동작이다.

| 내부 command | 필수 payload | 경계 |
|---|---|---|
| `function.record_result`, `agent.record_result` | `invocation_id`, `callback_id`, `runtime_status`, object `output`, `adapter_inbound_event_id`, `execution_id`, `provider_key`, `provider_run_ref`, `handler_version`, `attempt_count` | status는 `succeeded|failed|cancelled`; 선택 `error_class`, `error_detail`. public command HTTP는 403, Kernel은 `adapter_reducer` causation을 요구한다. |
| `workflow.complete` | `invocation_id`, `runtime_status`, object `output` | status는 `succeeded|failed`, 선택 `error_detail`. public HTTP는 403, Kernel은 `workflow` causation을 요구한다. |
| `work.record_deadline` | `deadline_at` | registry에서 숨긴다. Kernel은 `scheduler` causation, 열린 Work, 개설 당시 deadline과의 일치, 미기록 상태를 검사한다. |

공개 `submitCommand`가 이름으로 차단하는 것은 앞의 결과 command 세 개다. `work.record_deadline`은 이 차단 목록에 포함되지 않는다. 따라서 모든 내부용 command가 동일한 HTTP 차단을 받는다고 기술할 수 없다. causation 문자열 검사만으로 호출 출처의 신뢰성이 전부 증명된다고 주장하지 않는다.

근거: [등록 필드](../../internal/kernel/registry.go#L35), [공개 registry 제외](../../internal/kernel/registry.go#L81), [HTTP 차단](../../internal/server/server.go#L364), [Kernel 결과 출처 검사](../../internal/kernel/executor.go#L59), [기한 fact](../../internal/kernel/work.go#L956).

### 4.4 HTTP 읽기와 실행 응답

아래 workspace 경로의 접두사는 `/api/v1/workspaces/{workspace_id}`다. HTTP 요청은 인증 없이 들어오며 고정 principal `local`을 사용한다. workspace permission 판정은 없다.

| 경로 | 현재 응답·제한 |
|---|---|
| `POST /api/v1/kernel/commands` | JSON envelope 최대 8 MiB. 성공 응답은 `command_id`, `outcome`, `aggregate`, `projection:{workspace_id,projection_sequence}`, 선택 `client_refs`다. |
| `GET /api/v1/kernel/command-registry` | 공개 Definition 목록을 반환한다. 내부 결과 command와 기한 command는 숨긴다. |
| `GET …/definitions` | `kind=task|function|workflow`, 선택 `family_id`, `limit` 기본 50/범위 1~100, `offset` 기본 0. family 오름차순·revision 내림차순·id 순으로 정렬하며 최신 revision만 고르는 API가 아니다. `page.next_cursor`는 다음 offset 또는 null이다. |
| `GET …/definitions/{revision_id}` | 같은 세 kind만 조회한다. Event/Ontology/participant를 저장할 수 있다는 사실이 이 endpoint에서 조회 가능함을 뜻하지 않는다. 미존재는 404다. |
| `GET …/work` | 모든 human 참여자에게 배정된 미완료 Work를 반환하며 실제 참여자 ID를 표시한다. 최신 배정·미제출 조건은 `workflow_route_id`별로 검사한다. terminal Work는 제외한다. `limit` 기본 50/범위 1~100, `offset` 기본 0이며 전체 Work 목록이 아니다. |
| `GET …/work/{stream_id}` | Journal과 Task spec으로 상세를 계산한다. facts가 없으면 404다. task, context, version, status, owner, assignee, assignments, handoffs, submissions, executions, 활성 ID 목록, completion/cancellation/deadline, facts를 반환한다. |
| `GET …/events` | `domain.event.emitted`를 created_at/id 순으로 최대 1,000개 조회한다. 범용 Journal 조회 API는 아니다. |
| `POST /internal/v1/conductor/callbacks` | 외부 결과를 수신하고 callback evidence와 내부 결과 command로 환원한다. 일반 사용자 command endpoint와 다르다. |

Definition 조회는 `ReadLocal`과 `canonical_stale_bounded` consistency를 사용한다. Work 읽기도 `ReadLocal`을 사용한다. 이를 모든 요청이 quorum의 최신 상태를 읽는다는 보장으로 확대하지 않는다. Work 상세의 Journal 조회와 spec 조회는 별도 읽기이며 하나의 읽기 snapshot으로 묶는 트랜잭션은 이 handler에 없다.

명령 오류의 HTTP 매핑은 `validation_failed` 등 기본 400, `forbidden` 403, `idempotency_conflict`·`precondition_failed` 409, `schema_mismatch` 422, `unsupported_storage` 501, `quorum_unavailable` 503이다. 응답의 retryability는 CommandError 값에서 온다. 객체 변경 오류는 선택적인 change index/client ref/binding/field 정보를 포함한다.

근거: [라우트](../../internal/server/server.go#L79), [실행 응답](../../internal/server/server.go#L364), [Definition 조회](../../internal/server/definition_reads.go#L13), [Work 목록](../../internal/server/server.go#L518), [상세](../../internal/server/server.go#L596), [Event 목록](../../internal/server/semantic.go#L10), [오류 매핑](../../internal/server/server.go#L1071), [오류 구조](../../internal/kernel/types.go#L69).

## 5. Work 개설·배정·제출·완료

### 5.1 개설과 context

`work.open`은 같은 workspace의 Task revision을 읽고 context를 Task input schema로 검증한다. data binding이 있으면 context root, object type·schema pin, 관계 경로의 호환성과 실제 root 객체를 검사한다. string `context.object_id`가 있으면 해당 workspace의 비보관 객체가 존재해야 한다. 비어 있지 않은 deadline은 RFC3339로 읽어 UTC로 저장한다. 빈 문자열이나 공백 deadline은 개설 처리에서 저장하지 않는다.

개설 fact는 Task revision과 context, owner principal, 선택 deadline을 고정한다. 개설만으로 모든 완료 조건을 평가해 즉시 `work.satisfied`를 만드는 것은 아니다. Work ID는 command에서 결정적으로 생성한다. context가 object ID를 참조하더라도 객체 payload 전체가 context에 자동 복사되는 것은 아니다.

근거: [개설](../../internal/kernel/work.go#L918), [binding context 검사](../../internal/kernel/work.go#L881), [Rhiza Work 경로](../../internal/kernel/rhiza_work.go#L1).

### 5.2 배정과 실행 주체

배정은 열린 Work에서 `work.routed`를 추가한다. 명시적인 Workflow step route는 `causation.source=workflow`, 유효한 step ID, `workflow_route_id=그 route command ID`를 요구한다. route ID만 주고 step ID를 생략할 수 없다.

command actor는 `user|service|automation`이고 Journal participant는 `human|agent|automation`이다. 기본/user actor는 human principal, automation actor는 automation ID, service actor는 agent ID로 변환한다. `requested_actor.type=agent`를 직접 받는 계약이 아니다.

제출·Function 호출은 해당 route의 현재 참여자와 일치해야 한다. 이는 Work 도메인 precondition이며 HTTP/workspace 권한 정책이 아니다. `work.route`·`work.cancel`에는 이와 동일한 현재 참여자 검사가 직접 적용되지 않는다. “모든 Work command는 담당자만 실행한다”는 일반화는 부정확하다.

agent 배정은 `kind=participant` spec과 Task spec을 읽고 `agent.requested`를 함께 기록한다. 정의·Task·입력·handler 및 선택 credential·실행 정책을 외부 요청 payload에 넣는다. 이 배정이 agent tools를 Compos 안에서 자동 실행하거나 LLM을 직접 구동하는 것은 아니다.

근거: [배정](../../internal/kernel/work.go#L991), [actor 변환](../../internal/kernel/work.go#L1816), [현재 참여자](../../internal/kernel/work.go#L1826), [취소](../../internal/kernel/work.go#L1124).

### 5.3 제출과 완료 조건

일반 `work.submit`은 열린 Work·현재 참여자를 확인하고 submission을 출력 스키마로 검증한다. Workflow route 제출은 step ID와 route ID를 함께 요구한다. route의 고정 Workflow spec에 `output_schema`가 있으면 그것을 사용하고, 없으면 Task output schema를 사용한다. route command payload에서 임의로 스키마를 교체하지 않는다.

`submission_valid`의 일반 제출은 `work.submitted`와 `work.satisfied`를 함께 기록한다. Workflow route 제출은 해당 step을 진행시키는 증거이며 즉시 Work 전체를 완료하지 않는다. `object_state_equals`의 일반 제출도 그 경로 자체에서 객체 상태를 검사해 완료시키지 않는다. 객체 상태 기반 완료 판단은 Function 또는 Workflow 성공 처리 등 실제 `taskSatisfied` 호출 위치에 따른다.

`taskSatisfied`는 object-state 정책이면 현재 객체 상태를 읽고, submission 정책이면 기존 `work.submitted` 존재 여부를 읽는다. 현재 표시 상태의 복원과 새로운 완료 판단에 필요한 저장소는 다르다. 임의의 일반 object command로 상태가 바뀌었다는 이유만으로 모든 관련 Work를 자동 완료하는 경로는 이 처리에서 제공하지 않는다.

근거: [제출](../../internal/kernel/work.go#L1067), [route 스키마](../../internal/kernel/work.go#L1738), [완료 판정](../../internal/kernel/work.go#L1866), [Function 완료 반영](../../internal/kernel/work.go#L1278).

### 5.4 제출과 객체 변경의 원자성

`changes`를 생략하면 일반 제출이다. 제공하면 비어 있지 않은 최대 32개 변경이어야 한다. 각 변경은 `binding_id`, `command_type`, 객체 `payload`를 받으며 선택 `client_ref`, `target`, `preconditions`를 포함한다. 허용 command는 binding이 명시한 `object.create`, `object.update`, `relation.link`, `relation.unlink`다. `object.move`나 임의 command 목록을 제출에 넣을 수 없다.

Task data binding은 최대 32개이며 다음 계약을 갖는다.

- 고유한 identifier `id`, `object_type_id`, 양의 정수 `schema_version`, `context_key`가 필요하다. context key는 Task input schema에서 필수 string이어야 한다.
- `writable_fields`, `allowed_operations`는 각각 최대 128개 문자열 목록이다. `allow_title`은 선택 boolean이다.
- 선택 `relation_path`는 최대 4홉이며 각 홉은 `relation_definition_id`, 양의 `version`, `direction=incoming|outgoing`을 받는다.
- 발행·실행 시 schema revision과 활성 type/relation의 pin을 검증한다. 쓰기 가능한 필드가 pinned schema에 존재하고 readOnly·생성 필드가 아니어야 한다.
- binding은 데이터 범위를 제한한다. 별도의 사용자·actor 권한을 발급하지 않는다. Workflow route가 `data_binding_ids`를 명시하면 그 부분집합으로 더 제한하고, 생략하면 Task binding을 상속한다.

변경은 부모 command와 같은 principal·actor·causation을 사용하되 자식 command의 등록 schema·권한·precondition도 확인한다. 같은 기존 aggregate를 한 제출에서 두 번 변경하면 거부한다. 모든 변경과 submission 검증·Work fact·부모 영수증·효과 이벤트는 한 트랜잭션으로 커밋하며 실패하면 함께 롤백한다. 자식 변경마다 독립 command ledger 영수증을 만드는 구조가 아니다.

`object.create`에는 선택적인 고유 `client_ref`를 부여할 수 있다. 관계와 제출 등의 singleton `{"client_ref":"이름"}` 참조를 결정적 서버 ID로 해석한다. 새 레코드 의존 관계를 정렬하며 알 수 없는 참조·중복·순환을 거부한다. 영수증의 `client_refs`는 ID, version, 선택 generated values를 보관한다. 생성 번호·관계·필드 검증은 일반 객체 쓰기 규칙을 우회하지 않는다.

객체의 값 교체는 전체 payload 교체이며 merge가 아니다. 보호된 필드를 지우거나 binding 밖의 필드를 바꿀 수 없다. 일반 객체는 현재 스키마로 값을 검증하고 값 교체 시 최신 revision으로 다시 연결될 수 있다. Task binding의 고정 schema 계약과 일반 object update의 최신 schema 동작을 동일시하지 않는다.

근거: [binding 검증](../../internal/kernel/work_bindings.go#L15), [참조·보호 필드](../../internal/kernel/work_bindings.go#L94), [변경 파서](../../internal/kernel/work_changes.go#L33), [SQLite 원자 제출](../../internal/kernel/work_changes.go#L109), [Rhiza 원자 제출](../../internal/kernel/work_changes.go#L223), [route binding 제한](../../internal/kernel/work_changes.go#L486), [새 ID 참조](../../internal/kernel/work_references.go#L35), [보호 필드 테스트 소스](../../internal/kernel/work_bindings_test.go#L39).

## 6. Function·Agent의 실행과 결과

### 6.1 Function binding별 계약

Function 입력은 고정 input schema로 검증하며, 입력에 object ID가 있으면 Work context의 object ID와 일치해야 한다. 호출 시 Task가 정한 Function 허용 목록, 현재 참여자와 Work의 비종료 상태를 검사한다. 선택 `workflow_route_id`는 Workflow causation에서만 허용한다.

| binding | 허용 필드·행동 |
|---|---|
| `kernel_command` | `kind`, `command_type`, 선택 `to_state`, `force`. input schema는 `object_id:string`, `expected_version:integer`를 필수로 요구한다. command는 `object.move` 또는 `object.update`만 허용한다. move에는 to_state가 필수다. update는 to_state/force를 받지 않고 input schema에 title 또는 values가 있어야 한다. |
| `connector` | `kind`, 필수 `handler_version`, `provider`, `operation`; 선택 `connection_id`, `credential_revision`, `timeout_seconds`, `retry_policy`. 외부 요청을 만든다. |
| `native` | `kind`, 필수 `handler_key`, `handler_version`; 선택 비어 있지 않은 `operation`, `timeout_seconds`, `retry_policy`. 이름이 native여도 이 경로는 외부 실행 요청이다. |
| `composite` | `kind`, `workflow_revision_id`만 받는다. 같은 workspace의 Workflow spec을 고정하고 `function.requested`와 `workflow.started`를 기록한다. |

Kernel binding은 object expected version을 확인하고 실제 변경, Function 요청·성공 fact, output schema 검증, emits 검증과 선택 Work 완료를 같은 트랜잭션에 묶는다. 검증 실패로 롤백하면 반드시 `function.failed`가 따로 남는 것은 아니다. 성공한 Function이 `emits`에 지정한 각 Event revision의 payload schema는 Function output 전체에 적용한다. Event 발생 fact도 같은 실행 흐름에 추가한다.

connector/native는 outbox에 입력, 고정 Function 정의에서 선택한 handler 정보와 `_compos` 실행 메타데이터를 싣는다. Function 정의 JSON 전체를 외부 요청에 자동 포함하는 계약은 아니다. 실제 호출의 수행·timeout·retry를 이 저장 행만으로 검증했다고 주장하지 않는다.

근거: [binding 검증](../../internal/kernel/work.go#L264), [Function 실행](../../internal/kernel/work.go#L1144), [외부 요청](../../internal/kernel/work.go#L1399), [Event facts](../../internal/kernel/semantic.go#L30), [Rhiza 실행](../../internal/kernel/rhiza_runtime.go#L1).

### 6.2 외부 결과 환원과 멱등성

Conductor callback은 workspace와 execution request를 기존 요청 fact에 연결하고 adapter evidence를 저장한다. 필수 필드는 `callback_id`, `workspace_id`, `execution_request_id`, `execution_id`, `provider_key`, `provider_run_ref`, `handler_version`, `runtime_status`, 양의 `attempt_count`, RFC3339 `occurred_at`이다. `result`, `error_class`, `error_detail`, `task_id`, `workflow_instance_id`, `correlation_id`, `causation_id`는 구조체에 있는 추가 필드다. callback token 미설정은 503, bearer 불일치는 401이다. handler identity와 원래 요청의 연결을 검증하며 agent 결과와 Function 결과를 구분한다. [수신 형식](../../internal/server/conductor.go#L38)

callback reducer는 결정적인 내부 command ID와 `conductor:<callback_id>` 멱등키, `adapter_reducer` causation을 사용한다. callback HTTP의 허용 상태는 `succeeded|failed|cancelled`뿐이다. queued/running/retry_wait/timed_out 상태를 이 callback에 직접 보낼 수 없다. 외부 전송 ACK에서 허용하는 실행 상태 집합과도 다르다. `failed`·`cancelled`는 `result` 생략 또는 null을 허용하며 빈 output 객체와 기존 오류 정보를 유지한다. `succeeded`에서 생략·null이거나, 상태와 무관하게 null이 아닌 result를 객체로 해석할 수 없으면 `failed`와 `output_contract_mismatch`로 환원한다. [result 처리 분기](../../internal/server/conductor.go#L218)

Kernel은 열린 Work, 미종료 invocation, 고정 handler, `adapter_inbound_event_id=callback_id`와 아직 appended인 evidence를 확인한다. 성공 output의 스키마가 맞지 않으면 실패 상태와 `output_schema_mismatch`로 정규화한다. Function emits의 payload 검증 실패는 `event_schema_mismatch`로 환원한다. evidence의 `reduced` 전환과 Journal 결과 기록은 내부 command의 커밋에 묶인다. 새 수신 성공 응답은 202, 기존 callback의 동일 재수신은 200이며, 다른 내용으로 callback ID를 재사용하면 409다.

이미 처리된 callback의 동일 입력·동등한 terminal 결과는 수신/수렴 경로에서 중복으로 처리할 수 있지만, 다른 내용의 재사용은 충돌이다. Kernel 결과 command만 보면 이미 terminal인 invocation의 새 결과는 거부한다. 전송 ACK, callback 중복 처리, command 멱등성, invocation terminal 검사는 서로 다른 경계다.

근거: [수신](../../internal/server/conductor.go#L61), [환원·상태 정규화](../../internal/server/conductor.go#L200), [동등 결과 수렴](../../internal/server/conductor.go#L264), [Function 결과](../../internal/kernel/work.go#L1460), [Agent 결과](../../internal/kernel/work.go#L1586).

### 6.3 성공·실패가 Work에 미치는 영향

일반 외부 Function의 성공은 `function.succeeded`와 선택 Event facts를 남기고, 원래 요청이 Workflow 내부가 아니면 Task 완료 조건을 판단한다. Workflow 내부 자식의 성공은 부모 Workflow 진행 근거이며 자식 성공만으로 Work 전체를 완료시키지 않는다.

Agent 성공은 고정 Task 또는 route output schema를 검증한 뒤 `agent.succeeded`와 `work.submitted`를 남긴다. 일반 submission-valid Work라면 `work.satisfied`도 추가한다. route 제출이면 부모 진행을 기다린다. Agent의 `tools`·model/context/memory/permissions policy를 저장한다는 사실만으로 Compos가 그 외부 실행기의 모든 정책을 직접 강제한다고 설명하지 않는다.

최종 Function/Agent 실패는 각각 `function.failed`, `agent.failed`를 기록하고 Task의 해당 failure policy가 있으면 human 재배정 또는 Work 취소 fact를 추가한다. policy가 없으면 Work는 미완료 상태로 남을 수 있다. 이미 완료·취소된 Work를 늦은 Workflow 완료나 결과 command가 다시 진행시키는 것으로 설명하지 않는다. 실제 terminal 검사와 callback 수렴 처리를 함께 읽어야 한다.

근거: [Function 결과와 실패 정책](../../internal/kernel/work.go#L1460), [Agent 제출](../../internal/kernel/work.go#L1586), [실패 정책](../../internal/kernel/work.go#L1696), [terminal 경계 테스트 소스](../../internal/kernel/workflow_terminal_test.go#L14).

## 7. Workflow의 고정 계약과 replay

### 7.1 step schema

Workflow는 name과 steps를 가지며 전체 step 수는 최대 128, 중첩 검사는 depth 8까지다. 모든 step ID는 전체 트리에서 고유한 1~64자 ASCII 영문·숫자·`_`·`-` identifier다. branch ID는 해당 parallel 내부에서 고유하다.

| step kind | 필수·선택 계약 |
|---|---|
| `route` | 필수 `id`, `kind`, `participant`; 선택 `reason`, object `output_schema`, `output_mapping`, `data_binding_ids`(고유 ID 1~32개). |
| `invoke` | 필수 `id`, `kind`, `function_revision_id`; 선택 `input_mapping`, `output_mapping`. |
| `branch` | 필수 `id`, `kind`, `condition`, 비어 있지 않은 `then`, 배열 `else`(비어 있어도 됨); 선택 `output_mapping`. |
| `parallel` | 필수 `id`, `kind`, `branches` 2~16개; 각 branch는 `id`, 비어 있지 않은 `steps`; 선택 `output_mapping`. |

mapping을 제공하면 1~128키 객체여야 한다. 값이 `$`로 시작하면 `$.` 선택자여야 하고 그 외 값은 literal이다. 실행 시 `root`, `current`, `steps`, mapping의 `output`을 사용한다. 선택자는 객체 키를 점으로 따라가며 배열 인덱스나 임의 표현식을 평가하지 않는다. 없는 mapping 선택자는 실행 오류다. branch condition은 `left`, `operator`, 선택 `right`만 받으며 연산자는 `eq`, `not_eq`, `exists`, `not_exists`다. eq/not_eq에만 right가 필요하다.

근거: [Workflow 검증](../../internal/kernel/work.go#L366), [mapping](../../internal/kernel/work.go#L505), [condition](../../internal/kernel/work.go#L582), [실행 선택자](../../internal/server/workflow.go#L314).

### 7.2 replay의 실제 의미

composite invocation은 Work 안에 Function 요청과 고정 Workflow 시작을 남긴다. 진행기는 미종료 Workflow를 읽고, 고정 spec과 이미 기록한 route 제출·Function terminal 결과로 메모리 상태를 다시 계산한다. step별 command ID는 invocation/step/action으로 결정적으로 만들며 동일 command의 ledger 중복 처리가 반복 실행을 수렴시킨다.

- route는 배정을 기록하고 human/agent이면 해당 invocation·step·route의 제출을 기다린다. 제출자의 principal을 다음 실행에 이어받는 경로가 있다. automation route를 human 제출 대기와 동일하게 설명하지 않는다.
- invoke는 terminal 결과가 있으면 그것을 소비하고, 없으면 결정적인 child invocation을 호출하여 결과를 기다린다. 자식 Function 실패는 부모 실패 처리로 이어진다.
- branch는 복원된 상태에서 한쪽 step 목록을 고른다. 선택 결정을 별도의 범용 mutable node 상태 행으로 보관하지 않는다.
- parallel은 분기별 메모리 상태를 만들고 모든 분기가 준비되면 결과를 branch ID별 객체로 합친다. 이는 영속된 비동기 실행의 fan-out/join이며 Go goroutine으로 모든 분기를 동시에 계산한다는 뜻은 아니다.

완료 시 내부 `workflow.complete`가 고정 Function output·emits와 Work 완료 조건을 처리한다. terminal Work 또는 이미 끝난 Workflow invocation에는 새 완료를 추가하지 않는다. route 제출의 스키마·binding 범위는 임의 route payload가 아니라 고정 spec과 결정적인 route identity에서 구한다. 재호출한 Workflow의 동일 step과 병렬 route는 invocation/route ID로 구분한다.

여기서 replay는 읽기 상태 복원과 다음 command 결정을 뜻한다. 외부 부수 효과의 전역 exactly-once 실행, 임의 과거 시점 DB 복원, 모든 객체 상태의 Journal 재생을 보장하지 않는다. 스케줄러의 주기·기한·자동화 실행 운영 계약은 별도 자동화 문서의 범위다.

근거: [진행기](../../internal/server/workflow.go#L121), [step 실행](../../internal/server/workflow.go#L174), [command ID](../../internal/server/workflow.go#L453), [route 고정 계약](../../internal/kernel/work.go#L534), [완료 환원](../../internal/kernel/work.go#L1294).

## 8. 저장·트랜잭션·실패 경계

| 저장소 | 이 실행 모델에서의 역할 |
|---|---|
| `spec_revisions` | 여섯 kind의 불변 정의와 고정 참조 대상이다. |
| `journal_records` | stream 순서·participant·correlation·causation·command·spec 참조를 가진 불변 실행 사실이다. |
| `command_ledger` | 적용한 command의 입력·유효 actor·결과 영수증과 중복 판정 근거다. |
| `activities` | aggregate에 적용한 효과의 조회 기록이다. Work 실행 복원의 정본을 대신하지 않는다. |
| `outbox_events` | 외부 실행 요청 전달과 발행 상태를 보관한다. |
| `projection_events` | workspace별 sequence로 조회 모델에 변경을 전달한다. 완성된 검색 인덱스 자체가 아니다. |
| 일반 object/relation/필드 보조 테이블 | 실행이 참조하거나 원자 제출로 바꾸는 현재 업무 데이터다. |
| `adapter_inbound_events` | 외부 결과 command를 정당화하는 수신 evidence와 환원 상태다. |

SQLite는 트랜잭션 안에서 상태·ledger·효과·Journal을 묶는다. Rhiza는 선행 읽기의 commit-time guard와 SQL statement batch로 묶고 커밋 후 ledger receipt를 읽는다. 적용 전 실패와 이미 적용되었으나 응답이 불명확한 경우를 동일하게 취급하지 않는다. Rhiza 경로는 중복 receipt 재조회로 수렴을 시도하며, DB mutation precondition 실패를 `precondition_failed`, 실행 제약 실패를 `validation_failed`로 변환한다.

현재 Rhiza commit 구현은 한 원자 command 계획이 64 statements를 넘으면 `validation_failed`로 거부한다. 대형 제출을 여러 커밋으로 나누어 원자성을 포기하지 않는다. 32 changes 허용은 모든 조합이 64 statements 안에 들어간다는 보장이 아니다. 경쟁 상태·sequence·생성 번호·참조 계획에는 별도의 guard 코드가 있으나 이 문서의 소스 열람을 운영 환경의 동시성 검증으로 대체하지 않는다.

근거: [SQLite 실행](../../internal/kernel/executor.go#L59), [ledger·효과](../../internal/kernel/executor.go#L993), [Rhiza 커밋](../../internal/kernel/rhiza.go#L499), [Rhiza Journal](../../internal/kernel/rhiza_semantic.go#L46), [guard 계획](../../internal/kernel/work_guard_plan.go#L1), [sequence 예약](../../internal/kernel/work_sequences.go#L1).

## 9. 테스트 소스 증거와 확인하지 않은 범위

다음 항목은 테스트 파일의 시나리오를 읽은 결과다. 이 문서 작성 중 실행한 결과는 없다.

| 확인 의도 | 테스트 소스 |
|---|---|
| 정의 목록 필터·불변 상세, TaskRun 조회·제출 완료 | [definition reads](../../internal/server/definition_reads_test.go#L15), [TaskRun](../../internal/server/task_run_test.go#L13) |
| 일반 업무 객체와 Task/Function의 실행 연결 | [구매 승인](../../internal/kernel/work_test.go#L15), [고정 객체 update](../../internal/kernel/work_test.go#L161) |
| 원자 제출과 route binding 제한 | [원자 제출](../../internal/kernel/work_changes_test.go#L13), [route 범위](../../internal/kernel/work_changes_test.go#L162) |
| 새 객체·client ref·관계·동시 제출 | [새 레코드 SQLite/Rhiza](../../internal/kernel/work_new_records_test.go#L15), [관계 참조](../../internal/kernel/work_link_refs_test.go#L12), [순환·잘못된 참조](../../internal/kernel/work_references_test.go#L35) |
| context root·보호 필드·생성 sequence | [context](../../internal/kernel/work_context_test.go#L13), [보호 필드](../../internal/kernel/work_bindings_test.go#L39), [sequence](../../internal/kernel/work_sequences_test.go#L38) |
| Workflow replay·분기·병렬 join·반복 invocation | [replay](../../internal/server/workflow_test.go#L16), [병렬](../../internal/server/workflow_test.go#L86), [route 격리](../../internal/server/workflow_test.go#L389) |
| route 고정 스키마·현재 제출자·실행 순환 거부 | [고정 route 계약](../../internal/server/workflow_test.go#L210), [순환](../../internal/server/workflow_test.go#L467) |
| Agent 배정·결과 제출, 외부 callback 인증·중복 | [Agent](../../internal/server/agent_test.go#L12), [callback](../../internal/server/conductor_test.go#L343), [정규화 결과 수렴](../../internal/server/conductor_test.go#L578) |
| credential pin·terminal Work·statement 한도 | [credential](../../internal/kernel/credential_binding_test.go#L13), [terminal](../../internal/kernel/workflow_terminal_test.go#L14), [batch 한도](../../internal/kernel/rhiza_batch_limit_test.go#L13) |
| Event 입력 검증·일반 Ontology 보존 | [Event](../../internal/kernel/semantic_test.go#L12), [Knowledge 제거 경계](../../internal/kernel/semantic_test.go#L40) |

알려진 제한과 검증 공백은 다음과 같다.

1. Definition API는 저장 가능한 여섯 kind 중 세 kind만 노출한다. 전체 spec 조회 계약이 아니다.
2. Work 상세는 Journal과 spec을 별도로 읽고 단일 assignee를 계산한다. 모든 병렬 route를 완전한 현재 배정 집합으로 표시한다고 주장하지 않는다.
3. Task revision은 불변이지만 참조한 일반 객체의 현재 상태·활성 여부와 객체 타입·관계의 현재 pin 조건은 바뀔 수 있다. 과거 spec을 읽을 수 있다는 사실과 현재 실행 가능성은 다르다.
4. constraints와 Agent policy 객체의 의미 실행, 외부 timeout/retry·credential 보관·실제 도구 실행은 저장·형식 검증만으로 보장되지 않는다.
5. 공개 registry에서 숨긴 command와 HTTP에서 명시적으로 차단한 command 집합은 다르다. `work.record_deadline`과 causation 신뢰 경계를 완전한 내부 인증으로 일반화하지 않는다.
6. spec revision의 단조 증가·연속 번호는 현재 보장하지 않는다. 새 revision 발행이 기존 실행을 최신 정의로 재연결하지도 않는다.
7. Journal replay로 외부 부수 효과 전체를 복구하거나 전역 exactly-once를 보장한다는 실행 증거는 이 문서에 없다. 실제 Conductor, 분산 Rhiza 장애, 운영 재시작·장시간 경쟁·성능 시험도 수행하지 않았다.
8. Knowledge 전용 command·조회·graph worker는 현재 범위에 없다. 공유 Journal의 역사적 kind가 남아 있다는 사실을 활성 Knowledge 기능으로 해석하지 않는다.

## 10. 소스 지도

| 계약 | 기준 소스 |
|---|---|
| 공통 command·영수증·오류 | [types.go](../../internal/kernel/types.go#L8), [registry.go](../../internal/kernel/registry.go#L29) |
| 정의·참조·Work·Function·Agent·완료 | [work.go](../../internal/kernel/work.go#L27), [rhiza_spec.go](../../internal/kernel/rhiza_spec.go#L11), [rhiza_work.go](../../internal/kernel/rhiza_work.go#L1), [rhiza_runtime.go](../../internal/kernel/rhiza_runtime.go#L1) |
| 제한된 스키마 언어 | [schema.go](../../internal/kernel/schema.go#L40) |
| binding·원자 제출·새 참조·오류 주소 | [work_bindings.go](../../internal/kernel/work_bindings.go#L15), [work_changes.go](../../internal/kernel/work_changes.go#L33), [work_references.go](../../internal/kernel/work_references.go#L35), [work_change_errors.go](../../internal/kernel/work_change_errors.go#L11) |
| replay 진행·외부 결과 환원 | [workflow.go](../../internal/server/workflow.go#L121), [conductor.go](../../internal/server/conductor.go#L61) |
| 공개 route·조회·HTTP 오류 | [server.go](../../internal/server/server.go#L79), [definition_reads.go](../../internal/server/definition_reads.go#L13), [semantic.go](../../internal/server/semantic.go#L10) |
| 영속성·불변성 | [spec DDL](../../internal/store/schema.sql#L328), [command/Journal DDL](../../internal/store/schema.sql#L351), [트랜잭션](../../internal/store/tx.go#L29) |

이 문서의 source/test 링크는 작성 시점의 줄 번호다. 이후 다른 작업자의 코드 변경으로 줄이 이동하거나 계약이 바뀌면 다시 대조해야 한다. 이번 산출물은 현재 동작의 역명세이며 변경 승인·독립 검토·실행 검증을 대신하지 않는다.
