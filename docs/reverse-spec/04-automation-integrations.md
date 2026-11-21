# 04. 자동화와 외부 연동의 현재 계약

- 작성일: 2026-10-31
- 상태: 현재 작업 트리의 소스와 테스트를 읽어 작성한 역명세 초안
- 작성 방식: EGM draft의 목적·경계·흐름·검증 구조를 현재 구현 설명에 맞게 적용했다. 새로운 구조, 대안 선택, 구현 변경을 제안하지 않는다.
- 검증 수준: 아래의 테스트는 **테스트 코드에 작성된 기대 동작**의 근거다. 이번 문서 작업에서는 빌드, 테스트, 서버 실행, 외부 서비스 호출을 수행하지 않았다.

## 1. 목적과 경계

이 문서는 ReactiveRule의 정의·활성화·평가·명령 발행, adapter inbound 저장, Conductor 실행 요청 전송과 결과 환원, Agent 배정, Workflow 재생을 설명한다. 정본은 현재 작업 트리의 Go 코드와 SQL 스키마다. 기존 설계 문서와 삭제 중인 Knowledge 구현은 현재 기능의 근거로 사용하지 않는다.

Work 시작·제출·취소·완료의 일반 계약, Task 정의, 데이터 바인딩과 객체 변경은 [정의·Work 문서](./02-definitions-work.md)에서 설명한다. 이 문서는 그 경계를 넘나드는 호출과 결과만 다룬다. HTTP 요청의 고정 `local` attribution은 [공통 실행 문서](./00-overview.md)를 따른다.

Compos가 외부 Agent나 connector 코드를 실행하는 것은 아니다. 외부 실행에는 outbox와 Conductor를 사용하고, composite Function은 Compos의 Workflow scheduler가 재생한다. 근거: [외부 Function 요청](../../internal/kernel/work.go#L1399), [Agent 요청](../../internal/kernel/work.go#L1020), [Workflow scheduler](../../internal/server/workflow.go#L121).

## 2. 엔터티와 상태

| 엔터티 | 식별 및 저장 | 현재 상태·불변 조건 |
| --- | --- | --- |
| ReactiveRule | `reactive_rules`; `id`, workspace 내 고유 `rule_key`, `automation_id` | `draft`, `active`, `disabled`, `retired`; 별도로 `archived_at`, `version`, `current_revision_id`, `activation_event_cursor`, 회로 `closed/open`을 저장한다. |
| Rule revision | `reactive_rule_revisions`; `rule_id`, `definition_hash` | 정의 스키마 버전은 생성 시 1이다. actor·trigger·predicate·action·limits와 작성 principal을 고정한다. `permission_policy=owner_principal`이다. |
| Rule evaluation | `reactive_rule_evaluations` | predicate는 `matched/skipped/failed`; dispatch는 스키마상 `pending/dispatched/applied/rejected/retryable_failed/dead_lettered`다. 현재 실행기는 이 중 `pending/applied/rejected/retryable_failed`를 기록한다. |
| Adapter inbound | `adapter_inbound_events` | `(workspace_id, adapter_id, inbound_event_id)`가 고유하다. `appended/reduced/rejected`와 payload hash를 저장한다. |
| 외부 실행 요청 | `journal_records`의 `function.requested` 또는 `agent.requested`와 `outbox_events` | journal의 `correlation_id`와 outbox의 `_compos.execution_request_id`로 연결한다. outbox의 `published`는 수신 확인 완료이며 실행 성공을 의미하지 않는다. |
| Workflow 실행 | `workflow.started`와 `workflow.completed/workflow.failed` journal | 별도 가변 Workflow 실행 행 대신 고정 revision과 journal에서 실행 상태를 재구성한다. |

근거: [규칙·revision DDL](../../internal/store/schema.sql#L209), [inbound·evaluation DDL](../../internal/store/schema.sql#L283), [외부 요청](../../internal/kernel/work.go#L1399), [Workflow 재생](../../internal/server/workflow.go#L121).

Rule revision의 SQL UPDATE는 최초 `published_at: NULL → 값` 변경만 허용하고 다른 필드는 같아야 한다. DELETE는 금지한다. 정의 API는 규칙과 최초 revision을 함께 생성한다. 현재 registry에는 기존 규칙에 새 정의 내용을 추가하는 별도 revision 생성 명령이 없다. `publish_revision`은 존재하는 revision을 활성화한다. [불변 trigger](../../internal/store/schema.sql#L309), [생성](../../internal/kernel/executor.go#L802), [명령 목록](../../internal/kernel/registry.go#L48).

## 3. ReactiveRule 명령 계약

### 3.1 공통 입력과 local attribution

규칙 명령은 Kernel command envelope의 `command_id`, `idempotency_key`, `workspace_id`, `command_type`, `target`, `preconditions`, `payload`, `causation`을 사용한다. `target.aggregate_type=reactive_rule`이다. define은 aggregate ID가 null이어야 하고 나머지는 ID와 `reactive_rule` version precondition이 필요하다. 명령 멱등성 범위는 workspace·principal attribution·idempotency key다. [registry](../../internal/kernel/registry.go#L48), [envelope 타입](../../internal/kernel/types.go#L1).

규칙 명령은 credential 없는 공개 HTTP command로 제출할 수 있으며 요청 principal은 `local`이다. 별도 command 허가나 workspace permission 검사는 없다. 규칙 생성이나 저장된 principal은 외부 서비스 위임을 발급하지 않는다. [principal](../../internal/server/server.go#L1038), [command 경로](../../internal/server/server.go#L361)

### 3.2 명령별 결과

| 명령 | payload | 결과 |
| --- | --- | --- |
| `reactive_rule.define` | 필수: `automation_id`, `rule_key`, `name`, `requested_actor`, `trigger`, `predicates`, `action`, `idempotency`, `limits` | `draft`, version 1, current revision null인 규칙과 최초 revision을 생성한다. `reactive_rule.defined`에는 `id/revision_id/definition_hash/version`이 들어간다. |
| `reactive_rule.publish_revision` | 필수: `revision_id`, `activation_event_cursor` | 같은 workspace·규칙에 속한 revision을 요구한다. cursor는 0 이상 int64로 파싱되는 문자열이다. `active`, 회로 `closed`, current revision·cursor·enabled 시각을 설정한다. |
| `reactive_rule.disable` | 선택: `reason`(문자열 또는 null), `circuit_breaker`(boolean) | `disabled`와 disabled 시각·사유를 기록한다. `circuit_breaker=true`이면 회로를 `open`으로 만든다. |
| `reactive_rule.retire` | 선택: `reason`(문자열 또는 null) | `retired`가 된다. 구현은 이 reason을 별도 retired 사유 칼럼에 저장하지 않는다. |
| `reactive_rule.delete` | 선택: `hard`(boolean), `reason`(문자열 또는 null) | archive 경로로 `archived_at`을 설정한다. 규칙·revision을 물리 삭제하지 않는다. |

lifecycle 변경은 version을 증가시킨다. publish·disable·retire 구현에는 이전 status를 제한하는 전이표가 없으므로, `retired`를 재발행 불가능한 종단 상태라고 설명할 수 없다. archive된 규칙에는 lifecycle 명령을 적용할 수 없다. [SQL 실행 경로](../../internal/kernel/executor.go#L836), [archive](../../internal/kernel/executor.go#L877), [Rhiza 경로](../../internal/kernel/rhiza.go#L201).

### 3.3 정의 내부의 정확한 제약

| 필드 | 허용 계약 |
| --- | --- |
| 기본 문자열 | `automation_id/rule_key/name`은 trim 후 비어 있으면 안 된다. |
| `requested_actor` | 키는 `type/id`만 허용한다. `{type:"automation", id:automation_id}`여야 한다. |
| `trigger` | 키는 `source/event_type/event_schema_version/aggregate_type/allowed_causation_sources/excluded_automation_ids`만 허용한다. `source=kernel_canonical_event`, schema version은 정수 1이다. aggregate type을 주면 비어 있지 않은 문자열이어야 한다. |
| source 목록 | 두 목록은 각각 최대 64개이며 빈 배열을 허용한다. 원소는 비어 있지 않은 문자열이다. 허용 causation source는 `api/automation/adapter_reducer`다. `workflow/scheduler`는 이 목록에 직접 넣을 수 없다. 빈 allowed 목록은 평가 시 source 필터를 적용하지 않는다. |
| 제외 목록 | automation ID와 `self`를 사용할 수 있다. `self`는 이벤트의 automation ID가 해당 규칙의 automation ID와 같을 때 제외한다. |
| `action` | 키는 `action_id/kind/command_type/target_template/payload_template/preconditions_template`만 허용한다. `kind=emit_command`, action ID는 비어 있으면 안 된다. target·payload는 object, preconditions는 array다. |
| action allowlist | 현재는 `object.create`, `object.move`, `relation.link`만 허용한다. Function 실행을 ReactiveRule action으로 허용하지 않는다. |
| `idempotency` | 빈 object만 허용한다. namespace와 key는 시스템이 만든다. |
| `limits` | `max_recursion_depth`는 0 이상 정수다. `max_emits_per_minute/max_emits_per_hour/max_pending_dispatches/failure_threshold`는 모두 1 이상 정수이며 다섯 필드가 필요하다. 다른 키는 거부한다. |

등록되지 않은 action은 `forbidden`, 잘못된 내부 모양·limits·template은 주로 `schema_mismatch`, actor ID 불일치는 `validation_failed`로 거부한다. 근거: [정의 검증](../../internal/kernel/executor.go#L716), [목록 검증](../../internal/kernel/executor.go#L819), [Rhiza 검증](../../internal/kernel/rhiza.go#L104), [action allowlist](../../internal/kernel/registry.go#L75).

Trigger event는 `Registry()`가 공개하는 명령의 `EmittedEvents`에서 ReactiveRule lifecycle 이벤트를 제외한 값과 `work.deadline.reached`를 허용한다. 이 검사는 runtime journal kind 전체를 허용하는 검사가 아니다. 예를 들어 `function.invoke`의 등록 이벤트는 `function.invoked`이고, journal의 `function.succeeded`와 동일하지 않다. [이벤트 허용 검사](../../internal/kernel/registry.go#L91), [명령별 이벤트](../../internal/kernel/registry.go#L29).

### 3.4 Predicate와 template 언어

Predicate는 JSON 직렬화 기준 64 KiB 이하이며 중첩 깊이가 32를 초과하면 거부한다. 각 object에는 연산자가 하나만 있어야 한다. 지원 연산자는 `var`, `!`, `and`, `or`, `===`, `==`, `!==`, `!=`, `>`, `>=`, `<`, `<=`, `in`이다. `var`는 경로 문자열 또는 `[path, default]`, `!`는 인자 1개, and/or는 1개 이상, 비교·in은 인자 2개를 받는다. `==`와 `===`는 둘 다 `reflect.DeepEqual`을 사용한다. Match의 최종 결과는 boolean true여야 하며 단순한 truthy 값만으로 매치되지 않는다. [검증과 평가](../../internal/reactiverule/expression.go#L12).

평가 입력에는 `payload`, `target`, `metadata`와 이를 포함한 `event`가 있다. target은 aggregate type·ID·version·projection sequence이며 metadata는 event type·causation source·automation ID다. [입력 구성](../../internal/server/reactive_rule.go#L80).

Template도 각각 64 KiB와 깊이 32 제한을 받는다. literal object·array·scalar를 재귀적으로 복사하며, 참조는 단독 키 `{"$from":{"source":"event.payload|event.target|event.metadata","path":"JSON Pointer"}}` 형식이다. 빈 pointer는 root 전체를 뜻한다. `/`로 시작하는 pointer에서 `~1`과 `~0`을 해제하고 object 키 또는 array index를 찾는다. 경로 누락·지원하지 않는 source는 렌더링 실패다. [template 검증](../../internal/reactiverule/expression.go#L28), [렌더링](../../internal/server/reactive_rule.go#L307).

## 4. 규칙 평가·발행·회로 차단

### 4.1 이벤트를 평가하는 흐름

1. scheduler는 active이고 archive되지 않은 규칙의 current revision을 선택한다. activation cursor보다 큰 projection event를 command·aggregate type·aggregate ID로 activity와 결합하고 command ledger의 causation을 읽는다.
2. 해당 revision·event의 evaluation이 없을 때만 후보가 된다. 전역 순서는 projection sequence, rule ID이며 한 번에 최대 100개다. event type 필터는 SQL이 아니라 평가 단계에서 적용한다.
3. trigger와 predicate, 원래 automation depth+1, 발행량·pending 제한을 검사한다. 불일치는 `predicate_result=skipped`, `dispatch_status=applied`로 저장하며 명령 ID는 없다. Predicate 평가 오류는 별도 실패로 저장하지 않고 match false로 취급한다.
4. 매치되면 target·payload·precondition을 렌더링하고 발행할 command와 revision 작성 principal을 evaluation에 저장한다. 렌더링 실패는 `failed/rejected`로 저장한다. 정상 매치는 `matched/pending`이다.
5. `INSERT OR IGNORE`와 evaluation 고유키로 같은 action의 중복 기록을 억제한 뒤 dispatch를 진행한다.

근거: [후보 조회](../../internal/server/reactive_rule.go#L39), [평가](../../internal/server/reactive_rule.go#L80), [저장](../../internal/server/reactive_rule.go#L140).

### 4.2 결정적인 ID와 실제 actor 경계

SHA-256은 소문자 hex를 사용한다. evaluation ID는 `aev_`와 `workspace:revision:event:action` hash 앞 32자를 결합한다. command ID는 `cmd_automation_`와 evaluation ID hash 앞 24자다. namespace는 `workspace/automation/revision/action`, key는 `automation:<automation>:<revision>:<event>:<action>`이다. 생성 command의 actor는 automation이며 causation에는 source `automation`, triggering event ID, automation ID, revision ID, recursion depth를 넣는다. 렌더링한 전체 command의 hash도 저장한다. [ID와 envelope 구성](../../internal/server/reactive_rule.go#L103).

발행은 revision 생성 principal과 requested automation actor를 일반 `Execute`에 전달한다. actor는 감사 attribution이며 접근 허가가 아니다. 저장된 principal만으로 서비스 위임을 주장하지 않는다. [dispatch](../../internal/server/reactive_rule.go#L184), [actor 해석](../../internal/kernel/actor.go#L5)

### 4.3 제한·재시도·회로의 실제 단위

| 항목 | 구현 계약 |
| --- | --- |
| 분·시간 발행량 | workspace+revision에서 최근 1분/1시간 동안 `emitted_command_id IS NOT NULL`인 evaluation을 센다. 성공한 명령만 세는 것이 아니다. |
| pending 제한 | 같은 workspace+revision의 `pending/retryable_failed` 수를 센다. |
| 제한에 걸린 이벤트 | skipped evaluation을 저장한다. 동일 revision·event를 나중에 다시 평가하는 지연 큐가 아니다. |
| dispatch 후보 | active 규칙이며 회로가 closed인 `pending/retryable_failed` evaluation을 created_at·ID 순으로 최대 100개 선택한다. current revision과 같아야 한다는 조건은 없다. |
| 성공 | `applied`, attempt+1을 기록한다. Workflow를 깨운다. Function invoke이면 Conductor wake 코드도 있지만 현재 action allowlist에는 해당 명령이 없다. |
| 실패 | retryable CommandError 또는 일반 오류는 `retryable_failed`, 비재시도 CommandError는 `rejected`다. 오류 코드·메시지와 attempt를 저장한다. |
| 재시도 시점 | 다음 scheduler 순회에서 다시 선택한다. 별도 backoff·next-attempt 시각·evaluation별 최대 시도 횟수는 없다. |
| 회로 임계값 | 같은 workspace+revision에서 현재 상태가 `rejected/retryable_failed/dead_lettered`인 행의 `dispatch_attempt_count` 합을 쓴다. 연속 실패 횟수가 아니다. |
| 회로 열기 | 실패 후 합계가 threshold 이상이고 해당 revision이 여전히 current active/closed이면 생성 principal로 version precondition을 포함한 `reactive_rule.disable`을 실행한다. reason은 `automation dispatch failure threshold reached`, `circuit_breaker=true`다. |

회로 command ID는 `cmd_automation_circuit_`+`workspace:rule:revision` hash 앞 24자이며 key는 `automation:circuit:<revision>`이다. 회로를 재발행으로 닫아도 같은 revision의 실패 집계가 초기화되는 코드는 없다. 동일 revision 회로 명령의 멱등성도 유지된다. [dispatch 및 회로](../../internal/server/reactive_rule.go#L145), [제한 집계](../../internal/server/reactive_rule.go#L276).

**확인된 경계:** dispatch 후보 SQL은 `archived_at IS NULL`을 검사하지 않는다. 따라서 archive가 status를 active로 유지한 경우 이미 저장된 pending evaluation까지 중단한다고 보장할 수 없다. 또한 한 batch를 먼저 읽고 순서대로 실행하므로 중간에 회로가 열려도 이미 선택한 뒤쪽 항목을 다시 필터링하지 않는다. [후보 SQL](../../internal/server/reactive_rule.go#L145), [archive SQL](../../internal/kernel/executor.go#L892).

## 5. 일반 Adapter inbound HTTP 계약

`POST /api/v1/workspaces/{workspace_id}/adapter/inbound-events`의 body는 `adapter_id`, `inbound_event_id`, `payload`다. ID 둘은 trim 후 1~128 **bytes**여야 한다. payload는 임의 JSON 값이며 별도 필수 검증이 없어 생략 시 null로 저장된다. 공통 decode는 기본 1 MiB 제한과 알려지지 않은 struct 필드 거부를 적용한다. [route](../../internal/server/server.go#L89), [handler](../../internal/server/server.go#L890), [decode](../../internal/server/server.go#L978).

새 항목은 `ain_`+UUID, 정규화 payload·SHA-256 hash, `appended`와 UTC 수신 시각을 저장하고 202를 반환한다. 같은 workspace·adapter·event ID와 같은 payload hash는 최초 ID·수신 시각으로 200을 반환한다. 다른 hash는 409 `idempotency_conflict`다. 응답은 `data:{id,workspace_id,adapter_id,inbound_event_id,status,received_at}`다. handler는 저장된 상태를 재조회하여 응답하지 않고 status를 `appended`로 출력한다. [저장과 응답](../../internal/server/server.go#L919).

inbound append도 인증 gate 없이 등록된 route로 들어온다. append 경계 자체가 임의 payload를 Kernel event로 자동 환원하지 않는다. Rhiza는 기존 hash 확인·INSERT OR IGNORE·재조회로 멱등성을 처리한다. [HTTP 경계](../../internal/server/server.go#L54)

## 6. Conductor outbound 계약

### 6.1 설정과 실행 요청 payload

`EnableConductor(baseURL, token, callbackToken)`은 host가 있는 절대 HTTP(S) URL만 받으며 HTTP client timeout은 30초다. outbound token은 비어 있으면 Authorization을 보내지 않는다. callback token과 connector read용 token은 별도 검사 경로를 갖는다. [설정](../../internal/server/conductor.go#L24).

외부 Function은 connector이면 handler key `connector.invoke`, native이면 binding의 handler key를 쓴다. connector는 input에 provider·operation을 덧붙인다. 전송 data는 input, `subject_ref=Work context`, `_compos`를 포함한다. `_compos`에는 workspace, execution request, handler key/version, operation, workflow instance와 task ID, actor를 넣는다. 여기서 workflow instance ID와 task ID는 둘 다 **Work stream ID**이며 Task revision ID가 아니다. optional connection ID, credential revision, timeout, retry policy도 전달한다. participant human은 outbound actor user로 바꾼다. [payload 생성](../../internal/kernel/work.go#L1399).

외부 Function invocation ID는 command에서 파생한 `inv_` ID이며 pinned Function revision과 handler identity를 journal에 기록한다. Agent는 route command에서 파생한 `agn_` ID를 사용한다. Agent data에는 `agent_definition`, `task_definition`, `input`, `subject_ref`, `_compos`가 있고 actor type은 agent다. [Agent payload](../../internal/kernel/work.go#L1033).

실행 정책은 timeout 정수 1~86400초, retry policy의 양수 정수 `max_attempts`와 선택 `backoff`를 검증한다. backoff는 비어 있지 않은 문자열 또는 음수가 아닌 숫자이며 지원 알고리즘 목록을 검사하지 않는다. credential revision은 1~9007199254740991이며 connection ID가 필요하다. 이러한 정책의 외부 실행·재시도는 Conductor에 전달되는 계약이다. Compos outbound poller의 재시도 횟수 제한으로 쓰이지 않는다. [정책 검증](../../internal/kernel/work.go#L808), [credential 검증](../../internal/kernel/work.go#L346).

### 6.2 전송과 acknowledgement

1. `_compos.execution_request_id`가 JSON text이고 pending인 outbox를 created_at·ID 순으로 최대 **64개** 읽는다. 이 크기는 Rhiza 한 요청의 64 SQL statements 제한에 맞춰진 값이다.
2. `<baseURL>/v1/events`로 `POST {events:[{id,name,idempotency_key,data}]}`를 전송한다. name은 outbox event type, idempotency key는 원래 command ID다. Content-Type은 application/json이다.
3. HTTP 2xx여도 acknowledgement 검증에 실패하면 pending을 유지한다. non-2xx 오류 본문은 최대 4096 bytes를 읽는다.
4. 전체 batch acknowledgement가 유효할 때만 해당 outbox를 published로 바꾼다. 외부 실행의 terminal callback은 별도로 받는다.

근거: [전송과 상태 갱신](../../internal/server/conductor.go#L463).

Acknowledgement는 최대 1 MiB이며 `{accepted:true, events:[...]}` 형식이다. 각 항목에는 비어 있지 않은 `execution_id`, `execution_request_id`와 허용 status가 필요하다. status는 `queued/running/retry_wait/succeeded/failed/timed_out/cancelled`다. 모든 요청을 정확히 한 번씩 확인해야 하며 foreign·중복·누락 request는 거부한다. `event_id`는 생략할 수 있지만 주면 원래 outbox ID 문자열과 같아야 한다. 빈 문자열·null은 일치하지 않는다. trailing JSON 또는 junk도 전체 JSON unmarshal에서 거부한다. [ack 검증](../../internal/server/conductor.go#L532).

Dispatcher는 pending callback 환원 후 outbox 전송을 수행하고 1초 ticker 또는 wake로 다시 순회한다. 오류는 로그를 남기며 pending 항목을 다음에 다시 전송한다. 영속 attempt 일정, lease, backoff, outbound 최대 재시도 횟수나 circuit breaker는 이 poller에 없다. 수신 성공 후 published 저장 전에 실패하면 같은 ID로 다시 전송할 수 있다. Conductor가 제공해야 할 외부 멱등 실행 자체는 이 저장소만으로 검증할 수 없다. [poll loop](../../internal/server/conductor.go#L377).

## 7. Conductor callback과 환원

### 7.1 입력·principal·식별

`POST /internal/v1/conductor/callbacks`는 Compos가 incoming credential을 검증하지 않는다. callback evidence의 invocation·handler·terminal 상태 검사는 유지한다. network 경계는 배포 gateway가 제한해야 한다. body 제한은 8 MiB다. [route](../../internal/server/server.go#L90), [검증](../../internal/server/conductor.go#L61).

| 필드 | 입력 계약 |
| --- | --- |
| `callback_id/workspace_id/execution_request_id/execution_id/provider_key/provider_run_ref/handler_version` | 문자열이며 trim 검사에서 비어 있으면 안 된다. 일반 adapter의 128-byte 제한은 이 handler가 적용하지 않는다. |
| `runtime_status` | `succeeded/failed/cancelled` 중 하나다. ack의 timed_out·running 등은 callback terminal status가 아니다. |
| `attempt_count` | 1 이상 정수다. |
| `occurred_at` | RFC3339Nano로 파싱할 수 있어야 한다. |
| `result` | 선택 JSON 값이다. 성공 환원에는 JSON object가 필요하다. |
| `error_class/error_detail` | 선택 문자열 또는 null이다. |
| `task_id/workflow_instance_id/correlation_id/causation_id` | 선택 문자열이다. invocation 조회의 권위 있는 식별자로 사용하지 않는다. |

조회는 callback workspace와 execution request에 해당하는 requested journal 및 같은 command의 outbox를 찾는다. Function은 connector/native만 허용한다. provider key와 handler version은 outbox에 고정된 identity와 같아야 한다. 없음은 404 `not_found`, 불일치는 409 `execution_identity_conflict`다. execution ID·provider run ref의 과거 acknowledgement 일치 여부를 검사하는 경로는 없다. [구조체](../../internal/server/conductor.go#L41), [invocation 조회](../../internal/server/conductor.go#L156).

### 7.2 저장·환원·중복 수렴

Callback 전체를 정규화해 adapter `conductor`의 inbound로 저장한다. result 숫자는 UseNumber로 보존하고 result JSON을 재직렬화한 뒤 hash를 만든다. 같은 callback ID의 다른 정규화 내용은 409 `idempotency_conflict`다. 저장 후 즉시 환원하며 최초 정상 요청은 202, 같은 callback 재요청은 200을 반환한다. 정상 data에는 inbound 식별자, stream ID, status `reduced`, 수신 시각이 들어간다. [정규화·저장·응답](../../internal/server/conductor.go#L95).

환원의 순서는 다음과 같다.

1. 이미 reduced/rejected인 동일 inbound는 해당 상태로 끝낸다. rejected 재요청은 200과 status rejected를 반환한다.
2. Work journal에 `work.satisfied/work.cancelled`가 있으면 새 callback은 거부한다. 같은 reduced callback 재생은 이 검사 전에 끝나므로 완료 후에도 중복 성공 응답이 가능하다.
3. result가 있고 null이 아닌데 object로 읽지 못하면 `failed/output_contract_mismatch`로 바꾼다. succeeded인데 result가 없거나 null이어도 같은 실패로 바꾼다. failed/cancelled의 result 생략 또는 null은 허용하고 빈 output object를 사용하며 기존 error class/detail을 유지한다.
4. 이미 해당 invocation의 terminal journal이 있으면 status·output·error class/detail의 digest를 비교한다. 같으면 새 callback도 reduced로 수렴하고 다르면 409 `terminal_result_conflict`다. Kernel은 schema 검증으로 실패를 정규화하기 **전**의 result digest를 보관하므로 동일한 잘못된 성공 결과도 수렴할 수 있다.
5. 아직 terminal이 없으면 원래 request journal principal로 `function.record_result` 또는 `agent.record_result`를 실행한다. command ID는 `cmd_conductor_`+`workspace:callback` hash 앞 32자, key는 `conductor:<callback_id>`, causation source는 `adapter_reducer`다.

근거: [환원 core](../../internal/server/conductor.go#L200), [terminal 수렴](../../internal/server/conductor.go#L264), [Function 원본 digest](../../internal/kernel/work.go#L1499), [Agent 원본 digest](../../internal/kernel/work.go#L1625).

HTTP 환원에서 terminal conflict와 CommandError는 inbound를 rejected로 바꾸려고 시도한다. 이 rejected 갱신 오류는 handler가 무시한다. 일반 DB 오류는 appended를 남겨 복구할 수 있다. background 복구는 conductor/appended 최대 100개를 수신 시각순으로 조회한다. invocation 없음과 CommandError는 rejected로 바꾸고 계속하지만, identity conflict·terminal conflict 등 일반 오류는 해당 순회를 중단한다. HTTP 경로와 background 경로의 실패 처리는 같지 않다. [HTTP 오류 처리](../../internal/server/conductor.go#L175), [복구 loop](../../internal/server/conductor.go#L398).

### 7.3 Kernel 결과 명령의 경계

`function.record_result/agent.record_result`는 work_stream ID를 target으로 받으며 필수 payload는 `invocation_id/callback_id/runtime_status/output/adapter_inbound_event_id/execution_id/provider_key/provider_run_ref/handler_version/attempt_count`다. causation source는 `adapter_reducer`여야 한다. `adapter_inbound_event_id=callback_id`가 필요하고, 원래 pinned handler identity 및 아직 terminal이 아닌 invocation을 검사한다. output은 object다. `error_class/error_detail`은 선택 문자열이다. [registry](../../internal/kernel/registry.go#L35), [causation 검사](../../internal/kernel/executor.go#L66), [Function 환원](../../internal/kernel/work.go#L1460), [Rhiza 결과 파싱](../../internal/kernel/rhiza_runtime.go#L193).

성공 Function 결과는 pinned output schema와 emits Event schema를 검증한다. 불일치는 각각 `output_schema_mismatch/event_schema_mismatch`로 실패 사실을 기록한다. 성공이면 Function 결과와 필요한 Event·Work 완료 사실을 기록하고 실패이면 Task의 `on_function_failed` 정책을 적용한다. runtime cancelled도 function.failed 경로로 간다. inbound appended→reduced 갱신은 결과 기록과 같은 트랜잭션 또는 Rhiza SQL batch에서 정확히 한 행이어야 한다. [Function 결과와 증거 갱신](../../internal/kernel/work.go#L1508), [Rhiza Function 환원](../../internal/kernel/rhiza_runtime.go#L283).

이 세 runtime 명령 `function.record_result/agent.record_result/workflow.complete`는 공개 command endpoint에서 403으로 거부하고 공개 Registry에서도 숨긴다. [공개 endpoint 차단](../../internal/server/server.go#L364), [Registry 필터](../../internal/kernel/registry.go#L81).

## 8. Agent와 Workflow 오케스트레이션

### 8.1 Agent 정의와 route

Agent 정의의 spec kind는 `participant`다. 이름과 instructions는 필수 비어 있지 않은 문자열이며 execution policy에는 handler key/version이 필요하다. 선택 model/context/permissions/memory policy는 각 최대 64항목 object, tools는 최대 128개 문자열 목록이다. 정의를 outbox로 전달하는 것과 모델·tool·network 권한을 실제 외부 runner에서 집행하는 것은 별개다. [정의 검증](../../internal/kernel/work.go#L232).

`work.route`에서 agent participant를 지정하면 pinned participant revision, Task와 Work context를 읽고 `work.routed` 뒤 `agent.requested`를 기록하며 outbox를 생성한다. Agent 성공 callback은 Task 또는 Workflow route에 고정된 output schema를 적용한다. 성공 시 `agent.succeeded`와 `work.submitted`를 함께 기록한다. Workflow route가 아니고 completion이 submission_valid이면 `work.satisfied`도 기록한다. 실패는 `agent.failed`와 `on_agent_failed` 정책으로 간다. [route](../../internal/kernel/work.go#L991), [Agent 결과](../../internal/kernel/work.go#L1590), [Rhiza 결과](../../internal/kernel/rhiza_runtime.go#L388).

실패 정책은 cancel이면 work.cancelled, route이면 human participant의 work.routed 사실을 추가한다. Task 정의 검증에서 route 정책은 human만 허용하므로 agent 또는 automation으로 재배정하는 실패 정책은 발행할 수 없다. 실패 정책 자체는 일반 `work.route` handler를 호출하지 않는다. [정책 발행 검증](../../internal/kernel/work.go#L139), [실패 정책 적용](../../internal/kernel/work.go#L1695).

### 8.2 Workflow 정의 계약

Workflow spec은 `name/steps`만 허용하며 name과 steps는 비어 있으면 안 된다. 모든 step ID는 고유한 ASCII identifier이며 1~64자의 영문 대소문자·숫자·밑줄·하이픈만 허용한다. 전체 최대 128 steps, depth가 8을 초과하면 거부하며 parallel branches는 2~16개다. [정의 검증](../../internal/kernel/work.go#L366), [identifier 검사](../../internal/kernel/work.go#L599).

| step kind | 필드와 수행 |
| --- | --- |
| `route` | `id/kind/participant`, 선택 `reason/output_schema/output_mapping/data_binding_ids`를 받는다. human·agent는 해당 invocation/step/route의 work.submitted를 기다린다. automation은 제출을 기다리지 않는다. |
| `invoke` | `id/kind/function_revision_id`, 선택 input/output mapping을 받는다. 현재 route actor로 Function을 호출하고 function terminal을 기다린다. |
| `branch` | `id/kind/condition/then/else`, 선택 output mapping이다. then은 비어 있으면 안 되고 else는 빈 array도 허용한다. 조건에 따라 한쪽을 재생한다. |
| `parallel` | `id/kind/branches`, 선택 output mapping이다. 각 branch는 `id/steps`만 받으며 branch ID가 고유하고 steps는 비어 있으면 안 된다. 모든 branch가 끝나면 branch ID별 output object로 합친다. |

Route output schema는 object schema여야 한다. data binding ID는 주면 1~32개 고유 identifier다. mapping은 주면 1~128항목 object이고 key는 비어 있으면 안 된다. `$`로 시작하는 문자열 selector는 `$.`로 시작해야 한다. runtime mapping은 최상위 값만 해석하며 `root/current/steps/output`에서 점으로 구분한 object key를 탐색한다. array index·재귀 template 언어가 아니다. 찾지 못하면 Workflow 실패로 간다. branch 조건은 `left` selector와 `operator=exists/not_exists/eq/not_eq`, 비교 시 `right`를 사용한다. [mapping 검증](../../internal/kernel/work.go#L505), [runtime mapping·조건](../../internal/server/workflow.go#L314).

Spec 발행은 참조 revision의 종류·존재와 Function↔Workflow 실행 graph의 순환을 검사한다. 이는 모델 runner의 임의 tool 호출 graph를 검증하는 기능이 아니다. [Rhiza 참조와 순환 검증](../../internal/kernel/rhiza_spec.go#L11).

### 8.3 재생·actor·완료

Composite Function 호출은 function.requested와 workflow.started를 같은 invocation correlation으로 기록한다. scheduler는 아직 workflow.completed/failed가 없고 Work도 satisfied/cancelled가 아닌 started 항목을 최대 100개 읽는다. 고정 Workflow revision이 없으면 실패 완료를 시도한다. 매 순회 root/current를 최초 input으로, steps를 빈 map으로 시작하고 journal에서 이미 완료한 단계를 복원한다. [composite 호출](../../internal/kernel/work.go#L1195), [재생](../../internal/server/workflow.go#L121).

Workflow command ID는 `cmd_wf_`+SHA-256(`invocation + NUL + step + NUL + action`) 앞 16 bytes의 hex다. key는 `workflow:<invocation>:<step>:<action>`, causation은 source workflow와 invocation ID다. route command ID가 workflow_route_id이며 submission 조회는 step ID와 이 route ID를 함께 비교해 반복 실행 간 제출을 분리한다. child Function invocation은 `inv_`+invoke command ID에서 하이픈을 제거한 값이다. [command 생성](../../internal/server/workflow.go#L371), [submission 조회](../../internal/server/workflow.go#L419), [child ID](../../internal/server/workflow.go#L209).

human 제출 후 state principal은 제출 principal로 바뀐다. invoke의 현재 참여자가 human이면 그 principal과 같아야 진행하며, agent면 service actor, automation이면 automation actor로 Kernel을 호출한다. 배정은 외부 서비스 접근 허가를 발급하지 않는다. adapter·서비스 계약 없는 기본 실행에서는 연동 명령이 실행되지 않는다. [route 처리](../../internal/server/workflow.go#L179), [actor 선택](../../internal/server/workflow.go#L384).

parallel은 이 scheduler 안에서 branch를 순서대로 순회하여 외부 실행을 요청하고 pending 여부를 모은다. 모든 branch가 준비되어야 합치며 branch 내부 principal·route 변경은 상위 state로 복사하지 않고 step output들만 합친다. Workflow 처리 중 오류는 workflow.complete 실패 요청으로, 완료 준비가 되면 성공 요청으로 간다. [parallel 실행](../../internal/server/workflow.go#L276), [완료](../../internal/server/workflow.go#L445).

`workflow.complete`의 payload는 invocation ID, runtime status succeeded/failed, output object와 선택 error detail이다. Kernel은 Work가 열려 있고 원래 composite 요청·Workflow 시작이 있으며 아직 Workflow terminal이 없는지 검사한다. 성공 output을 Function schema와 Event 계약으로 검사한 뒤 Workflow와 Function terminal 및 필요한 Work 완료 사실을 기록한다. [registry](../../internal/kernel/registry.go#L37), [Kernel 완료](../../internal/kernel/work.go#L1290), [Rhiza 완료](../../internal/kernel/rhiza_runtime.go#L472).

### 8.4 Scheduler 생명주기와 deadline

Workflow scheduler는 매 순회 deadline → ReactiveRule → Workflow 순서로 처리하고 wake 또는 1초 ticker를 기다린다. 각 단계 오류는 로그를 남기고 다음 단계로 진행하지만 context 취소 또는 database is closed 오류이면 종료한다. deadline 후보는 Work 시작에 기록된 deadline이 현재 UTC 시각 이하고 satisfied/cancelled/deadline.reached가 없는 stream 최대 100개다. `work.record_deadline`을 원래 시작 principal로 실행하고 causation scheduler와 `deadline:<stream>:<deadline>` key를 사용한다. deadline 사실 자체가 자동 취소를 뜻하지는 않는다. [scheduler](../../internal/server/workflow.go#L38), [deadline 조회·명령](../../internal/server/workflow.go#L79).

## 9. Conductor가 읽는 Compos connector API

두 GET은 `X-Conductor-Workspace-Id`로 읽을 workspace를 선택한다. incoming service credential 검사는 없다. workspace header 없음은 400 validation_failed다. 접근 제한은 배포 gateway가 맡는다. [connector](../../internal/server/conductor.go#L313).

| API | 응답·실패 |
| --- | --- |
| `GET /v1/connector/objects/{object_key}/schema` | archive되지 않은 해당 workspace/type key의 `id,key,schema_version,field_schema,relation_schema,state_machine_id`를 200으로 직접 반환한다. 없음은 404다. |
| `GET /v1/connector/objects/{object_key}/records/{record_id}` | workspace 내 object 표현을 200으로 직접 반환한다. 없음 또는 object type key 불일치는 404다. |

이 응답은 data wrapper를 사용하지 않는다. DB 오류는 공통 500 internal_error/storage/retryable=true다. [읽기 handler](../../internal/server/conductor.go#L330), [공통 오류](../../internal/server/server.go#L1060).

## 10. 영속성·복구와 확인된 한계

규칙 정의·상태 변경은 command ledger 및 event/projection 기록과 Kernel commit 경계를 따른다. 평가 기록·dispatch 상태 갱신·회로 명령은 별도 쓰기다. 따라서 명령 적용 뒤 evaluation 갱신 전에 중단될 수 있으며, 이 경우 저장된 결정적 명령·idempotency key로 재호출한다. 같은 원리는 Conductor 수신과 outbox published 갱신 사이에도 적용되지만 외부 runner의 중복 방지는 Compos 밖의 계약이다. [evaluation dispatch](../../internal/server/reactive_rule.go#L179), [outbox 갱신](../../internal/server/conductor.go#L515).

아래 항목은 미래 설계에 대한 가정이 아니라 현재 코드에서 확인되는 경계다.

- `dispatched/dead_lettered`는 evaluation DDL에 있지만 현재 dispatcher가 해당 상태로 이동시키는 코드는 없다. 운영자가 사용할 replay·dead-letter API도 여기서 확인한 registry에 없다.
- activation cursor는 publish 시 정한 하한이며 매 평가 후 증가시키는 per-rule cursor가 아니다. event×rule 조회는 매번 이미 평가한 조합을 제외한다.
- 발행량 제한은 revision별 집계이므로 automation 전체나 workspace 전체의 공동 예산을 보장하지 않는다. 제한에 걸린 이벤트는 영구 skipped 평가로 남는다.
- Rule dispatch와 Conductor poller는 서로 다른 실패 집계와 재시도 경로다. Rule의 failure_threshold가 외부 runner 재시도를 제한하지 않는다.
- 회로 threshold는 실패 상태인 행들의 attempt 합이다. 렌더링 실패는 attempt 0이며 dispatch되지 않아 그 자체로 회로 체크를 호출하지 않는다.
- 일반 inbound API는 adapter ID를 예약하거나 payload를 callback 구조로 검증하지 않는다. background Conductor reducer는 adapter ID conductor의 appended 항목을 해석한다. inbound 경로는 모두 인증 gate 없이 열리므로 deployment gateway가 network 범위를 제한해야 한다.
- callback 수렴은 새 inbound의 Work terminal 검사 뒤 수행한다. Work가 끝난 후 새 callback ID로 온 동일 결과와, 기존 reduced callback ID를 재전송한 경우는 다르게 처리된다.
- terminal callback은 timeout·retry policy 전달과 별개다. Compos가 Conductor 실행 상태를 주기 조회하여 유실 callback을 생성하는 경로는 이 poller에 없다.
- HTTP decode는 첫 JSON 값을 읽고 알려지지 않은 필드를 거부하지만 후속 EOF 검사를 하지 않는다. acknowledgement 전체 JSON 검증과 같은 엄격도라고 볼 수 없다.
- SQL revision 불변 trigger는 최초 published_at 갱신만 허용한다. Rhiza publish SQL은 이미 published인 revision에도 UPDATE를 요청하므로, 동일 revision 재발행의 저장소별 동등성을 이 문서의 정적 대조만으로 입증하지 않는다.

근거: [evaluation DDL](../../internal/store/schema.sql#L297), [평가·cursor](../../internal/server/reactive_rule.go#L39), [회로](../../internal/server/reactive_rule.go#L222), [inbound](../../internal/server/server.go#L890), [callback 복구](../../internal/server/conductor.go#L398), [decode](../../internal/server/server.go#L1044), [불변 trigger](../../internal/store/schema.sql#L309), [Rhiza publish](../../internal/kernel/rhiza.go#L221).

## 11. 테스트 코드가 제공하는 근거와 검증 공백

다음 표는 테스트 함수와 본문의 assertion을 읽은 결과이며 이번 작업에서 실행한 검증 결과가 아니다.

| 테스트 근거 | 작성된 검증 대상 |
| --- | --- |
| [ReactiveRule 발행·재생·회로](../../internal/server/reactive_rule_test.go#L15) | Seed 이벤트에서 follow-up이 한 번 생성되고 재평가해도 중복되지 않는지, 실패 attempt를 설정한 뒤 disabled/open이 되는지 검사한다. |
| [다중 effect pairing](../../internal/server/reactive_rule_test.go#L89), [Rhiza pairing](../../internal/server/reactive_rule_test.go#L152) | parent command의 projection event와 activity가 올바른 aggregate로 연결되는지 검사한다. |
| [actor attribution](../../internal/kernel/actor_test.go#L21) | requested actor 형식과 effective actor 기록을 검사한다. 접근 허가 판정은 없다. |
| [허용 이벤트·action 목록](../../internal/kernel/executor_test.go#L147) | 공개 trigger와 제한된 automation action 경계를 검사한다. |
| [Rhiza inbound 경쟁](../../internal/server/adapter_rhiza_test.go#L13) | 동시 동일 inbound의 멱등성과 payload 충돌을 검사한다. |
| [Conductor 전송](../../internal/server/conductor_test.go#L19), [ack 오류](../../internal/server/conductor_test.go#L109) | 실행 요청만 전송하는지, malformed·누락·foreign·중복·잘못된 ID·oversize ack에서 pending이 유지되고 동일 invocation으로 재시도하는지 검사한다. |
| [ack status](../../internal/server/conductor_test.go#L233), [Rhiza batch](../../internal/server/conductor_test.go#L254) | queued와 terminal ack를 수락하고 64개 batch로 outbox를 소진하는지 검사한다. |
| [callback scope·멱등](../../internal/server/conductor_test.go#L343), [Rhiza 변경 callback](../../internal/server/conductor_test.go#L500) | callback의 workspace·중복 내용과 변경 충돌을 검사한다. |
| [잘못된 결과의 수렴](../../internal/server/conductor_test.go#L578) | Kernel 정규화 이후에도 동일 invalid result가 terminal 한 번으로 수렴하는지 검사한다. |
| [connector 읽기](../../internal/server/conductor_test.go#L648) | service token과 workspace 범위를 검사한다. |
| [Agent 요청·결과](../../internal/server/agent_test.go#L12) | route outbox, 성공 제출·완료, 완료 뒤 동일 callback 재생, 새 late callback 거부, invalid output 수렴을 검사한다. |
| [Workflow 재생](../../internal/server/workflow_test.go#L16), [parallel join](../../internal/server/workflow_test.go#L86) | composite의 단일 완료와 branch output mapping을 검사한다. |
| [route 대기](../../internal/server/workflow_test.go#L161), [고정 schema·human](../../internal/server/workflow_test.go#L210) | 제출 대기와 제출한 human principal의 후속 invoke를 검사한다. |
| [parallel route 격리](../../internal/server/workflow_test.go#L327), [반복 invocation 격리](../../internal/server/workflow_test.go#L389) | step/route identity로 제출이 섞이지 않는지 검사한다. |
| [cycle 거부](../../internal/server/workflow_test.go#L467), [deadline](../../internal/server/workflow_test.go#L489), [scheduler 종료](../../internal/server/workflow_test.go#L520) | 실행 graph 순환, deadline 사실 한 번 기록, 저장소 종료 후 scheduler 종료를 검사한다. |
| [terminal Work 경계](../../internal/kernel/workflow_terminal_test.go#L14) | 완료·취소된 Work에 Workflow 완료가 추가되지 않는지 검사한다. |

테스트 fixture의 mock Conductor나 직접 inbound INSERT는 실제 Conductor 배포와 외부 provider의 실행을 입증하지 않는다. 이번에는 테스트를 실행하지 않았으므로 현재 작업 트리의 통과 여부, 다중 프로세스 발행 경쟁, crash 시점별 복구, archive된 규칙의 pending 처리, 같은 revision 재발행과 회로 재개, 장기 backlog에서의 순회 지연을 실행 검증했다고 주장하지 않는다.

## 12. 소스 지도

| 책임 | 정본 소스 |
| --- | --- |
| 명령 공개·payload·action/event allowlist | [registry.go](../../internal/kernel/registry.go#L29) |
| 규칙 정의·lifecycle·archive | [executor.go](../../internal/kernel/executor.go#L716), [rhiza.go](../../internal/kernel/rhiza.go#L104) |
| predicate·template 정적 제한 | [expression.go](../../internal/reactiverule/expression.go#L12) |
| 규칙 평가·dispatch·회로·template runtime | [reactive_rule.go](../../internal/server/reactive_rule.go#L26) |
| HTTP route·inbound·decode | [server.go](../../internal/server/server.go#L89), [inbound](../../internal/server/server.go#L890), [decode](../../internal/server/server.go#L978) |
| 요청 principal·HTTP 경계 | [server.go](../../internal/server/server.go#L54) |
| Conductor callback·outbox·connector 읽기 | [conductor.go](../../internal/server/conductor.go#L24) |
| Workflow scheduler·step 재생 | [workflow.go](../../internal/server/workflow.go#L38) |
| Agent·Workflow spec·정책·외부 실행 결과 | [work.go](../../internal/kernel/work.go#L232), [외부 요청](../../internal/kernel/work.go#L1399) |
| Rhiza spec 참조·실행 graph | [rhiza_spec.go](../../internal/kernel/rhiza_spec.go#L11) |
| Rhiza Function·Agent·Workflow 결과 환원 | [rhiza_runtime.go](../../internal/kernel/rhiza_runtime.go#L193) |
| 정본 테이블·불변성·고유키 | [schema.sql](../../internal/store/schema.sql#L209) |

*Drafted with EGM.*
