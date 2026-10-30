# Constitutional Architecture Implementation Plan

Status: Implemented for the bounded first slice

Implemented in the first Go/SQLite slice:

- `spec_revisions` and `journal_records` with JSON, uniqueness, foreign-key and
  immutability constraints
- Task, Function, Workflow, AgentDefinition, Domain Event and Ontology spec validation
- `spec.publish`, `work.open`, `work.route`, `work.submit`, `function.invoke`,
  `event.emit`, `knowledge.propose` and human-only `knowledge.review`
- allowlisted `object.move` Function binding and atomic completion facts
- native, connector and composite Function bindings
- Journal-replayed Workflow coordination with `route`, `invoke`,
  deterministic branch, parallel fan-out/join and step mappings
- Human inbox/submission, deadline facts, ReactiveRule SLA reactions, final
  Function failure policy, and Agent result submission
- Knowledge proposal → human review → immutable Ontology revision promotion
- `GET /api/v1/workspaces/{workspace_id}/work/{stream_id}` projection
- authorization, idempotency, immutable history and end-to-end tests

이 계획은 [Compos Constitutional Architecture](../architecture/constitutional-architecture.md)를
현재 Go Kernel과 canonical SQL runtime 위에 가장 작은 형태로 구현한다. 운영은
Rhiza, local/test는 SQLite이며 개념은 유지하되
각 개념마다 table과 mutable status를 만들지 않는다.

## 1. 구현 결정: World — Spec — Journal

물리적 모델은 네 계열만 사용한다.

```text
1. World
   기존 object_instances와 relation storage
   현재 operational state의 유일한 source of truth

2. Spec
   spec_revisions
   bounded kind별 immutable definition revision

3. Journal
   journal_records
   work, routing, invocation, submission, completion의 append-only facts

4. 기존 Kernel infrastructure
   command ledger / authorization / idempotency / activity / outbox / projection
```

`TaskRun`, `WorkItem`, `Assignment`, `Handoff`, `Execution`, `Submission`,
`FunctionRun`, `WorkflowRun`과 `StepRun`은 canonical table이 아니라 journal
projection이다.

## 2. 기존 기반에서 유지할 것

- `object_types`, schema revisions, `object_instances`와 relations
- relation table을 canonical source로 두고 `relation_schema_json`은 derived
  revision snapshot으로 사용하는 원칙
- Go Kernel command와 단일 canonical SQL transaction의 write boundary
- command ledger, activity, outbox와 projection event의 atomic 기록
- JSON validity, schema identity, revision digest, automatic number uniqueness,
  RFC3339 time와 typed scalar index
- `actor_capabilities` Kernel authorization
- 기존 `reactive_rules` 저장 호환성

제거된 legacy `task.create`, `task_submission.*`, `compos task`와
`compos submission`은 복원하지 않는다.

## 3. 두 개의 새 persistence primitive

### `spec_revisions`

최소 필드:

```text
id
workspace_id
family_id
revision
kind
payload_json
digest
created_at
```

지원 `kind`는 `task`, `function`, `workflow`, `participant`, `event`, `ontology`다. 각 kind는 독립 validator를
가지며 `payload_json`은 SQLite `json_valid()`와 application-level schema 검증을
모두 통과해야 한다. `(workspace_id, family_id, revision)`과 digest는 unique다.
mutable Definition row나 current revision pointer는 두지 않는다.

Function spec의 binding은 revision payload에 포함한다. 지원 binding은 allowlist된
`kernel_command`, pinned `connector`, `native`, `composite`다. 모든 성공 결과는
선언된 `output_schema`와 `emits` Domain Event payload schema를 통과해야 한다.

### `journal_records`

최소 필드:

```text
id
workspace_id
stream_id
stream_sequence
kind
spec_revision_id?
participant_kind?
participant_id?
principal_id
correlation_id?
causation_id?
command_id?
payload_json
created_at
```

`(workspace_id, stream_id, stream_sequence)`은 unique다. fact는 update/delete하지
않고 correction도 새 fact로 기록한다. state-changing function의 terminal fact는
가능한 경우 World mutation 및 기존 Kernel 기록과 같은 transaction에서 append한다.

command ledger는 요청과 Kernel outcome의 truth이고 Journal은 semantic work fact의
truth다. 둘은 `command_id`로 연결하며 같은 내용을 서로 대신하지 않는다.

## 4. Work facts algebra

초기 work protocol은 다섯 동작만 가진다.

```text
OPEN     work.opened
ROUTE    work.routed
ATTEMPT  function.requested / function.succeeded / function.failed
AGENT    agent.requested / agent.succeeded / agent.failed
SUBMIT   work.submitted
RESOLVE  work.satisfied / work.cancelled
```

현재 상태는 projection으로 계산한다.

- completed: `work.satisfied`가 존재함
- assignee: 최신 `work.routed`의 target
- active function: terminal fact가 없는 `function.requested`
- active agent: terminal fact가 없는 `agent.requested`
- awaiting review: 최신 관련 fact가 `work.submitted`
- handoff: 이전 route와 다음 `work.routed`의 의미 있는 전환

`work.satisfied`는 pinned Task spec, completion evaluation 결과, proof transaction과
평가한 object version을 포함한다. mutable `TaskRun.status`나
`WorkflowRun.status`는 만들지 않는다.

## 5. 최소 command surface

초기 외부 write surface는 다음으로 제한한다.

```text
spec.publish
work.open
work.route
work.submit
function.invoke
work.cancel     optional
```

Assignment/Handoff/Execution/Submission CRUD와 Run status update API는 만들지
않는다. `work.route`는 업무 배정일 뿐 권한 부여가 아니다. `function.invoke`와
모든 state mutation은 effective Principal의 Kernel authorization을 다시 검증한다.

## 6. 첫 vertical slice

구매 승인 하나로 전체 경계를 검증한다.

```text
TaskSpec purchase-approval@1
    ↓ work.open
work.opened W123
    ↓ work.route
human:manager
    ↓ work.submit
manager approval
    ↓ work.route
automation:auto-approve
    ↓ function.invoke purchase-request.approve@1
kernel_command binding: object.move(approved)
    ↓ existing Kernel transaction
World mutation + ledger/activity/outbox/projection
    ↓
function.succeeded
    ↓ completion expression
work.satisfied(proof transaction, object version)
```

이 slice에 필요한 구현만 한다.

1. `spec_revisions`, `journal_records` DDL과 repository methods
2. Task/Function spec validators
3. 위 다섯 command 중 scenario에 필요한 command handlers
4. `object.move` allowlisted binding
5. journal projection query: current assignee, completion, function outcome
6. SQLite end-to-end test 하나

필수 검증:

- 같은 idempotency key의 invoke가 World를 두 번 변경하지 않는다.
- authorization 실패는 World와 success fact를 남기지 않는다.
- Function 성공 뒤 completion expression이 거짓이면 `work.satisfied`가 없다.
- journal replay가 같은 current work view를 만든다.
- `work.route`만으로 수신자 Principal에게 mutation 권한이 생기지 않는다.

## 7. 다음 단계

### Phase 2 — Conductor leaf Function binding

`connector`와 `native` binding은 구현되었다. Function revision이 binding을 고정하고,
`function.invoke`는 Conductor가 이해하는 handler/version/operation/connection 실행
문맥을 outbox에 기록한다. 같은 transaction에서 `function.requested` Journal fact를
append하지만 외부 실행 결과를 미리 성공으로 처리하지 않는다.

```text
connector → connector.invoke@version
native    → handler_key@version
```

Conductor execution request outbox delivery worker와 authenticated callback ingress는
구현되었다. callback은 workspace와 기존 invocation correlation을 확인하고 append-only
adapter inbound evidence를 저장한 뒤, deterministic한 내부 `function.record_result`
Kernel command로 terminal Journal fact를 기록하고 Task completion을 평가한다. callback
재전송은 같은 command id와 idempotency key로 수렴한다. 외부 실행 outbox와 연결되지
않은 generic/composite invocation callback은 거절하며, `appended` evidence는 재시작 후
background reducer가 복구한다.

Conductor의 built-in `provider=compos`에는 service-token과 workspace header를 검증하는
schema/record read endpoint만 제공한다. connector 경계가 canonical write 우회로가 되지
않도록 create/update/delete와 arbitrary operation endpoint는 열지 않는다.

`composite` binding은 pinned Workflow revision을 참조한다. durable한 부분 실행,
재시작 복구와 완료 계산은 Function 안의 두 번째 graph runtime이 아니라 Workflow
coordinator가 소유한다.

### Phase 3 — Agent participant (implemented)

새 aggregate 없이 `ParticipantRef.kind=agent`와
`SpecRevision(kind=participant)` AgentDefinition을 사용한다. Agent route는 같은
transaction에서 `agent.requested`와 Conductor outbox를 기록한다. authenticated
callback 성공은 `agent.succeeded`, `work.submitted`, completion fact로 환원되고,
최종 실패는 `agent.failed` 뒤 Task의 `on_agent_failed` route/cancel 정책을 적용한다.
별도 mutable AgentRun table은 없다.

### Phase 4 — Workflow coordinator (implemented)

`SpecRevision(kind=workflow)`와 `route`, `invoke`, `branch`, `parallel`
node를 지원한다.
WorkflowRun/StepRun table 없이 pinned spec과 Journal replay로 crash/restart 뒤 동일한
다음 action을 계산한다. deterministic child command와 기존 command ledger가 재실행을
멱등하게 만든다. parallel은 모든 branch를 fan-out하고 terminal 결과를 join한다.
Human/Agent route와 submission은 의미적 `workflow_step_id`와 invocation별
`workflow_route_id`로 결합되어 병렬 route나 같은 Workflow의 반복 실행이 서로의
submission을 소비하지 않는다. publish는 도달 가능한 composite
Function↔Workflow cycle을 거부한다.
input/output mapping은 `$.root`, `$.current`, `$.steps`, `$.output`의 map selector만
지원하며, 외부 lookup이나 비결정적 expression은 허용하지 않는다.

### Phase 5 — ReactiveRule convergence (implemented)

`reactive_rules` storage를 사용하면서 committed projection event만 평가한다.
bounded JsonLogic, closed event template, deterministic command/idempotency,
durable evaluation recovery, recursion/rate/pending limit과 failure-threshold circuit
breaker를 적용하고 모든 action은 Kernel command로 재진입한다.
Trigger는 공개 Kernel Registry 이벤트로 제한하고, v1 action allowlist는
`object.create`, `object.move`, `relation.link` 세 command다.

```text
ReactiveRule → allowlisted Kernel command → Journal
```

### Phase 6 — consumer-driven expansion (first slice implemented)

Domain Event와 Ontology는 immutable spec revision으로 구현되었다. `event.emit`은
Event revision의 payload schema를 검증해 pinned event fact를 기록한다. Function의
`emits`는 Event revision 존재를 검증하고, 성공 output 전체를 payload로 검증해 같은
transaction에서 자동 발행한다. Knowledge feedback은
accepted OKF Concept을 근거로 proposal을 남기고 human review acceptance가 Ontology
revision을 같은 transaction에서 생성한다. 성능 측정 전에는 generated
DDL, dedicated domain table이나 materialized projection을 만들지 않는다.

Knowledge Space와 Ontology grounding은 이제 concrete consumer가 있으므로 별도
[Knowledge Space and Ontology Implementation Plan](knowledge-ontology-implementation.md)의
순서를 따른다. canonical SQL model을 먼저 완성하고 Rhiza Graph는 검증을 통과한
read projection으로만 추가한다. SQLite는 local/test support mode다.

## 8. 명시적으로 만들지 않는 것

- mutable Definition / current revision rows
- FunctionBinding, FunctionRun 전용 table
- TaskRun, WorkItem, Assignment, Handoff, Execution, Submission 전용 table
- WorkflowRun, StepRun 전용 table
- 범용 Policy/Expression registry
- 별도 mutable EventDefinition/Ontology/Knowledge proposal table
- 모든 definition kind를 미리 지원하는 plugin framework
- 범용 message/timer/wait node와 pause/resume/skip UI
- loop, dynamic fan-out, subworkflow와 자동 compensation engine

이 중 하나가 필요해질 때는 먼저 Journal projection으로 해결할 수 없는 독립적
identity, consistency 또는 성능 요구를 제시해야 한다.
