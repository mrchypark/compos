# Compos Constitutional Architecture

Status: Accepted

이 문서는 Compos의 최상위 개념 모델과 설계 원칙을 정의한다. 제품 용어,
하위 명세, API, 저장소와 런타임 구현은 이 문서의 의미를 구체화해야 한다.
현재 구현이 이 문서의 전체 범위를 제공한다는 뜻은 아니다. 구현된 범위와
계획된 범위는 명세와 구현 계획에서 구분한다.

## 1. 정의

Compos는 조직의 지식과 업무 세계를 Ontology와 Dynamic Data로 표현하고,
그 위에서 Human, Agent, Automation이 같은 Task와 Capability를 공유하며
협업하도록 하는 실행 플랫폼이다.

핵심 명제는 다음과 같다.

> 모든 일은 같은 업무 세계 위에서 발생하며 Human, Agent, Automation은
> 같은 Task와 같은 데이터를 대상으로 협업한다.

> 업무의 수행 방식은 고정되어 있지 않으며 Human, Agent, Automation 사이를
> 이동할 수 있다.

Compos가 다루는 순환은 다음과 같다.

```text
Knowledge
    ↓
Meaning
    ↓
State
    ↓
Intent
    ↓
Work
    ↓
Execution
    ↓
State Change
    ↓
Knowledge
```

## 2. 개념 공간과 canonical model

```text
External Knowledge Space (owned outside Compos)
    Note / Document / Claim / Source / Evidence / Proposal

Semantic World
    OntologyConcept / OntologyRelation / EventDefinition

Operational World
    EntityDefinition / RelationDefinition / EntityInstance / EntityRelation

Intent & Work
    TaskDefinition / TaskRun / WorkItem / Assignment / Delegation / Handoff
    WorkflowDefinition / WorkflowRun / StepRun

Capability
    FunctionDefinition / FunctionRevision / FunctionBinding
    AgentDefinition / AssignmentPolicy / Policy / Expression

Execution
    WorkExecution / AgentRun / AutomationExecution / FunctionRun / Submission
    Event / Audit
```

이 공간 아래에는 제품 의미를 소유하지 않는 Kernel이 있다.

```text
Kernel
    Command / Transaction / Identity / Principal / Authorization
    Idempotency / Mutation / Projection / Outbox / Scheduler / Dispatcher
```

여섯 공간은 제품과 설계를 설명하는 vocabulary다. 각각을 별도 persistence
aggregate나 table로 만들지 않는다. Compos의 canonical model은 세 종류뿐이다.

```text
World
    현재 업무 세계의 canonical state
    기존 EntityInstance / relation storage

Spec
    재현되어야 하는 immutable typed revision
    Entity / Task / Function / Workflow / Participant

Journal
    업무, 배정, 위임, 실행, 제출, 완료의 append-only facts
```

모든 runtime view는 다음 식의 projection이다.

```text
Current Work View = World + pinned Spec revisions + Journal facts
```

`TaskRun`, `WorkItem`, `Assignment`, `Handoff`, `Execution`, `Submission`,
`FunctionRun`, `WorkflowRun`과 `StepRun`은 중요한 도메인 개념이지만 기본적으로
별도 mutable aggregate가 아니다. Spec과 Journal stream에서 계산되는 이름 있는
projection이다. 독립적인 consistency identity가 실제로 필요하다는 증거가 생길
때만 materialize한다.

## 3. External Knowledge ownership

Knowledge content, extraction, proposals, review and lifecycle belong to an
external system. Compos consumes meaning through immutable Ontology specs and
Task ontology context; it does not implement a Knowledge Space or its graph.
Operational execution evidence remains in the Compos Journal.

## 4. Ontology와 Operational World

Ontology는 의미 계약이고 EntityDefinition은 운영 계약이다.

```text
OntologyConcept
        ↓ grounding
EntityDefinition
        ↓ instantiate
EntityInstance
```

Ontology는 Concept, property semantics, relation semantics, classification과
도메인 invariant를 표현한다. EntityDefinition은 필드, 관계, 제약, 색인,
표현과 저장 매핑을 정의한다.

둘을 1:1로 강제하지 않는다. 하나의 Concept이 여러 EntityDefinition에
grounding될 수 있고, 하나의 EntityDefinition이 여러 Concept을 조합할 수
있다. 따라서 grounding은 단일 `ontology_ref` 컬럼이 아니라 revision 가능한
first-class mapping으로 본다.

EntityDefinition은 다음 두 종류를 가진다.

- Domain Entity: Customer, Contract, Person, Project처럼 Ontology grounding이
  권장되는 업무 세계의 대상
- System Entity: WebhookDelivery, ImportBatch, ExecutionLock처럼 grounding이
  필수가 아닌 플랫폼 운영 대상

현재 구현의 `object_types`, schema revision과 `object_instances`는 각각
EntityDefinition, EntityRevision과 EntityInstance의 operational 표현이다.
코드와 저장소 이름을 즉시 바꾸지는 않는다.

## 5. Task와 Work

Task는 특정 수행자에게 종속된 레코드가 아니라 달성해야 할 Goal 또는
Desired State의 계약이다.

```text
Current State
     ↓
Task
     ↓
Desired State
```

재사용 가능한 의미 계약과 실제 업무를 분리한다.

```text
TaskDefinition
        ↓ instantiate
TaskRun
        ↓ decompose
WorkItem
```

Task spec은 goal, input schema, output contract, constraints, completion
expression, capability policy와 ontology context를 가진다. TaskRun은 특정 Task
revision에 pin된 work journal stream이다. 별도 mutable `status`를 소유하지 않는다.

WorkItem은 runtime 협업 단위다. 별도 WorkItemDefinition이나 기본 table을
만들지 않는다. 필요하면 parent/child work stream으로 표현한다.

업무 책임과 수행은 구분한다.

- Owner: 최종 책임
- Assignee: 현재 업무를 맡은 Participant
- Executor: 특정 실행을 실제 수행한 Participant
- Reviewer: 결과를 검토하거나 승인하는 Participant

Assignment는 latest `work.routed` fact의 projection이다. Delegation과
Responsibility Transfer도 목적과 범위가 기록된 routing fact로 표현한다.

Handoff는 단순 assignee 변경이 아니다. 전달자, 수신자, 이유, 완료된 내용,
남은 내용, 기대 결과, 기한과 return target을 보존하는 first-class journal fact다.

## 6. Participant와 보안 Actor

업무 배정 관점의 Participant는 다음과 같다.

```text
Participant
├─ Human
├─ Agent
└─ Automation
```

`code`는 Participant가 아니라 Function 또는 Runtime의 구현 수단이다.

Participant와 Principal/Actor는 동일하지 않다.

```text
Participant
    = 업무 배정과 책임의 대상

Principal / Actor
    = 인증, 권한과 Kernel 실행의 보안 주체
```

예를 들어 `automation:invoice-reconciliation@3`가 Participant여도 실제
Kernel command의 effective actor는 별도 ServicePrincipal일 수 있다.
requested actor와 effective actor는 감사 기록에서 모두 보존한다.

## 7. Capability와 Function

Function은 수행자와 무관한 논리적 capability contract다.

```text
FunctionDefinition
├─ input schema
├─ output schema
├─ side-effect contract
├─ error contract
├─ audit contract
└─ emitted domain events
```

별도 ActionType abstraction을 만들지 않는다. 업무 의미가 있는 Action은 Domain
FunctionDefinition으로 표현한다.

- Domain Function: `purchase_request.approve`, `customer.merge`,
  `contract.renew`
- System Function: `records.update`, `object.move`, `http.request`,
  `email.send`

Function의 의미와 구현은 분리하되 초기 구현에서 binding table은 만들지 않는다.

```text
FunctionDefinition
        ↓ revision
FunctionRevision
        ↓ immutable embedded binding
Binding
        ├─ kernel_command
        ├─ connector
        ├─ native
        ├─ composite
        └─ workflow
```

예를 들어 `purchase_request.approve`는 runtime-defined 의미 계약으로 유지하고,
그 binding만 `object.move`, Workflow 또는 native implementation으로 교체할 수
있다. interpreted implementation을 native code로 최적화해도 Function identity와
revision contract는 바뀌지 않는다.

Function은 실제 capability이고 Tool은 Agent에게 Function을 노출하는 interface다.
동일한 Function은 Human에게 UI Action, Agent에게 Tool, Automation에게 실행
Step으로 노출될 수 있다.

## 8. Authorization

Semantic capability authorization과 Kernel authorization을 분리한다.

```text
Task Capability Policy
∩ Participant Permission
∩ Environment Policy
= Effective Semantic Capabilities

Effective Principal Capability
∩ Kernel Command Policy
= Allowed Kernel Mutation
```

현재 `actor_capabilities`는 두 번째 Kernel security boundary에 남긴다. 상위
Policy가 Kernel 보안을 대체하지 않는다.

## 9. Execution

Assignment는 누가 맡았는지, WorkExecution은 누가 특정 WorkItem에서 실제로
무엇을 했는지를 뜻한다. 하나의 WorkItem에는 여러 WorkExecution이 발생할 수
있고 실행 방식도 전환될 수 있다.

```text
Automation
    ↓ exception
Agent
    ↓ uncertainty
Human
```

```text
Human
    ↓ repeated pattern
Agent
    ↓ stable pattern
Automation
```

WorkExecution은 participant, runtime, definition revision pins, 시작/종료 시각,
inputs, actions, outputs, result, error와 audit references를 보존한다.

`Execution`은 architectural space와 공통 audit vocabulary다. 모든 Run을
포괄하는 generic Execution super-table이나 `work_executions` table을 만들지
않는다. Work 수행 시도, FunctionRun, WorkflowRun과 AgentRun은 각각 관련 journal
facts를 묶은 projection으로 lifecycle을 표현한다.

FunctionRun은 `function.requested`와 같은 correlation id의 terminal fact를 묶은
projection이다. pinned Function spec과 binding, requested/effective principal을
보존한다. Function call 하나를 Task로 승격하지 않는다.

Submission은 Execution 과정과 구분되는 immutable `work.submitted` fact다.
completion expression이 output contract, evidence와 World state를 평가해 만족하면
proof transaction과 object version을 포함한 `work.satisfied` fact를 기록한다.

## 10. Human, Agent와 Automation Runtime

세 Runtime은 같은 Task, World State와 Capability를 공유하지만 판단 방식이
다르다.

- Human Runtime: assignment, inbox/UI, context, submission과 completion
  validation을 관리하는 durable state machine
- Agent Runtime: context 구성, planning, reasoning, Tool 선택, iteration,
  evaluation과 escalation을 수행
- Automation Runtime: 미리 정의된 executable을 idempotency, retry, timeout과
  함께 수행

AgentDefinition은 instructions, model policy, context policy, tools,
permissions, memory policy와 execution policy를 가진다. AgentRun은 실제 planning,
tool calls, observations, intermediate state와 output을 보존한다.

Automation은 actor 구현체 이름이나 Code Task가 아니라 execution mode다. 실행
대상은 Function, Workflow, Script, Pipeline 또는 State Machine일 수 있다.

현재 `reactive_rules`는 이 넓은 Automation Runtime과 동일하지 않다. 해당
모델은 다음 의미의 `ReactiveRule`이다.

```text
WHEN event
IF predicate
THEN invoke Function / Workflow / Task
```

저장소와 wire name은 호환성 기간 동안 유지할 수 있지만 새 제품 문서에서는
ReactiveRule로 부른다.

## 11. Workflow

Workflow는 단순 Function DAG가 아니라 순서, 조건, 분기, 병렬 처리, 대기,
이벤트, 승인, 재시도와 완료를 관리하는 durable coordination model이다.

```text
WorkflowStep
├─ Task Step
├─ Function Step
├─ Condition
├─ Parallel
├─ Wait
├─ Event
└─ Subworkflow
```

Workflow를 모든 업무의 최상위 primitive로 만들지 않는다.

```text
TaskRun
├─ Ad-hoc Work
└─ Workflow-managed Work
```

WorkflowDefinition은 특정 executor보다 TaskDefinition과 AssignmentPolicy를
참조하는 것을 기본으로 한다. 반복되는 ad-hoc Work와 Handoff 패턴은 검토를
거쳐 WorkflowDefinition으로 승격될 수 있다.

Workflow spec은 immutable coordination definition이다. node는 `route`, `invoke`,
`branch`, `parallel`을 지원한다. Human/Agent `route`는 해당 route 이후의
`work.submitted`까지 열려 있고 Function `invoke`는 terminal Function fact까지
기다린다. `invoke`와 container node는
`$.root`, `$.current`, `$.steps`, `$.output` selector로 input/output mapping을
고정할 수 있다. WorkflowRun과 StepRun table은 만들지 않는다. Orchestrator는
pinned workflow spec과 journal replay로 다음 action을 결정하고, parallel branch를
모두 fan-out한 뒤 terminal 결과를 join하며 실제 수행은 Human, Agent 또는
Automation Runtime에 dispatch한다.

## 12. Event, Expression과 Policy

Kernel Event와 Domain Event를 구분한다.

- Kernel Event: `object.created`, `object.updated`, `object.state_changed`처럼
  committed mutation을 정확히 기술하는 infrastructure event
- Domain Event spec: `PurchaseRequestApproved`, `CustomerEnriched`처럼 구체적인
  consumer가 요구할 때만 추가하는 runtime-defined semantic event contract
- Event Instance: 특정 EventRevision에 pin된 실제 발생

Domain Function은 발생 가능한 Domain Event revision을 `emits` contract로 선언한다.
Function 성공 output 전체가 각 Event payload schema를 통과하면 같은 transaction의
`domain.event.emitted` fact로 자동 기록된다. schema 불일치는 성공으로 기록하지 않는다.
Kernel Event를 semantic event로 projection할 수도 있다.

Task completion, Function precondition, AssignmentPolicy, Workflow condition,
trigger와 UI condition은 공통 typed Expression AST와 evaluator core를 사용할
수 있다. BusinessExpression profile은 업무 데이터와 deterministic business
operator만 허용한다.

AuthorizationPolicy는 같은 evaluator core를 재사용할 수 있어도 별도 보안
profile이다. 허용 symbol/function을 더 좁게 고정하고, connector/runtime lookup을
금지하며, unknown/error/timeout에는 항상 fail-closed한다. 범용 Policy 저장
abstraction을 먼저 만들지 않고 각 DefinitionRevision이 필요한 expression을
직접 pin하는 것으로 시작한다.

## 13. Spec revision과 runtime projection

플랫폼 전체에 다음 규칙을 적용한다.

```text
Definition identity = family_id
SpecRevision        = immutable typed semantic/executable snapshot
Run                 = revision에 pin된 journal stream projection
```

| Concept | Spec kind | Runtime projection |
| --- | --- | --- |
| EntityDefinition | entity | EntityInstance |
| FunctionDefinition | function | FunctionRun |
| TaskDefinition | task | TaskRun |
| WorkflowDefinition | workflow | WorkflowRun |
| AgentDefinition | participant | AgentRun |
| EventDefinition | event, 필요할 때만 | Event Instance |

`SpecRevision`은 무제한 범용 JSON registry가 아니다. `kind`는 bounded enum이고
각 kind는 등록 시점에 독립된 강한 validator를 통과한다. mutable Definition row나
`current_revision` pointer는 두지 않고 `(family_id, revision)`과 projection으로
identity와 최신 revision을 표현한다. expression은 별도 registry가 아니라 이를
사용하는 spec payload에 immutable typed value로 포함한다.

## 14. Kernel boundary

Kernel은 hard-coded execution primitive와 canonical write boundary만 소유한다.

```text
Hard-coded Kernel
    Identity / Revision mechanics / Transaction
    Command / Idempotency / Authorization
    Object mutation / Relation mutation
    Event persistence / Outbox / Projection
    Dispatch boundary

Immutable Specs
    Entity / Function / Task / Workflow / Participant
    Event / Ontology는 concrete consumer가 있을 때만

Immutable Journal
    Work / Route / Attempt / Submission / Completion / Coordination facts
```

Kernel command는 state change primitive이고 Domain Function은 semantic
capability다. 모든 canonical mutation은 기존과 동일하게 Kernel command,
transaction, ledger, activity, outbox와 projection boundary를 통과한다.

## 15. Storage and physicalization

Ontology 변경은 SQL migration이 아니다. EntityDefinition 변경도 반드시 DDL
migration일 필요는 없다.

현재 generic `object_instances.payload_json`과 typed scalar index를 logical store로
유지한다. 구체적인 성능 요구가 생길 때만 storage mapping을 physicalize한다.

```text
EntityDefinition
    ↓ Storage Mapping
    ├─ generic_json
    ├─ dedicated_table
    ├─ materialized_projection
    ├─ external
    └─ computed
```

생성 DDL은 physicalization 전략이지 Ontology나 Entity model의 본질이 아니다.

## 16. Integrated execution example

```text
Ontology
    PurchaseRequest / Employee

        ↓ grounding

Operational World
    EntityDefinition: purchase_requests
    EntityInstance: PR #123, state=review

        ↓ intent

Task spec: approve_purchase_request@3
Journal stream: W900
work.opened → work.routed(human:finance-manager)

        ↓ capability

FunctionDefinition: purchase_request.approve@2

        ↓ execution

work.submitted(manager-review=approved)
function.requested #922

        ↓ binding

Kernel command: object.move(to=approved)

        ↓ atomic transaction

entity mutation + command ledger + activity + projection + outbox

        ↓ events

Kernel Event: object.state_changed
function.succeeded #922
work.satisfied(task=approve_purchase_request@3, proof=transaction)

        ↓

next Work/Workflow activation + Audit + Knowledge evidence
```

Agent나 Automation이 수행해도 Task, WorkItem, Function과 World State는 같고
Execution runtime만 달라진다.

## 17. Compatibility vocabulary

| 기존 구현/논의 | Constitutional model |
| --- | --- |
| ObjectType | EntityDefinition의 현재 operational 표현 |
| ObjectInstance | EntityInstance |
| ActionType | Domain FunctionDefinition으로 흡수 |
| ActionBinding | Function spec의 immutable binding |
| Command | Kernel execution primitive |
| reactive_rule | ReactiveRule의 현재 저장/wire 표현 |
| Domain Event | journal kind, concrete consumer가 있으면 Event spec |
| Kernel Event | committed mutation event |
| Rule predicate | spec에 포함된 typed expression |
| Execution | journal facts의 projection |
| Actor capability | Kernel authorization |

이전의 특수 `task.create`와 `task_submission.*` command/table 모델은 되살리지
않는다. 새 Work 모델은 immutable Task spec과 journal facts로 구현한다.

## 18. Constitutional principles

1. World와 Work를 분리한다.
2. Task와 Executor를 분리한다.
3. Human, Agent, Automation을 같은 Work model에 둔다.
4. Code를 Actor로 만들지 않는다.
5. Function과 Agent를 분리한다.
6. Function과 Tool을 분리한다.
7. Workflow를 모든 Work의 최상위 primitive로 만들지 않는다.
8. Workflow는 반복되는 Work/Handoff의 구조화된 표현이다.
9. Execution은 TaskRun과 별도로 기록한다.
10. Semantic capability policy와 Kernel authorization을 분리한다.
11. Ontology와 Entity schema를 동일시하지 않는다.
12. 모든 System Entity에 Ontology grounding을 강요하지 않는다.
13. Human과 Agent는 같은 World model을 본다.
14. Automation → Agent → Human escalation과 Human → Agent → Automation
    stabilization을 모두 지원한다.
15. Definition identity, immutable revision과 runtime revision pin을 보존한다.
16. canonical state mutation은 오직 Kernel command boundary를 통과한다.
17. Run이나 Assignment의 존재는 mutation authority를 부여하지 않는다. 모든
    mutation은 effective Principal/Actor의 Kernel authorization을 다시 검증한다.
18. canonical persistence는 World, immutable Spec revisions와 immutable Journal
    facts로 제한한다.
19. runtime status는 저장하지 않고 journal projection으로 계산한다.
