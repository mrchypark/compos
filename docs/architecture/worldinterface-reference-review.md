# WorldInterface 설계 검토와 Compos 적용 경계

상태: 참고 설계 검토. 기존 Compos 헌법과 소유권 경계를 바꾸는 규범 문서가
아니며, 그 경계 안에서 채택할 원칙과 거절할 구현을 기록한다.

검토 기준은 `zed-colonel/worldinterface`의 2026-06-09 커밋
[`f48df82e`](https://github.com/zed-colonel/worldinterface/tree/f48df82e98203fa94c86eecae60425205c3e00e9)이다.
공개 저장소는 릴리스와 태그가 없고 소수 기여자가 만든 초기 단계 프로젝트다.
따라서 Compos 또는 Conductor의 의존성이나 포크 후보가 아니라 설계 참고 자료로만
사용한다.

## 결론

WorldInterface에서 가져올 핵심은 DAG runtime이나 connector 구현이 아니라 다음
일곱 원칙이다.

1. 실행 식별자는 immutable revision과 instance/node identity에서 결정적으로 만든다.
2. 외부 경계의 input, output, status는 immutable evidence와 digest로 남긴다.
3. durable output/evidence를 기록한 뒤에만 step 또는 Task 완료를 확정한다.
4. 다음 실행 가능성은 mutable coordinator state가 아니라 durable facts에서 계산한다.
5. connector effect는 별도 정책 모델 대신 pinned operation으로 표현한다.
6. 동적 코드는 현재의 module allowlist와 resource ceiling 아래 실행한다.
7. ingress 요청은 orchestration보다 먼저 durable evidence로 기록한다.

이 원칙들은 이미 정한 Compos의 `World + immutable SpecRevision + append-only
JournalRecord` 모델에 흡수한다. 별도 `ContextStore`, mutable Run aggregate,
ActionQueue 기반 graph runtime은 추가하지 않는다.

## 관찰한 구조

WorldInterface의 [README](https://github.com/zed-colonel/worldinterface/blob/f48df82e98203fa94c86eecae60425205c3e00e9/README.md)는
FlowSpec을 ActionQueue DAG로 compile하고, coordinator가 queue child state와 SQLite
ContextStore를 읽어 다음 wave를 제출하는 구조를 설명한다.

```text
FlowSpec
  -> compiler
  -> coordinator + connector/transform/branch ActionQueue tasks
  -> ActionQueue WAL
  -> connector registry (native/WASM)
  -> SQLite ContextStore outputs
  -> receipts
```

- FlowSpec validation은 duplicate node, missing endpoint와 cycle을 한 번에 진단한다.
- step/task ID는 flow run과 node에서 UUIDv5로 결정적으로 파생한다.
- output은 `(flow_run_id, node_id)` write-once key로 저장한다.
- connector descriptor는 input/output schema, idempotency, side effect, read/write,
  concurrency safety와 read-before-write를 기술한다.
- receipt는 attempt, input/output SHA-256, status, error와 duration을 기록한다.
- WASM manifest는 HTTP, filesystem, process, environment, socket 등 capability와
  fuel/time/memory 한도를 선언한다.

주요 근거는
[FlowSpec](https://github.com/zed-colonel/worldinterface/tree/f48df82e98203fa94c86eecae60425205c3e00e9/crates/worldinterface-core/src/flowspec),
[deterministic IDs](https://github.com/zed-colonel/worldinterface/blob/f48df82e98203fa94c86eecae60425205c3e00e9/crates/worldinterface-core/src/flowspec/id.rs),
[ContextStore](https://github.com/zed-colonel/worldinterface/tree/f48df82e98203fa94c86eecae60425205c3e00e9/crates/worldinterface-contextstore/src),
[connector contract](https://github.com/zed-colonel/worldinterface/blob/f48df82e98203fa94c86eecae60425205c3e00e9/crates/worldinterface-connector/src/traits.rs),
[receipt](https://github.com/zed-colonel/worldinterface/blob/f48df82e98203fa94c86eecae60425205c3e00e9/crates/worldinterface-core/src/receipt.rs),
[WASM manifest](https://github.com/zed-colonel/worldinterface/blob/f48df82e98203fa94c86eecae60425205c3e00e9/crates/worldinterface-wasm/src/manifest.rs)다.

## Compos 적용 결정

| 주제 | 결정 | Compos/Conductor 적용 |
| --- | --- | --- |
| 결정적 파생 ID | 채택 | Function invocation은 canonical command/execution request ID를 semantic idempotency identity로 사용한다. attempt ID는 fencing과 감사에만 쓴다. |
| output-before-completion | 채택 | inbound callback evidence append, terminal Journal fact, Task completion 평가 순서를 reducer transaction으로 보장한다. |
| durable facts 기반 eligibility | 채택 | `World + pinned SpecRevision + Journal facts` projection으로 계산한다. 별도 coordinator state는 두지 않는다. |
| immutable boundary receipt | 조정 채택 | Compos Journal/inbound evidence와 Conductor attempt evidence에 나눠 보존한다. 양쪽 DB를 공유하지 않는다. |
| connector descriptor | 원칙만 참고 | 새 descriptor 모델을 만들지 않는다. 기존 pinned handler/version/operation/connection이면 충분하다. |
| compile/admission diagnostics | 채택 | Function/Workflow revision publish 시 graph, schema, binding, operation, version과 capability 요구를 모아 검증한다. |
| pure transform/branch | 제한 채택 | Compos가 workflow를 leaf Function/Task로 전개한다. 프로세스 안에서 원자적으로 끝나는 조합만 하나의 pinned native handler로 패키징한다. |
| ingress-first durability | 채택 | webhook/adapter/callback evidence를 append한 뒤 Kernel reducer가 canonical mutation을 수행한다. |
| ActionQueue DAG runtime | 거절 | Conductor에는 leaf ExecutionRequest만 보낸다. 두 번째 workflow/control plane을 만들지 않는다. |
| 별도 ContextStore | 거절 | Compos canonical SQL의 Journal/World projection이 canonical truth다. 두 저장소의 backup/recovery 결합을 만들지 않는다. |
| connector name hot-load | 거절 | mutable name이 아니라 `handler_key@version`과 immutable artifact digest를 pin한다. |
| marker-file exactly-once | 거절 | side effect와 marker 사이 crash window가 있다. provider idempotency key, fencing, reducer dedupe로 처리한다. |
| unauthenticated invoke/webhook | 거절 | service token, workspace scope, operation/connection scope를 trust boundary에서 검증한다. |
| broad shell/filesystem connector | 거절 | Compos Kernel에는 넣지 않는다. 필요한 runtime effect는 Conductor의 격리·capability policy 아래 둔다. |

## Compos 문서 계약에 반영할 요구사항

### P0: 완료와 evidence의 원자성

`function.record_result` reducer는 한 canonical SQL transaction에서 다음을 보장해야 한다.

1. callback이 원래 `function.requested`와 같은 workspace, execution request,
   Function revision과 pinned binding을 가리키는지 검증한다.
2. terminal `function.succeeded|failed` fact를 append한다.
3. 같은 correlation의 terminal fact가 이미 있으면 동일 결과는 성공으로 수렴하고
   다른 결과는 conflict로 거절한다.
4. Task completion expression을 terminal fact와 output contract에 대해 평가한다.
5. Journal append나 completion projection 중 하나라도 실패하면 전부 rollback한다.

Conductor가 output을 가졌다는 사실만으로 Task를 완료하지 않는다. Compos가 callback
evidence를 durable하게 받아 reducer가 terminal fact를 확정한 뒤에만 완료한다.

### P0: semantic identity와 audit identity 분리

WorldInterface receipt ID는 timestamp와 duration을 포함한 content hash이므로 재시도
dedupe key로 사용할 수 없다. Compos는 다음 identity를 구분한다.

- semantic idempotency: `command_id` / `execution_request_id`
- immutable spec identity: Function revision + binding revision
- execution fencing: attempt/lease/fencing token
- evidence content digest: canonical input/output/error payload의 digest
- audit metadata: observed time, duration, provider request ID

timestamp나 duration은 semantic key에 포함하지 않는다. 원문 payload에 secret이 있을
수 있으므로 digest와 최소 redacted evidence를 canonical 기록으로 삼고, 원문 보존은
별도 retention/encryption 정책이 있을 때만 허용한다.

### P0: 최소 leaf 안전 계약

새 effect enum, capability registry 또는 policy table을 만들지 않는다. immutable
Function binding과 ExecutionRequest의 기존 필드만 사용한다.

- `handler_key@version`
- connector이면 `provider + operation + connection_id`
- timeout/retry policy
- workspace와 idempotency key

Conductor는 input 안의 operation으로 pinned operation을 덮어쓰지 않는다. 두 값이
함께 오면 반드시 같아야 한다. `read/list/search`만 현재 retry policy를 사용할 수
있고 나머지 connector operation은 provider idempotency가 별도로 증명되기 전까지
실행 예산을 1회로 고정한다. lease recovery도 같은 retry budget을 적용한다. mutation
worker가 원격 성공 뒤 죽어 결과를 확정할 수 없으면 재실행하지 않고
`outcome_unknown` terminal failure로 callback하여 상위 reducer가 reconciliation을
선택하게 한다. 이것으로 effect 분류와 mutation safety를 해결하며 별도 capability
언어를 추가하지 않는다.

Kavun handler는 이미 source/bytecode hash, runtime ABI, module allowlist, frame/stack과
timeout을 pin한다. 실제로 부족하다는 측정이나 exploit evidence가 생기기 전에는 다른
resource policy 계층을 추가하지 않는다.

### P1: 결정적 workflow identity

Workflow가 도입될 때 `workflow_revision_id + workflow_instance_id + node_id`에서
leaf invocation identity를 결정적으로 파생한다. 재시작이나 reducer replay가 같은
leaf를 다시 만들더라도 같은 execution request에 수렴해야 한다. branch exclusion도
mutable flag가 아니라 immutable fact로 표현한다.

### P1: admission diagnostics

immutable revision publish 전에 다음 오류를 한 번에 반환한다.

- duplicate node와 cycle
- missing node/edge target
- incompatible input/output schema
- unknown Function revision, handler, handler version 또는 connector operation
- missing connection/workspace scope
- input operation이 pinned operation과 다른 경우

runtime에 도달한 뒤 처음 발견할 수밖에 없는 provider 오류와 publish 시 검증 가능한
구조 오류를 구분한다.

### P1: ingress evidence 최소화

adapter/webhook은 요청을 orchestration보다 먼저 append하지만 raw headers 전체를
저장하지 않는다. 인증 header, cookie, token은 allowlist 밖에서 제거하고 필요한
provider event ID, workspace, subject, canonical body digest와 검증 결과만 보존한다.

## Conductor 경계 요구사항

Conductor는 Compos가 해석한 leaf ExecutionRequest만 실행한다.

- native: registered `handler_key@version`
- connector: pinned `connector.invoke` handler version + provider/operation/connection/input
- dynamic: pinned source/artifact digests + runtime ABI + capability/resource policy

Conductor가 소유할 것은 handler registry, connector provider, lease/retry/fencing,
cancellation, timeout, sandbox/capability enforcement와 attempt evidence다. Compos canonical SQL을
직접 수정하거나 workflow eligibility를 판단하지 않는다. terminal callback은
`workspace_id`, `execution_request_id`, pinned handler/version, terminal status,
canonical result/error digest와 fencing에 필요한 attempt evidence를 포함해야 한다.

## 그대로 가져오면 안 되는 구현상 위험

검토한 커밋에는 좋은 문서 원칙과 실제 enforcement 사이에 다음 간극이 있다.

- ActionQueue WAL과 SQLite ContextStore가 모두 복구되어야 하므로 split-brain과 backup
  일관성 표면이 커진다.
- step output 저장 뒤 receipt 저장이 best-effort인 경로가 있어 “모든 boundary crossing은
  receipt를 남긴다”는 불변식이 fail closed가 아니다.
- filesystem write 후 marker를 쓰므로 crash가 append 중복을 만들 수 있다.
- HTTP connector는 임의 URL을 허용하고 4xx/5xx를 connector 성공 결과로 반환한다.
- daemon invoke/flow/webhook route에 공통 인증 경계가 보이지 않는다.
- shell connector와 이른바 sandbox connector는 OS/container 격리를 제공하지 않는다.
- WASM loader는 mutable name으로 module을 hot-load하며 signature, artifact digest와
  pinned version을 강제하지 않는다.
- manifest의 memory 및 HTTP concurrency 한도는 실제 invoke path에서 enforcement가
  확인되지 않았다.
- suspended coordinator를 매 tick 재개하는 방식은 event-driven wakeup보다 polling과
  중복 판단 비용이 크다.
- webhook request headers 전체 보존은 credential/PII retention 위험을 만든다.

이는 WorldInterface가 쓸모없다는 뜻이 아니라, 문서의 invariant를 이름만 복사하지
말고 crash window와 trust boundary까지 테스트로 증명해야 한다는 뜻이다.

## 검증 메모

공개 커밋에서 `cargo test --workspace`를 실행했다. connector suite에서 261개가
통과하고 16개가 실패했다. 실패 중 14개는 macOS의 `/var`와 `/private/var` 경로
별칭을 동일시하지 않은 portability 문제였고, 2개는 `python` 실행 파일이 항상
존재한다고 가정한 문제였다. 설계 참고에는 영향을 주지 않지만 dependency/fork로
채택하기 전에 portability와 hermetic test contract가 필요하다는 추가 근거다.

## 채택하지 않은 것

현재 Compos에는 이 검토 때문에 새 table, runtime, connector, dependency를 추가하지
않는다. 기존 헌법이 이미 같은 원칙을 더 작은 canonical model로 표현한다. 후속
구현은 위 P0/P1 중 현행 계약에 없는 항목이 실제 사용자 시나리오에서 필요하고,
Compos-Conductor contract test로 검증할 수 있을 때만 추가한다.
