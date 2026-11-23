# Compos 현재 구현 역명세

- 기준일: **2026-10-31**
- 상태: **6개 장이 모두 작성된 소스 기반 통합 초안 / 제한적 교차 검토 반영**. 02·04장 교차 검토와 05장 coverage 점검을 마쳤으며, 최종 승인·라이브 검증 상태가 아니다.
- 방법: EGM 문서 작성 방식을 현재 구현 설명에 적용했다. 사용자 승인에 따라 전체 범위를 장으로 나누어 작성한다. 새 아키텍처·가상 대안·구현 변경 계획을 만들지 않는다.
- 증거: 현재 작업 트리의 코드·DDL·테스트 소스. 문서 작성 중 테스트나 배포를 실행하지 않는다. 테스트 소스의 존재와 테스트 통과를 구분한다. 정식 격리 Goldfish 리뷰를 수행했다는 주장은 하지 않는다.

Compos의 HTTP 동작에는 authentication mode가 없다. 기본이자 유일한 계약은 인증 없는 요청 수락이며 모든 요청은 고정 principal `local`로 기록된다. Compos는 caller identity를 받지 않는다. 보호가 필요한 배포는 신뢰된 네트워크 경계에서 접근을 제한하고 caller를 인증해야 한다. `local`은 attribution과 idempotency scope에만 쓰이며 권한을 부여하지 않는다. 자체 계정·세션·membership·capability·authorization policy와 principal–Person auth binding은 없다. greenfield schema identity는 `compos-go-sqlite-authless-v7`이며 구형 DB를 자동 변환하지 않는다. [05장](05-storage-deployment.md).

## 읽는 순서와 coverage

| 장 | 담당 계약·소스 영역 | 이 목차 작성 시 검토 상태 |
|---|---|---|
| [00 실행 구조](00-overview.md) | runtime, Kernel envelope, idempotency, ledger/effect; cmd/server, internal/kernel, internal/store | 소스 대조·범위 내 교차 검토 완료, 지적 사항 반영 |
| [01 객체·관계](01-objects-relations.md) | dynamic types, schema revisions, fields, states, generated values, relations | 소스 기반 초안 작성, 장 간 인터페이스 peer review 수행 |
| [02 정의·Work](02-definitions-work.md) | spec kinds, Task/Work, submit changes, Function/Agent/Workflow, Journal, Event | 교차 검토 완료. callback result의 failed/cancelled null 허용 수정이 본문에 반영된 것을 확인 |
| [04 자동화·연동](04-automation-integrations.md) | ReactiveRule, expression, scheduler, adapter, Conductor outbox/callback | 교차 검토 완료. 실패 정책 route의 human-only 제약 반영 |
| [05 저장](05-storage-deployment.md) | SQLite/Rhiza, v7 identity·구형 DB 거부, 22개 테이블, 업무 import/export | 새 DDL 22개 정적 대조. 라이브 검증 없음 |
| [06 API·검색](06-api-search.md) | HTTP route/registry inventory, typed pagination, search-syncer와 검색 속성 | 소스 대조 |

00·01·02·04·05·06장의 파일과 목적·경계·연결 장·테스트 근거 및 공백 절을 확인했다. 위 peer review는 장 사이의 계약을 대조한 제한적 검토이며 전체 의미 검증, 정식 EGM Goldfish 리뷰, 내용 승인 또는 테스트 통과를 뜻하지 않는다. 오래된 README/spec 설명과 충돌하면 현재 코드를 우선하고 차이를 기록한다.

## 주요 사용자 여정(CUJ)와 검증 지점

| 여정 | 시작 → 결과 | 주 담당 장·소스 |
|---|---|---|
| workspace 준비 | HTTP 요청의 고정 `local` attribution → 빈 도메인 모델로 workspace 생성 | 05 / [principal](../../internal/server/server.go#L1038), [provision](../../internal/server/server.go#L122) |
| 동적 업무 데이터 구성 | 객체형 정의 → 객체 생성 → 값 교체/상태 이동 → 관계 link/unlink | 01, 06 / [registry](../../internal/kernel/registry.go#L40), [command surface](../../internal/server/server.go#L391) |
| 업무 규칙 발행·실행 | Task 등 spec 발행 → pinned Work open → route → submit/invoke → 완료/취소 | 02, 04 / [Work](../../internal/kernel/work.go#L918) |
| 일괄 업무 변경 | work.submit.changes와 client refs → 원자적 변경·생성 ID 반환 | 02, 05 / [work changes](../../internal/kernel/work_changes.go#L1) |
| 자동화와 외부 실행 | 커밋 이벤트 → 규칙 평가/Workflow → Kernel intent → outbox → callback 증거·결과 | 04 / [rules](../../internal/server/reactive_rule.go#L27), [Conductor](../../internal/server/conductor.go#L1) |
| 검색과 재구축 | SQL snapshot/event → Meilisearch → workspace 제한·객체 결과만 반환 | 06 / [sync](../../cmd/search-syncer/main.go#L140), [search](../../internal/server/search.go#L28) |
| 한국어 제목 검색 | title → 초성·자모·혼합 검색 표현 생성 → 등록된 searchable 속성으로 검색 | 06 / [정규화](../../internal/korean/korean.go#L138), [생성 필드](../../cmd/search-syncer/main.go#L460), [검색 속성](../../cmd/search-syncer/main.go#L249) |
| 새 DB·재시작 | 새 v7 DDL 초기화 또는 동일 identity 재개방; 구형 DB 거부 | 05 / [identity](../../internal/store/store.go#L20) |

## 모든 장에 적용하는 구분

1. **Task와 Work:** Task는 불변 정의, Work는 그 revision을 고정한 실행 stream이다. 일반 동적 객체와 같은 저장 모델이라고 설명하지 않는다.
2. **조회와 실행 판단:** Work 상세는 Journal과 spec을 읽지만 `object_state_equals` 완료는 현재 객체 상태를 읽는다.
3. **규칙과 주체:** constraints 문자열, Function 허용 목록, principal attribution, actor attribution, Work participant 검사는 서로 다르다. Compos에는 authentication mode나 authorization policy가 없다.
4. **정본과 파생물:** object payload·spec·Journal·서버 command ledger와 검색 projection을 구분한다. 모든 보조 테이블을 cache로 부르지 않는다.
5. **현재와 제안:** `definition_revisions`로 이름 변경, Task의 object_instances 이전은 미승인 제안이며 현재 계약에 포함하지 않는다.
6. **제거와 이력:** 현재 Knowledge 런타임은 제거되었지만 공유 Journal/spec의 과거 데이터와 일반 Ontology 기능까지 제거된 것으로 해석하지 않는다.

## 검토 기록과 남은 일

현재 테이블은 22개다. HTTP 요청은 credential 없이 route로 전달되고 항상 `local`을 attribution과 idempotency에 사용한다. 새 workspace의 도메인 모델은 비어 있다. 멤버십·capability 판정은 없다.

6개 장이 작성되었다는 사실만으로 실제 IdP·Conductor·검색 엔진·다중 노드 저장소의 운영 동작을 입증하지 않는다. 이번 문서 작성에서는 테스트·빌드·서버·배포를 실행하지 않았다. 알려진 구현 한계는 해당 장에 기록하고, 미확인 사항은 가정으로 채우지 않는다. 새 아키텍처 결정은 추가하지 않았다.
