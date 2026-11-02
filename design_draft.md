# Compos 현재 구현 역명세와 외부 authority 전환

- 기준일: 2026-10-31. 사용자 승인으로 EGM 설계 전용 흐름을 종료하고 greenfield 구현 결과를 문서에 반영했다.
- 증거: 현재 Compos 소스·DDL·테스트 정적 열람. 문서 담당자는 테스트·빌드·서버·네트워크·배포·라이브 데이터 변경을 실행하지 않았다.
- 상태: 소스 기반 통합 초안. authority seam은 구현되어 있으나 Goauthy/Grant network adapter 및 서비스 인가 통합은 미구현이다. 정식 Goldfish 리뷰·라이브 통합 검증 완료가 아니다.

## 승인된 책임 경계와 현재 코드

계정·인증은 **Goauthy**, 인가는 **Grant**가 소유한다. Compos는 자체 계정·세션·membership·capability를 소유하지 않는다. `server.NewWithAuthority`에 신뢰된 Authority를 주입하고 Kernel의 `AuthorizeCommand`에서 전체 명령을 승인받는 경계가 있다. `server.New`와 현재 서버 실행 진입점은 nil authority이므로 정확한 GET health 외 요청을 거부한다. 임의 principal 헤더나 dev fallback은 없다. [HTTP 경계](internal/server/authority.go#L11), [Kernel 경계](internal/kernel/authority.go#L10), [생성자](internal/server/server.go#L44)

현재 스키마는 **22개 애플리케이션 테이블**, identity는 `compos-go-sqlite-external-authority-v5`다. 기존 28개 중 membership, provider identity, session, actor capability, principal–Person binding, claim의 6개 테이블을 제거했다. 구형 DB는 identity 불일치로 거부하며 인증 호환 마이그레이션이나 자동 삭제·이관 경로는 없다. 기존 라이브 DB는 변경하지 않았다. legacy export/import 도구는 21개 업무 테이블을 다루며 인증 상태 이관이 아니다. [DDL](internal/store/schema.sql#L1), [identity](internal/store/store.go#L19), [import 목록](internal/store/logical_import.go#L69)

workspace·Person·Work participant, 명령 ledger와 감사 actor, 도메인 상태·스키마 검증은 유지한다. membership/capability가 아니라 외부 승인을 검사하며 **duplicate receipt 조회 전에도 승인**을 요구한다. service/automation은 actor 형식으로 표현할 수 있어도 외부 workload/delegation 계약이 구현되었다는 뜻은 아니다.

## 확인된 외부 계약과 남은 공백

출처는 부모가 전달한 프로젝트 소유자 채팅의 검증 보고다. 다른 프로젝트 저장소를 직접 조사하지 않았다. Grant **v0.4.1 (`2ec29f6`)**의 human voter는 `POST application-authz/check`, 서버 app-auth-key, Goauthy bearer, org ID와 resource provider/type/id를 사용하며 user+app+consent를 확인한다. batch/filter 후보 상한은 1000이다. workspace는 별도 native dimension이 아니고 권한 상속도 없다. 범용 workload/service/delegation은 없으며 `act`는 거부한다. master-key authz는 human fallback이 아니다.

Goauthy identity는 issuer+sub이고 ID token은 API용이 아니다. confidential Basic introspection으로 active·aud/scope/sub를 확인하며 권위 있는 workspace claim은 없다. 로컬 [GoAuthy pilot](docs/goauthy-local-pilot.md#L1)은 과거 opt-in harness 설명이며 현재 network adapter 또는 라이브 통합 테스트 근거가 아니다.

확인 대기는 Compos org/workspace와 Grant org/resource namespace, 작업 이름, 서비스 identity·승인 계약 및 실제 adapter 설정이다. 현재는 **연동 준비용 경계**다. 기본 서버·CLI가 인증 가능한 운영 제품이라고 기재하지 않는다. 상세는 [03장](docs/reverse-spec/03-auth-permissions.md)에 있다.

## 전체 문서

진입점은 [역명세 목차와 coverage](docs/reverse-spec/README.md)다.

| 장 | 계약 |
|---|---|
| [00 공통 실행](docs/reverse-spec/00-overview.md) | runtime·envelope·receipt·외부 승인·멱등성 |
| [01 객체·관계](docs/reverse-spec/01-objects-relations.md) | schema pin·필드·상태·관계·생성값 |
| [02 정의·Work](docs/reverse-spec/02-definitions-work.md) | spec·Task·Function·Workflow·Journal·참여자 |
| [03 외부 인증·인가](docs/reverse-spec/03-auth-permissions.md) | Goauthy/Grant 책임·authority seam·미구현 계약 |
| [04 자동화·연동](docs/reverse-spec/04-automation-integrations.md) | 규칙·scheduler·outbox·Conductor와 추가 승인 경계 |
| [05 저장·배포](docs/reverse-spec/05-storage-deployment.md) | 새 스키마 22개·v5 거부 경계·업무 import/export·Operator |
| [06 CLI·API·검색](docs/reverse-spec/06-cli-api-search.md) | 현재 라우트·CLI identity 주입·bbolt·SSR·검색 |

기존 zoom_out_report.md는 작성 시점의 이력으로 보존한다. 이전 28개 테이블·인증·migration에 대한 평가는 새 구현의 승인이나 실행 검증이 아니다. 새 이름 변경이나 Task 저장 모델 변경을 이 구현 승인으로 확대하지 않는다. Knowledge 런타임 제거와 일반 Ontology·공유 Journal 이력 유지도 구분한다.
