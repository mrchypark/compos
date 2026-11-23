# Compos 현재 구현 역명세

- 기준일: 2026-10-31. 사용자 승인으로 EGM 설계 전용 흐름을 종료하고 greenfield 구현 결과를 문서에 반영했다.
- 증거: 현재 Compos 소스·DDL·테스트 정적 열람. 문서 담당자는 테스트·빌드·서버·네트워크·배포·라이브 데이터 변경을 실행하지 않았다.
- 상태: 소스 기반 통합 초안. Compos는 무인증 HTTP 동작과 고정 `local` attribution만 제공한다. standalone과 HA는 저장·배포 토폴로지이며 인증 모드가 아니다. 정식 Goldfish 리뷰·라이브 통합 검증 완료가 아니다.

## 승인된 책임 경계와 현재 코드

Compos는 자체 계정·세션·권한 정책이 없으며 인증 없는 동작이 기본이자 유일한 런타임 계약이다. 모든 HTTP 요청은 고정 principal `local`로 기록하며 caller identity를 받지 않는다. 보호가 필요한 배포는 신뢰된 네트워크 경계에서 접근을 제한하고 caller를 인증해야 한다. 이 경계 밖의 인증 결과는 Compos 요청 주체로 전달되지 않는다. [principal 해석](internal/server/server.go#L1038), [생성자](internal/server/server.go#L41)

현재 스키마는 **22개 애플리케이션 테이블**, identity는 `compos-go-sqlite-authless-v7`다. membership, provider identity, session, actor capability, claim, principal과 Person을 연결하는 표시용 표는 없다. 구형 DB는 identity 불일치로 거부하며 인증 호환 마이그레이션이나 자동 삭제·이관 경로는 없다. 기존 라이브 DB는 변경하지 않았다. [DDL](internal/store/schema.sql#L1), [identity](internal/store/store.go#L20)

workspace·Work participant, 명령 ledger와 감사 actor, 도메인 상태·스키마 검증은 유지한다. actor type/id는 실행 주체 기록일 뿐 권한 부여가 아니다. workspace 생성은 creator principal을 기록하고 빈 도메인 모델로 시작한다. [actor 경계](internal/kernel/actor.go#L5), [workspace provisioning](internal/server/server.go#L285)

## 고정 local attribution과 standalone·HA

Compos는 bearer credential이나 인증 제공자 endpoint를 직접 호출하지 않는다. 보호가 필요한 배포는 네트워크 경계에서 접근을 제한하고 caller identity를 확인한다.

Grant를 포함한 authorization engine 연계, membership, actor capability, row permission은 현재 범위에 없다. 실행 토폴로지는 `standalone/sqlite`, `standalone/rhiza`, `ha/rhiza` 세 가지다. 인증 유무는 이 구분에 포함되지 않는다. 상세는 [00장](docs/reverse-spec/00-overview.md)과 [05장](docs/reverse-spec/05-storage-deployment.md)에 있다.

## 전체 문서

진입점은 [역명세 목차와 coverage](docs/reverse-spec/README.md)다.

| 장 | 계약 |
|---|---|
| [00 공통 실행](docs/reverse-spec/00-overview.md) | runtime·envelope·receipt·고정 local attribution·멱등성 |
| [01 객체·관계](docs/reverse-spec/01-objects-relations.md) | schema pin·필드·상태·관계·생성값 |
| [02 정의·Work](docs/reverse-spec/02-definitions-work.md) | spec·Task·Function·Workflow·Journal·참여자 |
| [04 자동화·연동](docs/reverse-spec/04-automation-integrations.md) | 규칙·scheduler·outbox·Conductor |
| [05 저장](docs/reverse-spec/05-storage-deployment.md) | SQLite·Rhiza·v7 identity·22개 테이블·import/export·배포 산출물 |
| [06 API·검색](docs/reverse-spec/06-api-search.md) | HTTP route·typed pagination·검색 projection |

이전 인증 및 migration에 대한 평가는 현재 구현의 실행 검증이 아니다. Knowledge 런타임 제거와 일반 Ontology·공유 Journal 이력 유지도 구분한다.
