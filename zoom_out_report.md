# Compos 전체 역명세 — Stage 5 전체 구조 검토

- 기준일: 2026-10-31
- 대상: `design_draft.md`, `docs/reverse-spec/README.md`, 00~06장 전체
- 기준: 인증 및 권한 정책은 외부 소유. 현재 구현 설명과 목표 소유권 충돌을 구분한다.
- 방식: `fork_context:false`로 생성한 새 검토자가 EGM zoom_out persona와 zoom-out-standard 전문 및 모든 대상 문서를 읽고 보고했다. 코드 근거는 필요한 범위만 대조했다. 테스트·구현 변경 없음.
- 범위: Stage 5만 수행. 구현 계획이 없어 DetailedRange는 N/A. 후속 단계는 미수행.

## 판단

현재 상태를 설명한 역명세는 인증·인가 외부화 설계가 아니다. 현재 동작을 거짓으로 고치지 말고, 외부화 목표와 차이 및 미결정을 별도로 명시해야 한다. 차단 2건, 중요 5건, 경미 1건이다.

## 차단 사항

### B1. 현재 책임과 외부 소유 목표의 충돌이 표시되지 않음

Compos는 IdP 결과 확인 외에도 자동 가입, identity 저장, Person 연결, 세션 발급, membership 및 actor capability 판정을 수행한다. 외부 IdP 사용만으로 인증·인가 외부화가 충족되지 않는다.

근거: [03장](docs/reverse-spec/03-principal.md#L101), [인증 처리](internal/server/auth.go#L396), [Kernel 권한](internal/kernel/executor.go#L139), [00장](docs/reverse-spec/00-overview.md#L14).

필요한 결정: 외부로 이전·폐기할 책임과 외부 결과를 집행하기 위해 Compos에 남길 최소 책임을 구분한다. 공급자는 미선택이다.

### B2. 외부 판단을 받는 신뢰 계약이 없음

현재 envelope의 요청 actor나 causation은 외부 신원·인가 판단의 보증이 아니다. 신뢰할 전달자, 주체와 대행 actor, workspace·행위·데이터 범위, 유효기간, 누락·만료·외부 장애 처리, 커밋 시점 유효성, 판단 증거를 정의해야 한다.

근거: [명령 envelope](docs/reverse-spec/00-overview.md#L26), [현재 인증 순서](docs/reverse-spec/03-principal.md#L53), [현재 권한 계산](docs/reverse-spec/03-principal.md#L159).

## 중요 사항

### M1. 업무 유효성과 권한 정책의 경계

스키마·CAS·관계 수·종료 상태 검증과 외부 인가를 구분해야 한다. 참여자 일치, Function 허용 목록, writable fields, 생성자 전용 lifecycle, actor별 금지 목록은 소유권 분류가 필요하다. 권한 정책을 업무 규칙으로 이름만 바꾸어 유지해서도 안 된다.

근거: [Function 정책](docs/reverse-spec/02-definitions-work.md#L94), [참여자](docs/reverse-spec/02-definitions-work.md#L193), [쓰기 제한](docs/reverse-spec/02-definitions-work.md#L215), [권한 계산](docs/reverse-spec/03-principal.md#L160).

### M2. 내부 실행도 외부 판단 계약이 필요

규칙·scheduler·callback reducer·connector·syncer의 실행 주체와 대행 범위를 정해야 한다. 사용자 HTTP 경로만 외부화하면 background 실행은 로컬 정책에 남는다. 내부 호출이라는 명칭은 면제 근거가 아니다.

근거: [자동화 principal](docs/reverse-spec/04-automation-integrations.md#L93), [callback](internal/server/conductor.go#L251), [서비스 읽기](internal/server/conductor.go#L313).

### M3. 일반 inbound와 신뢰된 외부 결과의 경계

일반 inbound에서 받은 adapter_id가 conductor이면 background reducer의 소비 대상이 될 수 있다. 기존 invocation·handler·종료 검사가 있어 임의 주입 성공을 단정하지 않는다. 다만 저장된 이름만으로 신뢰가 승격되지 않도록 수신·환원 계약을 명시해야 한다.

근거: [inbound 저장](internal/server/server.go#L903), [reducer 선택](internal/server/conductor.go#L398), [현재 한계](docs/reverse-spec/04-automation-integrations.md#L251).

### M4. 권한 철회 후 재시도 의미

기존 receipt 읽기, 미실행 의도 재전송, 이미 외부에서 완료된 결과 기록, 후속 업무 변경을 분리해야 한다. 현재 duplicate receipt는 권한 재검사보다 먼저 반환된다. 철회 시점과 외부 판단 장애·재시도 규칙은 미결정이다. 같은 의도를 새 command로 바꾸어 멱등성을 깨뜨려서는 안 된다.

근거: [duplicate](docs/reverse-spec/00-overview.md#L47), [세션 철회](docs/reverse-spec/03-principal.md#L188), [callback 실패](docs/reverse-spec/04-automation-integrations.md#L178), [CLI scope](docs/reverse-spec/06-cli-api-search.md#L107).

### M5. 데이터 이전·보존 경계

identity·session·membership·capability, 과거 principal·Person 참조, 업무·감사 이력을 구분해야 한다. 현재 import는 이들을 함께 이동하므로 외부화 migration으로 간주할 수 없다. principal 참조를 일괄 삭제하면 과거 이력과 배정을 훼손할 수 있다.

근거: [테이블 목록](docs/reverse-spec/05-storage-deployment.md#L32), [import](internal/store/logical_import.go#L69).

## 경미 사항

### N1. 진입 문서의 책임 경계 안내

현재 책임·외부 목표·충돌·미결정을 먼저 제시하고 작성 이력과 세부 직렬화 규칙은 상세 장으로 둔다. 상세 역명세 자체의 존재는 문제로 보지 않는다.

## 재사용과 한계

Kernel 명령 경계, 원자적 커밋, 원장, outbox/inbound, 고정 revision은 재사용 가능한 기반이다. 이를 외부화와 함께 다시 만들 필요는 확인되지 않았다. 기존 OAuth2 모드도 직접 검증과 로컬 권한 계산이 남아 있어 목표를 이미 충족한 해법은 아니다.

검토자는 persona·standard, 대상 9개 문서, 프로젝트 README 전문을 읽었다고 보고했다. auth.go·executor.go·rhiza.go·server.go·conductor.go·logical_import.go는 관련 범위만 대조했다. 모든 코드·링크·테스트를 전수 검증한 결과가 아니다. 다른 프로젝트나 외부 서비스는 조사하지 않았다.

## 다음 체크포인트

Stage 6 이해도 검토로 진행하거나, 목표 책임 경계를 문서에 보완한 뒤 재검토하거나, 검토를 보류한다. 현재 역명세·운영 코드는 변경하지 않았다.
