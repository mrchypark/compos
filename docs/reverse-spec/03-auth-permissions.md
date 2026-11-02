# 03. 외부 인증·인가와 실행 주체 경계

- 기준일: 2026-10-31. 현재 작업 트리의 greenfield 구현을 설명한다.
- 증거: Compos 소스·테스트 정적 열람과 외부 프로젝트 소유자 채팅의 검증 보고를 구분한다. 문서 작업에서는 테스트·빌드·네트워크 연동·배포를 실행하지 않았다.
- EGM 설계 전용 흐름은 사용자의 구현 승인으로 종료했다. 코드·테스트·설정은 core 담당자가 소유하며 이 문서는 현재 코드와 외부 계약의 공백을 기록한다.

## 1. 목적과 책임 경계

계정·인증은 **Goauthy**, 인가는 **Grant**가 소유한다. Compos는 자체 계정·세션·membership·capability를 관리하지 않는다. 신뢰된 adapter를 주입하는 `server.NewWithAuthority`와 Kernel `AuthorizeCommand` 경계가 구현되어 있다. **Goauthy/Grant network adapter는 구현되어 있지 않다.** 외부 계약 준비와 실제 통합을 구분한다. [서버 생성](../../internal/server/server.go#L44), [Authority](../../internal/server/authority.go#L11), [Kernel 경계](../../internal/kernel/authority.go#L10)

workspace·Person·Work participant는 업무 데이터이며 사용자 계정이나 접근 권한의 정본이 아니다. 감사 principal·actor와 명령 ledger의 멱등 범위는 유지한다. 업무 상태·스키마·참여자 검사는 외부 인가를 대신하지 않는다.

## 2. 현재 제거된 상태와 영속성

새 DDL은 **22개 애플리케이션 테이블**이다. `workspace_memberships`, `auth_provider_identities`, `auth_sessions`, `actor_capabilities`, `principal_person_bindings`, `person_binding_claims` 여섯 테이블은 없다. 인증 토큰 교환·세션 발급/폐기·자동 가입·capability 관리 API와 principal–Person 로그인 바인딩도 제거되었다. [DDL](../../internal/store/schema.sql#L1), [현재 라우트](../../internal/server/server.go#L78)

schema identity는 `compos-go-sqlite-external-authority-v5`다. 기존 identity의 DB는 거부하며 v3/v4 자동 업그레이드나 인증 호환 마이그레이션을 제공하지 않는다. 이 작업은 라이브 DB를 열어 변환·삭제한 작업이 아니다. legacy 업무 데이터 export/import 도구의 존재를 인증 상태 이관이나 구형 DB 직접 개방 지원으로 해석하지 않는다. [identity·개방](../../internal/store/store.go#L19), [SQLite 검사](../../internal/store/store.go#L113), [Rhiza 검사](../../internal/store/rhiza.go#L47), [업무 import 목록](../../internal/store/logical_import.go#L69)

## 3. HTTP authority의 정확한 계약

`Authority`는 두 메서드를 요구한다.

| 메서드 | 입력·결과 | 책임 |
|---|---|---|
| `AuthorizeRequest(*http.Request) (string,error)` | 실제 HTTP 요청 → 검증된 principal 문자열 | credential뿐 아니라 resource scope를 포함한 HTTP 작업 전체를 승인해야 한다. 인증만 성공한 결과로 허용해서는 안 된다. |
| `AuthorizeCommand(context.Context,string,kernel.Command) (kernel.Actor,error)` | context, principal, 전체 명령 → 승인된 actor | workspace·actor·payload·causation을 포함한 전체 명령을 승인한다. 저장된 principal은 서비스 위임 증거가 아니다. |

근거: [interface 주석과 서명](../../internal/server/authority.go#L11), [CommandAuthorizer](../../internal/kernel/authority.go#L10).

요청 처리 순서는 다음과 같다. [middleware](../../internal/server/authority.go#L24)

1. 정확히 `GET /api/v1/health`이면 authority 없이 통과한다. 저장소 health 자체가 실패하면 handler에서 503을 반환한다.
2. authority가 nil이면 나머지 모든 경로에 `503 external_authority_unavailable`, category=`dependency`, retryable=true를 반환한다. 미등록·과거 auth 경로도 router의 404보다 이 검사를 먼저 받는다.
3. 명시적 `kernel.ErrAuthorizationDenied` 또는 성공 결과의 trim 후 빈 principal이면 `403 forbidden`, category=`authz`, retryable=false다. 그 밖의 외부 오류·취소·timeout은 503 external_authority_unavailable/dependency/retryable=true로 구분한다. 오류 내용을 외부에 그대로 노출하지 않는다.
4. 승인 principal을 request context에 넣은 뒤 router/handler를 실행한다. Compos가 provider protocol이나 임의 identity header를 직접 해석하지 않는다.

`server.New(store)`는 `NewWithAuthority(store,nil)`이다. 현재 `cmd/server`도 `server.New`를 사용하므로 외부 adapter를 연결하지 않은 기본 실행에서 health 이외 API는 사용할 수 없다. dev principal이나 무인증 fallback은 없다. [생성자](../../internal/server/server.go#L44), [실행 진입점](../../cmd/server/main.go#L93)

`/internal/v1/conductor/callbacks`, `/v1/connector/...`, search-syncer discovery/projection도 authority를 먼저 통과해야 한다. Conductor handler의 별도 callback/service token 검사는 추가 검사일 뿐 authority 대체 경로가 아니다. 기존 서비스 token 설정만으로 새 통합이 운영 가능하다고 주장할 수 없다. [현재 middleware](../../internal/server/authority.go#L24), [callback 검사](../../internal/server/conductor.go#L61), [connector 검사](../../internal/server/conductor.go#L313)

## 4. Kernel actor와 멱등성

Executor의 AuthorizeCommand가 nil이거나 일반 외부 오류를 반환하면 external_authority_unavailable/retryable=true다. 명시적 ErrAuthorizationDenied는 forbidden이다. HTTP command 매핑은 각각 503 dependency와 403 authz다. 반환 actor는 다음 조건을 만족해야 한다. [검사](../../internal/kernel/authority.go#L15)

- principal과 actor ID는 trim 후 비어 있지 않아야 한다.
- actor type은 `user|service|automation`이다. 이는 형식 허용값이며 Grant의 service/delegation 지원 선언이 아니다.
- user actor ID는 principal과 같아야 한다.
- requested_actor가 없으면 요청 actor를 `{type:user,id:principal}`로 간주한다. 외부 승인 actor와 요청 actor가 정확히 같아야 한다.

`Execute`는 envelope·principal·내부 결과 명령 causation을 확인한 뒤 **중복 조회 전에 외부 명령 승인을 검사한다**. 동일 요청 재시도도 현재 authorizer의 거부를 우회하지 않는다. 신규 SQL 변경은 트랜잭션 안에서, Rhiza 변경은 prepare 단계에서도 authorizer를 호출하므로 한 실행에서 여러 번 호출될 수 있다. callback에 단일 호출만 발생한다고 가정해서는 안 된다. [실행 입구](../../internal/kernel/executor.go#L61), [Rhiza prepare](../../internal/kernel/rhiza.go#L250)

멱등 범위는 `(workspace_id,authenticated_principal_id,idempotency_key)`다. command ID/type, payload hash, target, preconditions, causation, requested actor의 동일성 비교는 유지한다. 외부 권한 검사는 SQL membership/capability guard로 복제되지 않는다. 새 명령의 workspace 존재와 도메인 사전조건 검사는 별도로 남는다. [SQL 실행](../../internal/kernel/executor.go#L61), [Rhiza guard](../../internal/kernel/rhiza.go#L250)

Workflow·ReactiveRule·Conductor는 내부에서 Kernel을 직접 호출한다. 이 경로에도 authorizer가 필요하다. 사람 사용자 Grant 승인이나 과거 principal을 저장했다는 사실은 scheduler/service 위임 증거가 아니다. 서비스 계약이 없는 상태에서 adapter가 이 호출을 허용한다고 문서화하지 않는다. [Workflow](../../internal/server/workflow.go#L108), [ReactiveRule](../../internal/server/reactive_rule.go#L187), [Conductor](../../internal/server/conductor.go#L251)

## 5. CLI와 workspace provision

CLI의 `setup|login|logout|whoami`는 로컬 계정 명령 제거 오류를 반환한다. `loadSession`이라는 내부 함수명은 남지만 파일 세션을 읽지 않고 주입된 `identityProvider`를 호출한다. provider 부재·오류, 비어 있는 server/principal/credential이면 실패한다. `cliSession`은 외부 credential을 전달하는 메모리 값이며 Compos 세션 발급 모델이 아니다. HTTP에는 credential을 bearer로 보낸다. [CLI identity](../../cmd/compos/auth.go#L9), [제거된 명령](../../cmd/compos/commands.go#L94)

`--token`, `COMPOS_SESSION_PATH`, `$HOME/.compos/auth.json`을 새 로컬 인증 경로로 제공하지 않는다. bbolt 요청 원장은 유지하며 외부 identity의 server/principal/workspace와 원장 pin을 대조한다. 현재 기본 CLI의 외부 identity network adapter도 없다. [credential 경계](../../cmd/compos/auth.go#L9), [원장 설정](../../cmd/compos/session.go#L24)

workspace 생성은 authority가 승인한 principal을 감사 주체로 사용하고 업무용 people/companies 타입, 생성자 Person, workspace Company, works_for 관계·슬롯·필드 인덱스를 원자적으로 만든다. owner membership과 principal–Person binding은 만들지 않는다. 응답에 남은 creator 식별자를 외부 인가의 소유권 부여로 해석하지 않는다. [provision](../../internal/server/server.go#L122)

`ConfigureSearchSyncer(workspace)`는 trim한 데이터 namespace만 설정한다. credential 검증은 Authority 책임이다. discovery의 workspace 미설정은 `503 workspace_unavailable/configuration/retryable=false`다. [설정과 응답](../../internal/server/authority.go#L41)

## 6. 소유자 채팅에서 확인된 외부 계약

출처는 2026-10-31 부모 세션을 통해 전달된 **각 프로젝트 소유자 채팅의 검증 보고**다. Grant 기준은 **v0.4.1, commit `2ec29f6`**이다. 외부 저장소를 직접 열거나 Compos에서 라이브 호출한 결과가 아니다. Goauthy 버전·commit은 전달되지 않았다.

| 책임·경로 | 확인된 계약 | 제한 |
|---|---|---|
| Goauthy identity | issuer+sub; confidential Basic introspection으로 active·aud/scope/sub 확인 | ID token은 API용이 아니며 권위 있는 workspace claim은 없다. |
| Grant human voter | `POST application-authz/check`, 서버 app-auth-key+Goauthy bearer+org ID, resource provider/type/id; user+app+consent 검사 | 정확한 필드·응답 스키마를 이 요약만으로 만들지 않는다. app key가 사용자 검증을 대체하지 않는다. |
| Grant batch/filter | 최대 후보 1000개 | 목록 처리·페이지·분할은 resource 계약과 함께 정해야 한다. |
| workspace | 별도 native dimension이나 권한 상속 없음 | org=workspace 또는 workspace→객체 상속을 가정하지 않는다. |
| workload/service/delegation | 범용 계약 없음; `act` 거부 | 서비스·automation을 human voter로 가장하지 않는다. |
| master-key authz | human 경로 fallback 아님 | 사용자 거부·장애 시 master key로 재시도하지 않는다. |

확인 대기는 Compos org/workspace와 Grant org/resource namespace의 대응, provider/type/id·작업 이름, 서비스 identity/승인 계약, 장애 처리 및 adapter 설정이다. **현재는 연동 준비용 seam이며 operational adapter가 아니다.** [로컬 GoAuthy pilot](../goauthy-local-pilot.md#L1)은 과거 opt-in harness 설명으로만 남아 있으며 현재 API 호환·테스트 통과의 근거가 아니다.

## 7. 테스트 근거와 공백

[서버 authority 테스트](../../internal/server/authority_test.go#L1)는 주입 경계·기본 거부·요청 승인 등을 검사하도록 작성되었다. [Kernel authority 테스트](../../internal/kernel/authority_test.go#L1)는 승인 누락/거부·actor 일치와 중복 실행 재승인 경계를 검사한다. [CLI authority 테스트](../../cmd/compos/authority_test.go#L1)는 외부 identity 주입과 기존 계정 경로 제거를 검사한다. 이들은 fake/in-process fixture를 사용하는 소스 근거이며 Goauthy/Grant network adapter나 범용 서비스 승인을 증명하지 않는다.

이번 문서 작업에서 테스트·빌드·실제 HTTP/IdP/Grant 호출·DB 변환을 실행하지 않았다. 링크 존재·DDL 목록·문서 정합성만 정적으로 확인한다.

## 8. 소스 지도

| 계약 | 소스 |
|---|---|
| HTTP 요청 승인·syncer namespace | [authority.go](../../internal/server/authority.go#L11) |
| 생성자·라우트·workspace | [server.go](../../internal/server/server.go#L44) |
| 명령 승인·actor 일치 | [Kernel authority](../../internal/kernel/authority.go#L10), [Executor](../../internal/kernel/executor.go#L61) |
| 새 스키마·identity | [DDL](../../internal/store/schema.sql#L1), [store.go](../../internal/store/store.go#L19), [rhiza.go](../../internal/store/rhiza.go#L47) |
| CLI 외부 identity | [auth.go](../../cmd/compos/auth.go#L9), [commands.go](../../cmd/compos/commands.go#L94) |

*EGM으로 작성한 역명세를 승인된 greenfield 구현에 맞춰 갱신했다.*
