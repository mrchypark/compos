# 01. 객체·타입·관계의 현재 계약

- 상태: 역명세 초안. 2026-10-31 작업 트리의 소스가 기준이다.
- 작성 방식: EGM draft의 목적·경계·사용 흐름·기술 계약·검증 구성을 현재 구현의 역명세에 맞추었다. 새로운 대안이나 구현 변경 계획은 만들지 않는다.
- 근거 수준: 아래의 구현 설명은 **소스 근거**이며, 테스트 절은 **테스트 코드 근거**이다. 이 작업에서는 테스트·빌드·서버 실행을 하지 않았으므로 실행 검증 결과는 없다.

## 1. 목적과 경계

워크스페이스 안에서 사용자가 정의하는 객체 타입, 불변 스키마 리비전, 객체 인스턴스, 관계 정의와 간선을 설명한다. 필드 검증, 자동 번호, 상태 이동, 보관과 연결 해제, 범용 커널 명령, 객체 리소스 HTTP API 및 관련 저장 제약을 포함한다. 현재 삭제 중인 Knowledge 구현이나 과거 문서를 현재 계약으로 인용하지 않는다.

Work 제출과 함수가 객체 변경을 호출하는 접점은 포함하지만, Work·함수·워크플로 전체 계약과 검색 서비스의 색인 계약은 이 문서의 범위 밖이다. 근거의 진입점은 [명령 등록부](../../internal/kernel/registry.go#L39), [저장소별 실행 분기](../../internal/kernel/executor.go#L59), [HTTP 라우트](../../internal/server/server.go#L88)이다.

## 2. 엔터티와 상태

| 엔터티 | 식별·내용 | 버전과 상태 |
|---|---|---|
| `object_type` | `id`, `workspace_id`, `key`, `singular`, `plural`, `field_schema_json`, `relation_schema_json`, nullable `state_machine_id` | `version`과 `schema_version`은 별도 정수이며 1부터 시작한다. `archived_at`이 NULL이면 활성이다. |
| 스키마 리비전 | `(workspace_id, object_type_id, schema_version)`, 필드·관계 스키마, 상태 머신 ID, `digest`, 생성 시각·주체 | 삽입 후 UPDATE와 DELETE를 트리거가 거부한다. |
| `object_instance` | `id`, 타입 ID와 `object_type_schema_version`, `title`, JSON `payload_json`(API에서는 `values`), nullable `state_key` | `version`은 1부터 시작한다. 수정·이동·보관 시 증가한다. |
| `relation_definition` | `id`, 워크스페이스 내 유일한 `key`, `from_object_type_id`, `to_object_type_id`, `cardinality`, `required` | `version`, nullable `archived_at`을 가진다. 정의 수정 명령은 등록되어 있지 않다. |
| `object_relation` | 정의 ID, 출발·도착 객체 ID, `version` | 생성은 버전 1이며 `archived_at`은 없다. unlink는 행을 물리 삭제한다. |
| 카디널리티 슬롯 | 정의·간선·`side`·객체 ID | `(workspace_id, relation_definition_id, side, object_instance_id)` 유일성으로 단일 연결을 강제한다. |

타입·객체·관계 정의·간선은 생성/수정 시각과 principal을 저장한다. 타입과 관계 정의의 `(workspace_id,key)` 유일성은 보관된 행에도 적용되므로 보관만으로 키를 재사용할 수 없다. 관계 간선은 `(workspace_id,definition,from,to)`도 유일하다. 외래 키는 워크스페이스를 함께 묶어 다른 워크스페이스의 타입·객체를 참조하지 못하게 한다. [타입·리비전·객체 DDL](../../internal/store/schema.sql#L18), [관계 DDL](../../internal/store/schema.sql#L120)

스키마 리비전 digest는 `schema_version`, `field_schema`, `relation_schema`, `state_machine_id`를 포함한 JSON의 해시이다. 객체 생성 시 현재 리비전을 고정하고, 필드 값 전체를 교체할 때 최신 리비전으로 다시 고정한다. 제목만 바꾸거나 상태를 이동할 때에는 기존 pin을 유지한다. [digest](../../internal/kernel/executor.go#L988), [객체 생성](../../internal/kernel/rhiza_object.go#L167), [수정](../../internal/kernel/rhiza_object.go#L284)

## 3. 범용 커널 명령 계약

### 3.1 봉투와 공통 응답

`POST /api/v1/kernel/commands`는 최대 8 MiB JSON을 받는다. 봉투는 `command_id`, `idempotency_key`, `workspace_id`, nullable `requested_actor:{type,id}`, `command_type`, `target:{aggregate_type,aggregate_id}`, `preconditions:[{aggregate_type,aggregate_id,expected_version}]`, `payload`, `causation:{source,attempt_id,event_id,automation?}`이다. 앞의 세 식별 문자열은 비어 있으면 거부한다. payload는 명령별 허용 속성만 받으며 미등록 속성과 타입 불일치는 `schema_mismatch`이다. [봉투 타입](../../internal/kernel/types.go#L8), [명령 검증](../../internal/kernel/registry.go#L117), [HTTP 제출](../../internal/server/server.go#L376)

성공 응답은 HTTP 200이며 `{command_id,outcome,aggregate:{type,id,version},projection:{workspace_id,projection_sequence}}`이다. 최초 성공의 `outcome`은 `applied`, 같은 명령의 재전송은 `duplicate`이다. 내부 receipt에는 `aggregate_version`도 있으나 HTTP 응답은 이를 별도 최상위 필드로 내보내지 않는다. [receipt](../../internal/kernel/types.go#L49), [응답 조립](../../internal/server/server.go#L394)

### 3.2 명령별 입력

아래에서 `!`는 필수 payload 키이고 `?`는 선택 키이다. 생성 명령은 target ID를 두지 않으며, 나머지는 해당 aggregate ID를 지정한다. 실제 target ID 검증은 “nil 또는 빈 값” 여부를 검사하므로 등록부의 `must_be_null` 표기보다 느슨하게 빈 문자열 포인터도 허용한다.

| `command_type` | target aggregate / ID | payload | 필요한 precondition aggregate 종류 | 이벤트 |
|---|---|---|---|---|
| `object_type.define` | `object_type` / 없음 | `key!`, `singular!`, `plural!`, `field_schema!`(object), `state_machine_id?`(string/null) | 없음 | `object_type.defined` |
| `object_type.schema_evolve` | `object_type` / 있음 | `field_schema!`(object) | `object_type` | `object_type.schema_evolved` |
| `relation_definition.define` | `relation_definition` / 없음 | `key!`, `from_object_type_id!`, `to_object_type_id!`, `cardinality!`, `required!`(boolean) | `object_type` | `relation_definition.defined` |
| `object.create` | `object_instance` / 없음 | `object_type_key!`, `title!`, `values?`(object), `relations?`(object) | 없음 | `object.created` |
| `object.update` | `object_instance` / 있음 | `title?`, `values?`(object) | `object_instance` | `object.updated` |
| `object.move` | `object_instance` / 있음 | `to_state!`, `reason?`, `force?`(boolean) | `object_instance` | `object.moved` |
| `relation.link` | `object_relation` / 없음 | `relation_definition_id!`, `from_object_id!`, `to_object_id!` | `relation_definition`, `object_instance` | `relation.linked` |
| `relation.unlink` | `object_relation` / 있음 | `relation_definition_id!` | `object_relation` | `relation.unlinked` |
| `object_type.delete` | `object_type` / 있음 | `hard?`(boolean), `reason?`(string/null) | `object_type` | `object_type.deleted` |
| `object.delete` | `object_instance` / 있음 | 같은 삭제 payload | `object_instance` | `object.deleted` |
| `relation_definition.delete` | `relation_definition` / 있음 | 같은 삭제 payload | `relation_definition` | `relation_definition.deleted` |

표에서 따로 타입을 쓰지 않은 payload 필드는 string이다. `cardinality`는 `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many` 중 하나이다. 필수 키라는 사실만으로 모든 문자열에 비공백 검사가 생기는 것은 아니다. 객체 생성/수정의 title과 상태 이동의 `to_state`는 실행 함수에서 추가로 검사한다. [명령 정의 전체](../../internal/kernel/registry.go#L39), [공통 삭제 속성](../../internal/kernel/registry.go#L69)

precondition 종류의 필수 검사는 **해당 종류가 하나 이상 있는지**만 확인한다. 예를 들어 관계 정의의 양쪽 타입, relation.link의 양쪽 객체 또는 수정 target과 일치하는 모든 ID의 precondition을 등록부가 자동으로 요구하지는 않는다. 제출한 각 precondition은 워크스페이스 내 존재와 정확한 버전을 검사하며, Rhiza는 읽은 상태를 적용 시 다시 확인하는 SQL guard도 추가한다. 보관 상태는 공통 precondition 쿼리에서 제외하지 않는다. [SQLite 검사](../../internal/kernel/executor.go#L144), [Rhiza 검사](../../internal/kernel/rhiza.go#L311)

### 3.3 local attribution·actor 기록·멱등성·실패

HTTP는 요청을 인증하지 않고 등록된 route로 전달하며 모든 요청을 고정 principal `local`로 기록한다. caller identity를 받지 않으며 workspace·객체·command 단위 허용 정책도 없다. `requested_actor`는 비어 있지 않은 `type`과 `id`를 기록하는 attribution이며 principal 일치나 actor type 제한을 받지 않는다. ReactiveRule과 Workflow도 같은 `Execute` 경로를 사용한다. [HTTP 처리](../../internal/server/server.go#L54), [actor 해석](../../internal/kernel/actor.go#L5)

멱등 범위는 `(workspace_id, principal_id, idempotency_key)`이다. 기존 명령의 ID·종류·payload hash·target·preconditions·causation·requested actor가 같으면 duplicate, 다르면 idempotency_conflict다. 이 비교는 중복 요청을 구분하는 ledger 계약이며 access-control 판정이 아니다. [실행](../../internal/kernel/executor.go#L59), [Rhiza](../../internal/kernel/rhiza.go#L18)

| 오류 outcome | HTTP | 의미 |
|---|---|---|
| `validation_failed` | 400 | 상태 이동 불허, 필수 관계 누락, 생성 필드 직접 입력, DB 제약 위반 등 |
| `schema_mismatch` | 422 | payload/필드 스키마나 관계 endpoint 타입 불일치 |
| `precondition_failed` | 409 | 미존재, 버전 불일치, 적용 직전 상태 변경 |
| `idempotency_conflict` | 409 | 같은 멱등 키에 다른 명령 |
| `forbidden` | 403 | 도메인 규칙 또는 내부 결과 command의 공개 제출 차단 |
| `quorum_unavailable` | 503 | Rhiza 가용성 오류 |
| `unsupported_storage` | 501 | 지원하지 않는 저장소 실행 경로 |

오류 응답은 `problemDetails` 경로로 생성된다. 커널 오류가 아닌 저장소 오류는 `dbError`로 처리하므로 모든 내부 실패가 위 outcome으로 정규화되는 것은 아니다. [HTTP 오류 매핑](../../internal/server/server.go#L1071)

## 4. 타입·필드·스키마 진화

### 4.1 등록 가능한 필드 스키마

루트 `type`을 생략하면 `object`를 채우고, 다른 루트 타입은 거부한다. 지원 키워드는 `type`, `properties`, `required`, `items`, `additionalProperties`, `enum`, `minLength`, `maxLength`, `minimum`, `maximum`, `pattern`, `format`, `readOnly`, `state_machine`, `x-compos-generation`으로 제한된다. 스키마 깊이가 32를 초과하면 거부한다. [등록 검증](../../internal/kernel/schema.go#L17)

필드 타입은 `string`, `number`, `integer`, `boolean`, `object`, `array`, `null`과 비어 있지 않은 타입 배열을 지원한다. 배열에는 `items` 스키마가 필요하고, `additionalProperties`는 boolean만 지원한다. `required` 목록은 선언된 속성을 가리켜야 한다. 문자열 길이는 rune 수로 검사하며, Go 정규식 `pattern`과 `regex:` format, `date`, `date-time`, `email`, `uri`를 지원한다. 숫자는 minimum/maximum, 값은 enum, 객체와 배열은 재귀 검증한다. 알 수 없는 필드는 `additionalProperties:false`일 때만 거부한다. [타입 목록](../../internal/kernel/schema.go#L297), [값 검증](../../internal/kernel/schema.go#L184)

`readOnly`는 허용 키워드이지만 일반 필드의 쓰기 금지로 집행되지 않는다. 생성 필드의 입력 금지는 별도 로직이다. 또한 허용 키워드 목록이 완전한 JSON Schema 구현을 뜻하지 않는다. 일부 키워드의 잘못된 자료형은 Go 타입 단언 조건에 걸리지 않아 해당 제약이 적용되지 않을 수 있다. [등록 검사](../../internal/kernel/schema.go#L39), [값 검사](../../internal/kernel/schema.go#L219)

### 4.2 진화와 객체 수정

타입 정의는 빈 관계 스키마 `{"links":[]}`와 첫 리비전을 함께 저장한다. `schema_evolve`는 필드 스키마를 교체하고 타입의 두 버전을 증가시키며 새 불변 리비전을 저장한다. 기존 객체 값 전체의 유효성이나 호환성을 소급 검사하지 않는다. 기존 `state_machine`은 생략 시 상속하며 다른 값으로 변경하려 하면 `validation_failed`이다. 관계 스키마와 `state_machine_id`는 유지한다. [타입 정의·진화](../../internal/kernel/rhiza.go#L362)

객체 생성은 활성 타입의 현재 스키마로 값을 검사하고 초기 상태를 정한다. `object.update.values`는 부분 병합이 아니라 **전체 교체**이며, 생략된 일반 필드는 남지 않는다. 기존 생성 필드는 서버가 새 값 집합에 복사한다. 교체 값은 최신 타입 스키마로 검사하고 schema pin도 갱신한다. title만 수정하면 값과 pin을 유지한다. 범용 `object.update`는 빈 payload도 허용하며 버전이 증가하지만, 타입별 HTTP PATCH는 title 또는 values를 요구한다. [생성](../../internal/kernel/rhiza_object.go#L167), [수정](../../internal/kernel/rhiza_object.go#L284), [HTTP PATCH](../../internal/server/object_resources.go#L227)

### 4.3 상태 이동

상태 머신은 루트 필드 스키마의 `{initial,transitions}`이며 `initial`은 transitions의 구체적인 키여야 한다. 각 값은 중복 없는 목적 상태 배열이고 `*`는 목적 목록에서만 허용한다. 객체 생성 시 `state_key=initial`을 기록한다. `state_machine_id` 문자열 자체가 실행할 전이 정의를 제공하는 것은 아니다. [상태 스키마 검사](../../internal/kernel/schema.go#L344), [초기 상태](../../internal/kernel/rhiza_object.go#L243)

`object.move`는 활성 객체와 타입의 현재 `field_schema.state_machine`을 읽는다. 목적 상태는 정의된 키여야 하며, 기본적으로 현재 상태의 목적 목록에 목적 상태 또는 `*`가 있어야 한다. `force:true`는 이 연결 검사를 생략하지만 미정의 목적 상태를 허용하지 않는다. 성공하면 객체 버전만 증가하고 `object.moved`에 `{id,state_key,version}`을 기록한다. `reason`은 등록 입력이지만 이 effect payload에는 포함되지 않는다. [상태 이동](../../internal/kernel/rhiza_object.go#L338)

## 5. 자동 번호와 생성 필드

`properties.<field>.x-compos-generation`은 string 타입 필드의 서버 생성 설정이다. 허용 속성은 `version`(있으면 1), `on`(있으면 `create`), 비어 있지 않은 `template`, `timezone`, `reset`, `unique`이다. timezone 기본값은 UTC이고, reset 기본값은 `never`이며 `day|month|year`도 가능하다. 명시된 timezone은 `time.LoadLocation`으로 검사한다. `unique`는 boolean으로 검사하지만 값 생성 실행 경로는 이 플래그와 무관하게 유일 값 테이블에 삽입한다. [설정 검사](../../internal/kernel/schema.go#L66), [설정 추출](../../internal/kernel/schema.go#L390)

| 토큰 | 현재 동작 |
|---|---|
| `{{today:YYMMDD}}` 등 | 지정 timezone의 시각을 사용한다. YYYY/YY/MM/DD를 Go 날짜 레이아웃으로 치환한다. |
| `{{sequence:N}}` | 같은 scope의 1부터 시작하는 번호를 최소 N자리로 0-padding한다. |
| `{{field:name}}` | 현재 값의 `fmt.Sprint` 결과를 넣는다. 필드가 없으면 별도 존재 검사를 하지 않는다. |
| `{{relation:key.field}}` | 생성 입력의 `relations[key]`를 시작점으로 관계 경로를 따라가 최종 객체의 scalar 필드를 읽는다. |

인식할 수 없는 `{{...}}` 토큰은 등록 시 거부한다. 번호 scope는 timezone에 맞춘 `never`, `YYYY-MM-DD`, `YYYY-MM`, `YYYY`이며 카운터 키는 `(workspace,type,field,scope)`이다. 필드 값이 달라도 scope가 같으면 같은 카운터를 공유한다. sequence 토큰이 없으면 카운터를 변경하지 않는다. [토큰 문법](../../internal/kernel/schema.go#L14), [렌더링](../../internal/kernel/schema.go#L412), [예약·생성](../../internal/kernel/rhiza_object.go#L187)

생성 필드를 클라이언트가 보내면 값이 같아도 `validation_failed`이다. 생성 전에 required 검사에서는 해당 생성 필드의 누락을 허용한다. 생성 문자열을 채운 뒤 전체 스키마를 다시 검증하는 단계는 생성 경로에 없다. 자동 생성 대상 추출은 루트 properties만 순회하므로 중첩 속성에 생성 설정을 등록할 수 있다는 사실과 실제 생성 동작을 구분해야 한다. [required 예외](../../internal/kernel/schema.go#L184), [생성 순서](../../internal/kernel/rhiza_object.go#L178)

관계 경로의 중간 hop은 같은 workspace의 outgoing relation key를 조회하며 여러 간선이 있으면 `r.id` 순서의 첫 간선을 택한다. 최종 필드는 string/number/boolean이어야 하고 누락·미해결 경로·비 scalar 값은 실패한다. 중간 hop 쿼리는 관계 정의의 보관 여부를 필터링하지 않는다. 최종 저장 객체는 활성 여부와 값·버전을 확인하고 commit guard를 추가한다. Work 내 신규 객체도 별도 pending map으로 참조한다. [경로 해석](../../internal/kernel/rhiza_object.go#L46)

카운터 갱신, 객체 삽입, `generated_field_values`의 예약, 관계와 필드 색인은 같은 명령의 원자 적용에 포함된다. 생성 값의 유일 키는 `(workspace,type,field,generated_value)`이며 reset scope는 여기에 포함되지 않는다. 날짜가 바뀌어 같은 문자열을 다시 만들면 제약에 걸릴 수 있다. 객체 보관은 예약을 해제하지 않는다. Work 제출의 여러 생성은 [예약 묶음](../../internal/kernel/work_sequences.go#L1)을 사용한다. [생성 저장](../../internal/kernel/rhiza_object.go#L259), [생성 값 DDL](../../internal/store/schema.sql#L207)

## 6. 관계와 카디널리티

관계 정의를 만들거나 보관하면 양 endpoint 타입의 활성 incident 관계를 `key,id` 순서로 정렬해 `relation_schema.links`를 다시 만든다. 각 링크는 `{id,key,from_object_type_id,to_object_type_id,cardinality,required}`이다. 두 타입의 `version`과 `schema_version`을 증가시키고 불변 리비전을 추가한다. 자기 참조 관계는 같은 타입을 한 번 갱신한다. 이 부수 변경은 주 명령 receipt의 aggregate가 아니라 타입 조회와 리비전에서 확인한다. [정의·보관](../../internal/kernel/rhiza.go#L429), [스키마 재생성](../../internal/kernel/rhiza.go#L486)

| cardinality | 유일 슬롯 side | 허용 범위 |
|---|---|---|
| `one_to_one` | from, to | 각 출발·도착 객체가 해당 정의의 간선을 하나씩만 가진다. |
| `one_to_many` | to | 출발은 여러 도착을 가질 수 있고 도착은 하나의 출발에만 속한다. |
| `many_to_one` | from | 출발은 하나의 도착만 가지며 도착은 여러 출발을 받을 수 있다. |
| `many_to_many` | 없음 | 양쪽에 여러 간선이 가능하나 동일 쌍 중복은 금지한다. |

[슬롯 선택](../../internal/kernel/executor.go#L961), [유일 제약·허용 side 트리거](../../internal/store/schema.sql#L140)

`relation.link`는 활성 정의와 활성 양 endpoint를 읽어 정의의 타입과 일치하는지 검사한다. 일치하지 않으면 `schema_mismatch`이고, 정의/endpoint가 없으면 `precondition_failed`이다. 성공 시 간선과 필요한 슬롯을 삽입한다. [link](../../internal/kernel/rhiza_object.go#L380)

`object.create.relations`는 `{관계 key: 도착 객체 ID}` 형태이다. 각 값은 비어 있지 않은 문자열이어야 하고, 출발 타입에 맞는 활성 정의와 도착 타입을 검사한다. 배열은 받지 않으므로 한 번의 inline 입력에서 같은 key의 복수 도착을 표현할 수 없다. 출발 타입의 활성 `required:true` 정의는 모두 제공해야 한다. [inline 생성](../../internal/kernel/rhiza_object.go#L111)

`relation.unlink`는 target 간선과 payload 정의 ID의 일치를 확인한다. required 정의라면 같은 정의·출발 객체의 간선 수가 1 이하일 때 거부한다. 성공 시 슬롯과 간선을 물리 삭제하고, receipt aggregate 버전은 삭제 전+1, effect는 `{id,deleted:true}`이다. 삭제한 간선은 새 멱등 키로 다시 unlink할 수 없지만 같은 성공 명령의 재전송은 ledger로 복구한다. required 여부는 unlink 시 정의 조회에서 보관 여부와 무관하게 읽는다. [unlink](../../internal/kernel/rhiza_object.go#L413)

`required`는 모든 기존 객체의 전역 불변식이 아니다. 관계 정의 추가 시 기존 객체를 소급 연결하지 않으며, 객체 보관이나 정의 보관에서 필수 연결 전체를 재검사하지 않는다. 정의 endpoint 타입 조회도 활성 타입만으로 제한하지 않는다. [정의 endpoint 조회](../../internal/kernel/rhiza.go#L486), [보관 구현](../../internal/kernel/rhiza.go#L238)

## 7. 수명 주기와 삭제

`object_type.delete`, `object.delete`, `relation_definition.delete`는 `archived_at`, 수정 시각·principal, 버전을 갱신하는 보관 명령이다. `hard:true`도 등록 입력으로 받지만 현재 실행 함수는 hard/reason에 따른 물리 삭제 분기를 하지 않는다. 복원 명령은 등록되어 있지 않다. [객체 보관](../../internal/kernel/rhiza.go#L238), [타입·관계 정의 보관](../../internal/kernel/rhiza.go#L448), [SQLite 보관](../../internal/kernel/executor.go#L877)

보관은 객체 간선, 슬롯, 생성 값 예약, 필드 색인, 과거 리비전을 cascade 삭제하지 않는다. 따라서 보관한 객체가 차지한 슬롯도 남는다. 타입 보관은 인스턴스 보관과 다르고, 일반 객체 읽기는 타입의 `archived_at`을 검사하지 않는 반면 타입별 목록은 검사한다. 관계 목록은 활성 정의와 활성 상대 객체를 요구하므로 저장된 간선 중 일부가 응답에서 숨겨질 수 있다. [일반 읽기](../../internal/server/server.go#L468), [타입별 목록](../../internal/server/object_resources.go#L151), [관계 읽기](../../internal/server/definition_reads.go#L89)

## 8. 리소스 HTTP API

아래 경로의 공통 접두사는 `/api/v1/workspaces/{workspace_id}`이다. 등록된 객체·타입 리소스 경로와 범용 명령을 구분한다. 타입/관계 정의의 쓰기는 앞의 커널 명령으로 수행한다. [라우트](../../internal/server/server.go#L90)

| 메서드·상대 경로 | 요청 | 응답·동작 |
|---|---|---|
| `GET /objects/{object_id}` | workspace와 ID | `{data,consistency}`. 없거나 보관된 객체는 404. |
| `GET /objects` | 선택 `object_type`(타입 key) | 활성 객체를 `updated_at DESC,id`로 정렬. 실제 limit/offset 제한 없이 전체를 반환하고 `page.next_cursor=null`. |
| `POST /objects` | `{object_ids:[...]}` 최대 100개 | **생성이 아닌 배치 읽기**. 입력 순서를 따라 존재하는 활성 객체만 반환하고 미존재는 생략. 중복 입력은 중복 결과가 될 수 있다. |
| `GET /object-types/{object_type_id}/objects` | `filter` 정확히 한 개, limit/offset | workspace·타입·활성 객체·필터 범위를 SQL에서 먼저 제한한 뒤 페이지 처리. |
| `POST /object-types/{object_type_id}/objects` | `{title,values?,idempotency_key,expected_version?}` | 활성 타입을 찾아 key로 `object.create` 생성. title/멱등 키는 비공백 필수. expected_version은 생성 명령에 전달하지 않음. |
| `PATCH /object-types/{object_type_id}/objects/{object_id}` | `{title?,values?,idempotency_key,expected_version}` | 멱등 키, 양수 버전, 비공백 title 또는 non-null values가 필요. 경로의 타입·객체 소속 확인 후 `object.update`. |
| `DELETE /object-types/{object_type_id}/objects/{object_id}` | `{idempotency_key,expected_version}` | 양수 버전 필요. `object.delete`에 `hard:false`를 전달한다. |
| `GET /objects/{object_id}/relations` | direction=`both`(기본)/`incoming`/`outgoing`, 선택 `relation_definition_id`, limit/offset | 실제 저장된 간선과 `related_object`를 반환. 기준 객체가 없으면 404. |
| `GET /object-types/{object_type_id}/schema-revisions` | 선택 before, limit | schema_version 내림차순, `schema_version<before`. `{data,page.next_cursor}`. |

[일반·배치 읽기](../../internal/server/server.go#L455), [타입별 변경](../../internal/server/object_resources.go#L194), [관계 읽기](../../internal/server/definition_reads.go#L89), [리비전 읽기](../../internal/server/server.go#L800)

### 8.1 범위 필터와 읽기 응답

타입별 GET의 filter 문법은 `(id = "a" || id = "b")`, `(1 = 0)` 또는 두 그룹의 `&&` 교집합만 지원한다. 일반적인 필드 조건이나 임의 괄호 식은 지원하지 않는다. filter는 최대 65,536 bytes, 각 그룹 최대 199개 ID이며 두 그룹의 ID 수 합계는 최대 200개다. ID 중복도 수에 포함된다. 필터 누락·중복 query parameter·잘못된 문법은 HTTP 400이다. [파서](../../internal/server/object_resources.go#L20), [목록](../../internal/server/object_resources.go#L138)

타입별 목록과 관계 목록의 limit은 기본 50, 범위 1~100이고 offset은 기본 0이다. 한 행을 더 읽어서 후속 결과가 있으면 `next_cursor=offset+limit`을 돌려준다. 다음 요청은 offset 파라미터로 이어간다. `boundedInt`는 잘못된 수나 범위 밖 값을 기본값으로 되돌린다. 리비전 목록은 `before`에 다음 schema_version을 사용한다. before를 생략하면 `1<<62`이며 정수 파싱 오류는 별도 400 처리 없이 파싱 결과를 사용한다. [페이지 유틸리티](../../internal/server/server.go#L1080), [리비전 페이지](../../internal/server/server.go#L800)

객체 data는 `{id,object_type_id,object_type_key,object_type_schema_version,title,values,state_key,version,created_at,updated_at}`이다. 관계 data는 `{id,relation_definition_id,relation_key,from_object_id,to_object_id,direction,version,created_at,updated_at,related_object:{id,title,object_type_key,version}}`이다. 자기 참조 간선의 응답 direction은 CASE 식상 `outgoing`이다. [객체 shape](../../internal/server/server.go#L468), [관계 shape](../../internal/server/definition_reads.go#L112)

일반 단건 읽기는 응답에 `canonical_linearizable`을 표시하지만 실제 저장소 호출은 `ReadLocal`이다. 목록·배치·관계 읽기는 `canonical_stale_bounded`를 표시하며 `ReadLocal`을 사용한다. 이 문서는 단건 응답 라벨만으로 선형 일관성이 실행 검증되었다고 주장하지 않는다. [단건 구현](../../internal/server/server.go#L455)

### 8.2 타입별 쓰기의 엄격한 body와 재전송

타입별 POST/PATCH/DELETE는 최대 1 MiB body, 미등록 필드 거부, 뒤따르는 추가 JSON 값 거부, 중첩 100 초과 거부를 적용한다. 모든 객체 레벨의 중복 JSON key를 거부하고, 최상위 키는 대소문자 충돌도 거부한다. 공통 body 구조에 title/values가 있으므로 DELETE가 이들을 받아도 실행 payload에는 포함하지 않는다. [decoder](../../internal/server/object_resources.go#L258)

타입별 명령 ID는 `[workspace,principal,idempotency_key]` JSON 배열의 SHA-256으로 `cmd_typed_<hex>`를 만든다. PATCH/DELETE는 target 객체에 대한 expected_version precondition을 자동 생성한다. 타입·객체 소속 확인은 보관 여부를 제외하지 않으므로 보관 후 DELETE 재전송도 ledger까지 도달할 수 있다. 생성 재전송은 먼저 활성 타입을 조회하므로 타입이 보관되었으면 duplicate receipt 이전에 404가 될 수 있다. 같은 멱등 키를 생성·수정·삭제에 재사용하면 생성된 command ID는 같더라도 다른 커널 명령이므로 충돌한다. [명령 생성·소속 확인](../../internal/server/object_resources.go#L338)

## 9. 주요 흐름과 원자 저장

1. **타입 정의 → 생성:** 멤버가 타입을 정의하면 첫 스키마 리비전이 함께 생긴다. 객체 생성은 최신 필드 검사, 번호 계산, 필수 inline 관계 검사 후 객체·간선·슬롯·번호 예약·scalar 필드 색인을 함께 저장한다. 어느 제약이든 실패하면 원자 명령 전체가 실패한다.
2. **스키마 진화 → 기존 객체 수정:** 진화는 기존 객체를 일괄 갱신하지 않는다. title만 수정하면 기존 pin을 유지하고 values 교체가 들어오면 최신 스키마로 검사하여 pin을 바꾼다. 새 required 필드를 충족하지 못하면 실패한다.
3. **관계 연결 → 해제:** 정의/endpoint 타입과 버전을 확인하고 간선·슬롯을 생성한다. 카디널리티 충돌은 저장소 제약으로 거부한다. required의 마지막 outgoing 간선을 unlink하면 실패한다.
4. **적용 후 재전송:** ledger가 같은 명령을 찾으면 원래 aggregate·projection sequence를 재사용한다. 새 버전 검사나 번호 발급을 반복하지 않는다. 다른 내용은 충돌한다.

[객체 계획](../../internal/kernel/rhiza_object.go#L167), [관계 계획](../../internal/kernel/rhiza_object.go#L380), [중복 처리](../../internal/kernel/rhiza.go#L554)

SQLite 경로는 트랜잭션에서 변경과 기록을 수행하고, Rhiza 경로는 선형 읽기로 계획한 뒤 expected row 수와 이전 값 guard를 포함한 SQL 묶음을 원자 적용한다. Rhiza commit에는 workspace projection sequence 증가, ledger receipt, activity, outbox, projection event 기록이 포함된다. 한 원자 명령이 64 SQL 문을 넘으면 `validation_failed`로 거부하고 분할 적용하지 않는다. 동시 변경은 `precondition_failed`, DB 제약 실패는 `validation_failed`로 변환하며 적용 실패 후 ledger를 다시 확인하여 동시 중복 요청에 수렴한다. [SQLite 실행](../../internal/kernel/executor.go#L59), [Rhiza commit](../../internal/kernel/rhiza.go#L499)

필드 색인은 루트 scalar(string/number/boolean/null)만 저장하고 배열·객체 값은 건너뛴다. 수정 시 기존 색인을 지우고 다시 만든다. 객체 payload와 스키마 리비전이 원본이며 필드 색인이 중첩 JSON 전체를 대신하지 않는다. [색인 작성](../../internal/kernel/rhiza_object.go#L16), [색인·번호 DDL](../../internal/store/schema.sql#L190)

## 10. 테스트 코드 근거와 확인 공백

아래는 테스트 본문에 정의된 assertion의 근거이며, 이번 작업에서 통과를 확인한 목록이 아니다.

| 근거 | 테스트가 확인하도록 작성된 계약 |
|---|---|
| [SQLite 번호·관계 스키마](../../internal/kernel/executor_test.go#L14) | 날짜/sequence 생성 번호, 파생 관계 스키마와 digest |
| [객체 schema pin](../../internal/kernel/executor_test.go#L104) | 제목 수정의 pin 유지와 values 교체의 최신 리비전 pin |
| [Rhiza 타입·관계 수명 주기](../../internal/kernel/rhiza_test.go#L142) | define/evolve, 관계로 인한 타입 버전 증가, 보관, ledger/activity/outbox/projection 기록 수 |
| [Rhiza 실패 rollback](../../internal/kernel/rhiza_test.go#L218) | 잘못된 스키마·실패 명령의 원자성 |
| [동시 duplicate](../../internal/kernel/rhiza_test.go#L299) | 같은 명령의 동시 재전송 수렴 |
| [incident·자기 참조 관계](../../internal/kernel/rhiza_test.go#L349) | 관계 스키마 재생성 시 기존 incident 링크 유지 |
| [객체·간선 수명 주기](../../internal/kernel/rhiza_test.go#L608) | 생성·수정·연결/해제·보관의 Rhiza 경로 |
| [함수를 통한 상태 이동](../../internal/kernel/rhiza_test.go#L406) | kernel_command 함수가 object.move를 호출한 뒤 상태·버전 변경 |
| [filter 파서](../../internal/server/object_resources_test.go#L20) | 허용/거부 필터와 escaping·범위 제한 |
| [타입별 페이지 범위](../../internal/server/object_resources_test.go#L59) | workspace/type/filter를 페이지 처리 전에 제한 |
| [타입별 변경](../../internal/server/object_resources_test.go#L105) | 경로 identity, expected_version과 엄격한 body 검사 |
| [재전송](../../internal/server/object_resources_test.go#L205) | typed 명령 재전송·내용 충돌과 저장된 durable effect 불변 |
| [관계 조회](../../internal/server/definition_reads_test.go#L40) | 방향·정의·workspace 범위 |
| [Work sequence 예약](../../internal/kernel/work_sequences_test.go#L38) | 기존/새 scope 예약, overflow, 최종 guarded mutation |

실행하지 않았으므로 동시성, rollback, 재전송, 저장소별 일치 여부는 **실행 검증 공백**이다. 읽기 consistency 라벨과 실제 `ReadLocal`의 차이, hard 삭제 미분기, 일반 readOnly 비집행, 중첩 generation 미생성, 생성 후 재검증 부재, required의 비소급성, 보관 시 슬롯·예약 유지 등은 위 소스에서 확인한 **현재 구현의 제한**이다. 이 문서가 이러한 제한의 개선 방향을 결정하지는 않는다.

## 11. 소스 지도와 문서 검증 범위

| 소스 | 역할 |
|---|---|
| [registry.go](../../internal/kernel/registry.go#L1) | 명령 이름, payload와 target, 필수 precondition 종류 |
| [types.go](../../internal/kernel/types.go#L1) | 봉투·actor·receipt 타입 |
| [schema.go](../../internal/kernel/schema.go#L1) | 필드·상태 머신·번호 설정 검사와 렌더링 |
| [executor.go](../../internal/kernel/executor.go#L59) | 저장소 분기, SQLite 실행, actor attribution·precondition·슬롯·digest |
| [rhiza.go](../../internal/kernel/rhiza.go#L18) | Rhiza 타입·리비전·관계 정의·보관·commit·멱등성 |
| [rhiza_object.go](../../internal/kernel/rhiza_object.go#L16) | 객체·번호·상태·inline 관계·간선·필드 색인 |
| [schema.sql](../../internal/store/schema.sql#L18) | 불변 리비전, 외래 키·유일 키·슬롯·번호 저장 제약 |
| [server.go](../../internal/server/server.go#L88) | 공개 라우트·범용 명령·일반/배치 읽기·리비전 읽기·오류 |
| [object_resources.go](../../internal/server/object_resources.go#L20) | 타입별 목록·filter·쓰기 body·커널 명령 생성 |
| [definition_reads.go](../../internal/server/definition_reads.go#L89) | 저장된 관계 간선 조회 |
| [server.go](../../internal/server/server.go#L54) | HTTP 고정 `local` attribution |

문서 검증은 인용한 소스와 테스트 코드의 정적 대조, 상대 링크 파일 존재 및 줄 번호 범위 확인으로 한정한다. 제품 코드는 수정하지 않으며 다른 작성자의 문서와 기존 변경도 이 문서의 작성 범위에 포함하지 않는다.

*Drafted with EGM — 현재 구현 역명세.*
