# tg900 SearchSyncer 검증 — 2026-09-05

Historical verification record: Knowledge support described here has since been removed; Knowledge is externally owned.

Compos·Conductor·SearchSyncer 모두 Ready 1/1. 기존 `compos` 네임스페이스의
인덱스와 Syncer는 변경하지 않았다. 빌드·푸시는 tg900 BuildKit 사용.

## 배포 확인

- Compos image: `sha256:24884bcc7a44a1983b35231090441c3f99f89b36fc1d19476ce2a938150f45b2`
- Syncer image: `sha256:da28ad46f135ec27898450fb1e26e151c85e3817d41547127fe1734ab7482c5b`
- Workspace: `wks_dd5cb4f2-11d7-452b-857c-773ac0763811`
- 검색 인덱스: `compos-tg900-objects`, 엔진 Meilisearch 1.12.8
- 기본 객체 2개 snapshot 완료(sequence 0) → 이벤트 polling 전환 확인.
- Syncer만 재시작한 뒤 snapshot 복구 → 이벤트 polling 재전환 확인.
- 실제 인덱스에서 Workspace 필터 + `Compos` 검색 결과 1개 확인.
- Compos health의 `search_backend=meilisearch` 확인.

| 배포된 인증 경계 | 결과 |
|---|---|
| Syncer 토큰의 Workspace discovery | 200 |
| Syncer 토큰으로 명령 호출 | 403 |
| Syncer 토큰으로 다른 Workspace projection 읽기 | 403 |
| 토큰 없는 discovery | 401 |
| Meilisearch 인덱싱 키로 기존 인덱스 접근·검색 실행 | 각각 403 |
| Meilisearch 검색 키로 문서 쓰기 | 403 |

키는 [Meilisearch의 action/index scope](https://www.meilisearch.com/docs/reference/api/keys/create-api-key)를
사용해 분리했고 Kubernetes Secret에만 전달했다.

## 실제 엔진 E2E

`cmd/search-syncer/meilisearch_e2e_test.go`에서 로컬 임시 Rhiza와 실제 Compos HTTP/Kernel,
실제 Syncer 함수를 tg900 Meilisearch의 격리된 임시 인덱스에 연결했다.
운영 Workspace의 사용자 세션을 위조하거나 인증을 비활성화하지 않았다.
테스트용 로컬 Compos는 auth-none이며, 운영 PocketBase 로그인 E2E를 의미하지 않는다.

| 시나리오 | 결과 |
|---|---|
| 초기 snapshot, 이벤트 기반 Object 생성·수정·삭제 | 통과 |
| Knowledge 등록·삭제, 두 Workspace의 동일 Knowledge ID 격리 | 통과 |
| 한글 `가나다`, 접두어 `가나`, 초성 `ㄱㄴㄷ` | 통과 |
| 혼합 `가ㄴ`, 자모 `ㄱㅏㄴㅏ` | 통과 |
| Workspace 필터, Knowledge type 필터, 정렬·페이지 처리 | 통과 |
| 임시 인덱스 삭제 후 snapshot 재구축, 새 SyncState 복구 | 통과 |

기존 `internal/korean`의 `SearchFields`, `MixedSearchFields`, `ExtractChosung`,
`DecomposeJamo`를 재사용한다. 별도 `NormalizeQuery` 함수는 현재 검색 API에서
호출하지 않으며, 위 검색들은 인덱스 측 표현 확장과 원문 쿼리로 통과했다.

실제 엔진에서 발견한 `core:search`의 `invalid_document_id`는 Workspace+Knowledge ID의
SHA-256 검색 ID로 해결했다. 원래 ID는 `knowledge_id`에 보존하고 삭제도 같은 매핑을 사용한다.

`go test ./...`, `go vet ./...`, Syncer 인증 race test, `git diff --check` 통과.
단위 회귀로 sequence 0, snapshot 실패, 이벤트 실패 시 cursor 유지, discovery 대상 변경을 확인했다.

## 제한

- 이벤트 수신은 push가 아니라 기본 5초 HTTP polling이다. 초기 rollout 중 연결 timeout
  1회가 있었으나 다음 주기에 복구됐고, 재시작 검증에서도 정상 동작했다.
- 실제 patch-data 사용자 토큰 로그인은 이번 검증에 포함되지 않았다.
- Compos는 기존 승인대로 emptyDir이며 이번 이미지 rollout으로 Workspace가 재생성됐다.
  이전 데이터·세션을 복구할 별도 백업은 없다.
- Meilisearch Pod 자체의 초기화는 수행하지 않았다. ephemeral 데이터가 초기화되면
  scoped API 키도 재발급해야 하며 인덱스 재구축만으로 키가 복구되지는 않는다.
- 환경변수 interval/batch의 잘못된 값 검증은 기존 운영 보완 사항으로 남는다.
  현재 manifest는 각각 5초·100건이다.
