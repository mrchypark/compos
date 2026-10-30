# Graph 적용 점검 — 2026-09-05

## 결론

현재 Graph가 필요한 구현은 Ontology의 고유 노드 도달성이다. 이미 있는
Rhiza projection과 bounded native adjacency API를 유지하고, 실제 발견한
누락 노드·최신성·읽기 버전 검증 부족을 보완했다. 새 Graph 저장소나 중복
projection을 추가하지 않았다.

| 구현 영역 | 판단 | 근거 |
| --- | --- | --- |
| Ontology `reachable-node-ids` | Graph 유지·읽기 경계 보강 | 단일 snapshot의 고유 도달성, 깊이·결과·간선 예산 지원 |
| Ontology `traverse` | SQL 유지 | 방향·predecessor·predicate·cardinality·중복 경로 계약 필요 |
| Object relation 생성·해제·경로 참조 | canonical SQL 유지 | 선택된 relation ID와 version을 같은 Kernel transaction에서 재검증 |
| Workflow cycle 검사·실행 | immutable Spec + DFS/Journal 유지 | projection 지연을 업무 전이와 게시 검증에 사용할 수 없음 |
| Grounding identity·권한 | canonical SQL 유지 | 최신 ObjectType 및 membership/capability의 원자적 검증 필요 |
| Knowledge 전문 검색 | Meilisearch 유지 | graph 탐색과 검색은 별도 기능 |

## 구현한 보완

- `Projection.Reachable`가 global schema와 workspace cursor 존재를 검증한다.
- 서버의 cached head만 믿지 않고 SQL Knowledge event head를 linearizable로
  읽어 graph cursor와 정확히 일치하는지 확인한다.
- graph state, canonical head, native traversal을 동일 applied slot에 묶는다.
  사이에 쓰기가 발생하면 `503 stale_projection`으로 실패 닫기한다.
- `StartFound=false`를 `ErrConceptNotFound`로 처리한다. canonical Concept가
  있는데 graph에서 누락된 경우 HTTP 200 빈 목록으로 숨기지 않는다.
- HTTP reader는 로컬 reconciler가 갱신 중일 때 기다리거나 중간 결과를 읽지
  않고 `503 stale_projection`을 반환한다. 쓰기 응답은 여전히 projection을
  기다리지 않는다.

관련 구현: `internal/rhizaprojection/projection.go`,
`internal/server/knowledge.go`, `internal/server/rhiza_projection.go`.

## 경로 탐색 전환의 선행 조건

현재 Rhiza `GraphQuery`는 한 번에 10,000행/16MiB 제한이며 호출 간 snapshot
고정 요청을 제공하지 않는다. 노드별 이웃 조회를 여러 번 조합하면 다른
시점의 간선을 한 경로로 연결할 수 있다. SQL 경로 API의 100,000 expansion
계약을 단순 치환해서는 안 된다. 전환하려면 방향·간선 속성·중복 경로와
작업 예산을 지원하는 단일 snapshot traversal 또는 동등한 버전 고정 API,
SQL과의 차등 테스트, 실제 비용 개선 근거가 먼저 필요하다.

## 검증 범위와 남은 경계

실제 embedded Rhiza로 정상 도달성, missing start, missing workspace cursor,
낮거나 높은 cursor, schema 불일치, workspace/bundle 격리, 결과/간선 예산,
HTTP 갱신 중 차단을 회귀 검증한다. SQL path/grounding/권한 계약은 변경하지 않는다.

이 변경은 새 Graph 소비자를 추가하거나 multi-projector HA를 승인하지 않는다.
여러 프로세스의 동시 rebuild, 부분 실패/재시작의 원자적 세대 발행, 동일
request ID를 재사용하는 rebuild의 복구 멱등성은 추가 검증·설계가 필요하다.
로컬 mutex와 applied-slot 일치만으로 이 경계가 해결됐다고 주장하지 않는다.
따라서 health의 `production_approved=false`는 유지한다.

이번 요청은 코드 적용 범위이며, 운영 배포와 Git commit/push는 하지 않았다.
