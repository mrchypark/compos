package korean

import (
	"testing"
)

// TestE2EKoreanSearchFlow simulates the full search flow:
// Document indexing → Search field generation → Query normalization → Match verification
func TestE2EKoreanSearchFlow(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected bool
	}{
		// 완성형 검색
		{"박찬엽", "박찬엽", true},
		{"박찬엽", "박찬", true},
		{"박찬엽", "박", true},

		// 초성 검색
		{"박찬엽", "ㅂ", true},
		{"박찬엽", "ㅂㅊ", true},
		{"박찬엽", "ㅂㅊㅇ", true},

		// 혼합 검색
		{"박찬엽", "박ㅊ", true},
		{"박찬엽", "박찬ㅇ", true},
		{"박찬엽", "박ㅊㅇ", true},

		// 다른 이름
		{"삼성전자", "ㅅㅅ", true},
		{"삼성전자", "ㅅㅅㅈㅈ", true},
		{"삼성전자", "삼성", true},

		{"스타벅스강남점", "ㅅㅌㅂ", true},
		{"스타벅스강남점", "스타", true},
		{"스타벅스강남점", "강남", false}, // 부분 문자열은 접두사 검색에서 매칭 안 됨

		// 매칭 안 되는 경우
		{"박찬엽", "김", false},
		{"박찬엽", "ㅇㅇ", false},
		{"박찬엽", "ㅈㅈ", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"_"+tc.query, func(t *testing.T) {
			// 1. 문서 인덱싱 시 생성되는 검색 필드
			searchFields := MixedSearchFields(tc.name)
			searchSet := make(map[string]bool)
			for _, f := range searchFields {
				searchSet[f] = true
			}

			// 2. 검색 쿼리 노멀라이제이션
			normalized := NormalizeQuery(tc.query)
			querySet := make(map[string]bool)
			for _, q := range normalized {
				querySet[q] = true
			}

			// 3. 매칭 확인
			found := false
			for q := range querySet {
				if searchSet[q] {
					found = true
					break
				}
			}

			if found != tc.expected {
				t.Errorf("Query %q for %q: expected match=%v, got match=%v\n"+
					"  Search fields: %v\n"+
					"  Normalized queries: %v",
					tc.query, tc.name, tc.expected, found, searchFields, normalized)
			}
		})
	}
}

// TestE2EBenchmarkIndexFields measures performance of index field generation
func TestE2EBenchmarkIndexFields(t *testing.T) {
	names := []string{
		"박찬엽",
		"삼성전자",
		"스타벅스강남점",
		"카카오뱅크",
		"현대자동차그룹",
		"서울특별시",
		"대한민국정부",
	}

	for _, name := range names {
		fields := MixedSearchFields(name)
		if len(fields) < 5 {
			t.Errorf("MixedSearchFields(%q) returned %d fields, want >= 5", name, len(fields))
		}

		// 원본 prefix가 모두 포함되어야 함
		runes := []rune(name)
		for i := 1; i <= len(runes); i++ {
			prefix := string(runes[:i])
			found := false
			for _, f := range fields {
				if f == prefix {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("MixedSearchFields(%q) missing prefix %q", name, prefix)
			}
		}
	}
}

// TestE2EMixedScriptSearch validates mixed script search patterns
func TestE2EMixedScriptSearch(t *testing.T) {
	// 사용자가 "박ㅊ"라고 입력했을 때 "박찬엽"을 찾아야 함
	name := "박찬엽"
	query := "박ㅊ"

	searchFields := MixedSearchFields(name)
	normalized := NormalizeQuery(query)

	searchSet := make(map[string]bool)
	for _, f := range searchFields {
		searchSet[f] = true
	}

	found := false
	for _, q := range normalized {
		if searchSet[q] {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Mixed script search failed: query=%q, name=%q\n"+
			"  Search fields: %v\n"+
			"  Normalized queries: %v",
			query, name, searchFields, normalized)
	}
}

// TestE2EMultiWordSearch validates search with spaces
func TestE2EMultiWordSearch(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected bool
	}{
		{"스타벅스 강남점", "스타", true},
		{"스타벅스 강남점", "강남", true},
		{"스타벅스 강남점", "ㅅㅌㅂ", true},
		{"스타벅스 강남점", "ㄱㄴ", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"_"+tc.query, func(t *testing.T) {
			searchFields := MixedSearchFields(tc.name)
			normalized := NormalizeQuery(tc.query)

			searchSet := make(map[string]bool)
			for _, f := range searchFields {
				searchSet[f] = true
			}

			found := false
			for _, q := range normalized {
				if searchSet[q] {
					found = true
					break
				}
			}

			if found != tc.expected {
				t.Errorf("Query %q for %q: expected match=%v, got match=%v",
					tc.query, tc.name, tc.expected, found)
			}
		})
	}
}
