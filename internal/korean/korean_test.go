package korean

import (
	"reflect"
	"strings"
	"testing"
)

func TestExtractChosung(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"박찬엽", "ㅂㅊㅇ"},
		{"삼성전자", "ㅅㅅㅈㅈ"},
		{"스타벅스", "ㅅㅌㅂㅅ"},
		{"서울", "ㅅㅇ"},
		{"한국어", "ㅎㄱㅇ"},
		{"ㄱ", "ㄱ"},
		{"hello", "hello"},
		{"박", "ㅂ"},
		{"찬", "ㅊ"},
		{"엽", "ㅇ"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ExtractChosung(tt.input)
			if result != tt.expected {
				t.Errorf("ExtractChosung(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecomposeJamo(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"박찬엽", "ㅂㅏㄱㅊㅏㄴㅇㅕㅂ"},
		{"한국", "ㅎㅏㄴㄱㅜㄱ"},
		{"서울", "ㅅㅓㅇㅜㄹ"},
		{"ㄱ", "ㄱ"},
		{"hello", "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := DecomposeJamo(tt.input)
			if result != tt.expected {
				t.Errorf("DecomposeJamo(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSearchFields(t *testing.T) {
	fields := SearchFields("박찬엽")
	if len(fields) < 2 {
		t.Errorf("SearchFields(\"박찬엽\") returned %d fields, want >= 2", len(fields))
	}
	if fields[0] != "박찬엽" {
		t.Errorf("SearchFields[0] = %q, want \"박찬엽\"", fields[0])
	}
	if fields[1] != "ㅂㅊㅇ" {
		t.Errorf("SearchFields[1] = %q, want \"ㅂㅊㅇ\"", fields[1])
	}
}

func TestMixedSearchFields(t *testing.T) {
	fields := MixedSearchFields("박찬엽")
	fieldSet := make(map[string]bool)
	for _, f := range fields {
		fieldSet[f] = true
	}

	expected := []string{
		"박", "박찬", "박찬엽", // 완성형 prefix
		"ㅂ", "ㅂㅊ", "ㅂㅊㅇ", // 초성 prefix
		"박ㅊ", "박ㅊㅇ", "박찬ㅇ", // 혼합 prefix
	}

	for _, exp := range expected {
		if !fieldSet[exp] {
			t.Errorf("MixedSearchFields(\"박찬엽\") missing %q", exp)
		}
	}
}

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains []string
	}{
		{
			name:     "full hangul",
			input:    "박찬엽",
			contains: []string{"박찬엽", "ㅂㅊㅇ"},
		},
		{
			name:     "chosung only",
			input:    "ㅂㅊㅇ",
			contains: []string{"ㅂㅊㅇ"},
		},
		{
			name:     "mixed script",
			input:    "박ㅊ",
			contains: []string{"박ㅊ", "ㅂㅊ"},
		},
		{
			name:     "partial chosung",
			input:    "ㅂㅊ",
			contains: []string{"ㅂㅊ"},
		},
		{
			name:     "partial hangul",
			input:    "박찬",
			contains: []string{"박찬", "ㅂㅊ"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := NormalizeQuery(tt.input)
			resultSet := make(map[string]bool)
			for _, r := range results {
				resultSet[r] = true
			}
			for _, exp := range tt.contains {
				if !resultSet[exp] {
					t.Errorf("NormalizeQuery(%q) = %v, missing %q", tt.input, results, exp)
				}
			}
		})
	}
}

func TestIsHangul(t *testing.T) {
	tests := []struct {
		input    rune
		expected bool
	}{
		{'가', true},
		{'힣', true},
		{'ㄱ', false}, // Hangul Jamo, not Syllable
		{'A', false},
		{'1', false},
	}

	for _, tt := range tests {
		if result := IsHangul(tt.input); result != tt.expected {
			t.Errorf("IsHangul(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestDecomposeCompose(t *testing.T) {
	// Round-trip test
	chars := []rune{'가', '나', '다', '라', '마', '박', '찬', '엽'}
	for _, r := range chars {
		d := Decompose(r)
		composed := Compose(d)
		if composed != r {
			t.Errorf("Round-trip failed: %q -> %+v -> %q", r, d, composed)
		}
	}
}

func TestIsMixedScript(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"박ㅊ", true},  // syllable + chosung
		{"ㅂㅊ", false}, // only chosung
		{"박찬", false}, // only syllables
		{"박찬엽", false},
		{"ㄱ", false},
	}

	for _, tt := range tests {
		if result := IsMixedScript(tt.input); result != tt.expected {
			t.Errorf("IsMixedScript(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestNormalizeIndexFields(t *testing.T) {
	fields := NormalizeIndexFields("박찬엽", "")

	if fields["name"] != "박찬엽" {
		t.Errorf("name = %q, want \"박찬엽\"", fields["name"])
	}
	if fields["name_chosung"] != "ㅂㅊㅇ" {
		t.Errorf("name_chosung = %q, want \"ㅂㅊㅇ\"", fields["name_chosung"])
	}
	if fields["name_jamo"] != "ㅂㅏㄱㅊㅏㄴㅇㅕㅂ" {
		t.Errorf("name_jamo = %q, want \"ㅂㅏㄱㅊㅏㄴㅇㅕㅂ\"", fields["name_jamo"])
	}

	searchFields, ok := fields["name_search"].([]string)
	if !ok {
		t.Fatalf("name_search is not []string")
	}
	if len(searchFields) < 2 {
		t.Errorf("name_search has %d fields, want >= 2", len(searchFields))
	}

	mixedFields, ok := fields["name_mixed"].([]string)
	if !ok {
		t.Fatalf("name_mixed is not []string")
	}
	if len(mixedFields) < 5 {
		t.Errorf("name_mixed has %d fields, want >= 5", len(mixedFields))
	}
}

func TestHangulToASCII(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"박찬엽", "bakcanyeop"},
		{"한국", "hanguk"},
		{"서울", "seoul"},
		{"hello", "hello"},
		{"테스트", "teseuteu"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := HangulToASCII(tt.input)
			if result != tt.expected {
				t.Errorf("HangulToASCII(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSearchFieldConsistency(t *testing.T) {
	// Verify that search fields can match normalized queries
	names := []string{"박찬엽", "삼성전자", "스타벅스 강남점", "카카오뱅크"}

	for _, name := range names {
		searchFields := MixedSearchFields(name)
		searchSet := make(map[string]bool)
		for _, f := range searchFields {
			searchSet[f] = true
		}

		// 공백으로 분리된 각 단어의 완성형 prefix가 모두 포함되어야 함
		parts := strings.Fields(name)
		for _, part := range parts {
			runes := []rune(part)
			for i := 1; i <= len(runes); i++ {
				prefix := string(runes[:i])
				if !searchSet[prefix] {
					t.Errorf("MixedSearchFields(%q) missing prefix %q from part %q", name, prefix, part)
				}
			}

			// 초성 전체가 포함되어야 함
			chosung := ExtractChosung(part)
			if !searchSet[chosung] {
				t.Errorf("MixedSearchFields(%q) missing chosung %q from part %q", name, chosung, part)
			}
		}
	}
}

func TestNormalizeQueryMatchesSearchFields(t *testing.T) {
	// Verify that normalized queries match the indexed search fields
	testCases := []struct {
		name  string
		query string
	}{
		{"박찬엽", "ㅂ"},
		{"박찬엽", "ㅂㅊ"},
		{"박찬엽", "ㅂㅊㅇ"},
		{"박찬엽", "박"},
		{"박찬엽", "박찬"},
		{"박찬엽", "박찬엽"},
		{"박찬엽", "박ㅊ"},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"_"+tc.query, func(t *testing.T) {
			searchFields := MixedSearchFields(tc.name)
			normalizedQueries := NormalizeQuery(tc.query)

			searchSet := make(map[string]bool)
			for _, f := range searchFields {
				searchSet[f] = true
			}

			found := false
			for _, q := range normalizedQueries {
				if searchSet[q] {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Query %q (normalized: %v) not found in search fields %v",
					tc.query, normalizedQueries, searchFields)
			}
		})
	}
}

func BenchmarkExtractChosung(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractChosung("박찬엽삼성전자스타벅스강남점")
	}
}

func BenchmarkMixedSearchFields(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MixedSearchFields("박찬엽삼성전자스타벅스강남점")
	}
}

func BenchmarkNormalizeQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NormalizeQuery("ㅂㅊㅇ")
	}
}

func TestSearchFieldsEqual(t *testing.T) {
	// Multiple representations of the same name should produce consistent search fields
	names := map[string]string{
		"박찬엽": "박찬엽",
		"김철수": "김철수",
	}

	for original, expected := range names {
		fields := SearchFields(original)
		if fields[0] != expected {
			t.Errorf("SearchFields(%q)[0] = %q, want %q", original, fields[0], expected)
		}
	}
}

func TestDecomposeEdgeCases(t *testing.T) {
	// Test edge cases
	d := Decompose('가') // First Hangul syllable
	if d.Chosung != 'ㄱ' || d.Jungseong != 'ㅏ' || d.Jongseong != 0 {
		t.Errorf("Decompose('가') = %+v, want {ㄱ, ㅏ, 0}", d)
	}

	d = Decompose('힣') // Last Hangul syllable
	if d.Chosung != 'ㅎ' || d.Jungseong != 'ㅣ' || d.Jongseong != 'ㅎ' {
		t.Errorf("Decompose('힣') = %+v, want {ㅎ, ㅣ, ㅎ}", d)
	}

	d = Decompose('ㄱ') // Not a syllable
	if d.Chosung != 0 {
		t.Errorf("Decompose('ㄱ') = %+v, want zero value", d)
	}
}

func TestSearchFieldsType(t *testing.T) {
	fields := SearchFields("박찬엽")
	if !reflect.TypeOf(fields).AssignableTo(reflect.TypeOf([]string{})) {
		t.Error("SearchFields should return []string")
	}
}
