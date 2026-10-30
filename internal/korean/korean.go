// Package korean provides Korean language search normalization for Compos.
//
// It generates multiple search representations (original, chosung, jamo) at
// index time and normalizes queries at search time, enabling prefix-based
// chosung and mixed-script search without modifying the search engine.
package korean

import (
	"strings"
	"unicode"
)

const (
	hangulBase     = 0xAC00
	hangulLast     = 0xD7A3
	chosungCount   = 19
	jungseongCount = 21
	jongseongCount = 28
)

var chosung = []rune{
	'ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ',
	'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ',
	'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ',
	'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ',
}

var jungseong = []rune{
	'ㅏ', 'ㅐ', 'ㅑ', 'ㅒ', 'ㅓ',
	'ㅔ', 'ㅕ', 'ㅖ', 'ㅗ', 'ㅘ',
	'ㅙ', 'ㅚ', 'ㅛ', 'ㅜ', 'ㅝ',
	'ㅞ', 'ㅟ', 'ㅠ', 'ㅡ', 'ㅢ',
	'ㅣ',
}

var jongseong = []rune{
	0, 'ㄱ', 'ㄲ', 'ㄳ', 'ㄴ',
	'ㄵ', 'ㄶ', 'ㄷ', 'ㄹ', 'ㄺ',
	'ㄻ', 'ㄼ', 'ㄽ', 'ㄾ', 'ㄿ',
	'ㅀ', 'ㅁ', 'ㅂ', 'ㅄ', 'ㅅ',
	'ㅆ', 'ㅇ', 'ㅈ', 'ㅊ', 'ㅋ',
	'ㅌ', 'ㅍ', 'ㅎ',
}

// Decomposed holds the jamo components of a single Hangul syllable.
type Decomposed struct {
	Chosung   rune // 초성 (initial consonant)
	Jungseong rune // 중声 (medial vowel)
	Jongseong rune // 종성 (final consonant), 0 if none
}

// IsHangul checks if r is a Hangul syllable (U+AC00-U+D7A3).
func IsHangul(r rune) bool {
	return r >= hangulBase && r <= hangulLast
}

// Decompose breaks a Hangul syllable into its jamo components.
// Returns Decomposed{0, 0, 0} if r is not a Hangul syllable.
func Decompose(r rune) Decomposed {
	if !IsHangul(r) {
		return Decomposed{}
	}
	s := int(r - hangulBase)
	return Decomposed{
		Chosung:   chosung[s/(jungseongCount*jongseongCount)],
		Jungseong: jungseong[(s%(jungseongCount*jongseongCount))/jongseongCount],
		Jongseong: jongseong[s%jongseongCount],
	}
}

// Compose joins jamo components back into a Hangul syllable.
func Compose(d Decomposed) rune {
	ci := 0
	for i, r := range chosung {
		if r == d.Chosung {
			ci = i
			break
		}
	}
	vi := 0
	for i, r := range jungseong {
		if r == d.Jungseong {
			vi = i
			break
		}
	}
	ti := 0
	if d.Jongseong != 0 {
		for i, r := range jongseong {
			if r == d.Jongseong {
				ti = i
				break
			}
		}
	}
	return rune(hangulBase + ci*(jungseongCount*jongseongCount) + vi*jongseongCount + ti)
}

// ExtractChosung returns only the initial consonants of Hangul syllables.
// Non-Hangul characters are passed through unchanged.
func ExtractChosung(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if IsHangul(r) {
			d := Decompose(r)
			b.WriteRune(d.Chosung)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// DecomposeJamo fully decomposes Hangul syllables into jamo.
// Non-Hangul characters are passed through unchanged.
func DecomposeJamo(s string) string {
	var b strings.Builder
	b.Grow(len(s) * 2)
	for _, r := range s {
		if IsHangul(r) {
			d := Decompose(r)
			b.WriteRune(d.Chosung)
			b.WriteRune(d.Jungseong)
			if d.Jongseong != 0 {
				b.WriteRune(d.Jongseong)
			}
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// SearchFields generates search representations for indexing.
// Returns a slice of normalized strings to be used as searchable fields.
// Handles space-separated names by processing each part separately.
func SearchFields(s string) []string {
	seen := make(map[string]bool)
	var fields []string

	add := func(v string) {
		if !seen[v] {
			seen[v] = true
			fields = append(fields, v)
		}
	}

	// 원본 전체
	add(s)

	// 공백으로 분리하여 각 부분에 대해 처리
	parts := strings.Fields(s)
	if len(parts) > 1 {
		for _, part := range parts {
			add(part)
			add(ExtractChosung(part))
			add(DecomposeJamo(part))
		}
	}

	// 전체 문자열에 대해 처리
	add(ExtractChosung(s))
	add(DecomposeJamo(s))

	return fields
}

// MixedSearchFields generates search representations including mixed-script
// prefixes. For example, "박찬엽" generates:
//
//	박, 박찬, 박찬엽 (완성형 prefix)
//	ㅂ, ㅂㅊ, ㅂㅊㅇ (초성 prefix)
//	박ㅊ, 박ㅊㅇ, 박찬ㅇ (혼합 prefix)
func MixedSearchFields(s string) []string {
	seen := make(map[string]bool)
	var fields []string

	add := func(v string) {
		if !seen[v] {
			seen[v] = true
			fields = append(fields, v)
		}
	}

	// 공백으로 분리하여 각 부분에 대해 처리
	parts := strings.Fields(s)
	if len(parts) > 1 {
		for _, part := range parts {
			// 각 부분에 대해 MixedSearchFields 재귀 호출
			for _, f := range MixedSearchFields(part) {
				add(f)
			}
		}
		// 전체 문자열도 추가
		add(s)
		return fields
	}

	runes := []rune(s)

	// 완성형 prefix
	for i := 1; i <= len(runes); i++ {
		add(string(runes[:i]))
	}

	// 초성 전체
	fullChosung := ExtractChosung(s)
	add(fullChosung)

	// 초성 prefix
	chosungRunes := []rune(fullChosung)
	for i := 1; i <= len(chosungRunes); i++ {
		add(string(chosungRunes[:i]))
	}

	// 혼합 prefix: 완성형 앞부분 + 초성 나머지
	for i := 0; i < len(runes); i++ {
		if !IsHangul(runes[i]) {
			continue
		}
		prefix := string(runes[:i+1])
		rest := string(runes[i+1:])
		if rest == "" {
			continue
		}
		// 뒷부분의 초성 전체
		suffixChosung := ExtractChosung(rest)
		if suffixChosung != "" {
			add(prefix + suffixChosung)
		}
		// 뒷부분의 첫 번째 초성만 (단일 초성 혼합)
		firstChosung := ExtractChosung(string([]rune{runes[i+1]}))
		if firstChosung != "" && firstChosung != suffixChosung {
			add(prefix + firstChosung)
		}
	}

	return fields
}

// NormalizeQuery expands a search query into multiple representations
// for matching against the indexed search fields.
func NormalizeQuery(q string) []string {
	seen := make(map[string]bool)
	var queries []string

	add := func(v string) {
		v = strings.TrimSpace(v)
		if v != "" && !seen[v] {
			seen[v] = true
			queries = append(queries, v)
		}
	}

	// 원본 쿼리
	add(q)

	// 초성 변환
	chosung := ExtractChosung(q)
	if chosung != q {
		add(chosung)
	}

	// 자모 분해
	jamo := DecomposeJamo(q)
	if jamo != q && jamo != chosung {
		add(jamo)
	}

	// 혼합 패턴: 완성형 음절 + 초성 매칭
	// 예: "박ㅊ" → ["박ㅊ", "ㅂㅊ"]
	runes := []rune(q)
	for i := 0; i < len(runes); i++ {
		if !IsHangul(runes[i]) {
			continue
		}
		// 앞부분은 완성형, 뒷부분은 초성으로 변환
		prefix := string(runes[:i+1])
		rest := string(runes[i+1:])
		if rest == "" {
			continue
		}
		// 뒷부분이 이미 초성인지 확인
		allChosung := true
		for _, r := range rest {
			if !isChosung(r) {
				allChosung = false
				break
			}
		}
		if allChosung {
			// 완성형 prefix + 초성 suffix → 초성 전체로도 변환
			restChosung := rest
			fullQuery := prefix + restChosung
			add(fullQuery)
			// 초성으로도 변환
			prefixChosung := ExtractChosung(prefix)
			add(prefixChosung + restChosung)
		}
	}

	return queries
}

func isChosung(r rune) bool {
	for _, c := range chosung {
		if r == c {
			return true
		}
	}
	return false
}

// ContainsChosung checks if the string contains any chosung characters.
func ContainsChosung(s string) bool {
	for _, r := range s {
		if isChosung(r) {
			return true
		}
	}
	return false
}

// IsMixedScript checks if the string contains both Hangul syllables
// and chosung characters (mixed script).
func IsMixedScript(s string) bool {
	hasSyllable := false
	hasChosung := false
	for _, r := range s {
		if IsHangul(r) {
			hasSyllable = true
		}
		if isChosung(r) {
			hasChosung = true
		}
		if hasSyllable && hasChosung {
			return true
		}
	}
	return false
}

// NormalizeIndexFields generates all search fields for a document.
// It returns a map of field names to their normalized values.
func NormalizeIndexFields(name, title string, extraFields ...string) map[string]interface{} {
	fields := make(map[string]interface{})

	// 원본
	if name != "" {
		fields["name"] = name
	}
	if title != "" {
		fields["title"] = title
	}

	// 검색용 필드 생성
	primary := name
	if primary == "" {
		primary = title
	}
	if primary != "" {
		fields["name_chosung"] = ExtractChosung(primary)
		fields["name_jamo"] = DecomposeJamo(primary)
		fields["name_search"] = SearchFields(primary)
		fields["name_mixed"] = MixedSearchFields(primary)
	}

	// 추가 필드 처리
	for i := 0; i < len(extraFields)-1; i += 2 {
		key := extraFields[i]
		val := extraFields[i+1]
		if val != "" {
			fields[key+"_chosung"] = ExtractChosung(val)
			fields[key+"_search"] = SearchFields(val)
		}
	}

	return fields
}

// HangulToASCII provides a rough romanization for fallback matching.
// Not intended for display, only for search normalization.
func HangulToASCII(s string) string {
	var b strings.Builder
	b.Grow(len(s) * 2)
	for _, r := range s {
		if IsHangul(r) {
			d := Decompose(r)
			b.WriteString(chosungASCIIMap[d.Chosung])
			b.WriteString(jungseongASCIIMap[d.Jungseong])
			if d.Jongseong != 0 {
				b.WriteString(jongseongASCIIMap[d.Jongseong])
			}
		} else if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

var chosungASCIIMap = map[rune]string{
	'ㄱ': "g", 'ㄲ': "k", 'ㄴ': "n", 'ㄷ': "d", 'ㄸ': "t",
	'ㄹ': "r", 'ㅁ': "m", 'ㅂ': "b", 'ㅃ': "p", 'ㅅ': "s",
	'ㅆ': "s", 'ㅇ': "", 'ㅈ': "j", 'ㅉ': "j", 'ㅊ': "c",
	'ㅋ': "k", 'ㅌ': "t", 'ㅍ': "p", 'ㅎ': "h",
}

var jungseongASCIIMap = map[rune]string{
	'ㅏ': "a", 'ㅐ': "e", 'ㅑ': "ya", 'ㅒ': "ye",
	'ㅓ': "eo", 'ㅔ': "e", 'ㅕ': "yeo", 'ㅖ': "ye",
	'ㅗ': "o", 'ㅘ': "wa", 'ㅙ': "we", 'ㅚ': "we",
	'ㅛ': "yo", 'ㅜ': "u", 'ㅝ': "wo", 'ㅞ': "we",
	'ㅟ': "wi", 'ㅠ': "yu", 'ㅡ': "eu", 'ㅢ': "ui",
	'ㅣ': "i",
}

var jongseongASCIIMap = map[rune]string{
	'ㄱ': "k", 'ㄲ': "k", 'ㄳ': "k", 'ㄴ': "n", 'ㄵ': "n",
	'ㄶ': "n", 'ㄷ': "t", 'ㄹ': "l", 'ㄺ': "k", 'ㄻ': "m",
	'ㄼ': "p", 'ㄽ': "s", 'ㄾ': "t", 'ㄿ': "p", 'ㅀ': "l",
	'ㅁ': "m", 'ㅂ': "p", 'ㅄ': "p", 'ㅅ': "t", 'ㅆ': "t",
	'ㅇ': "ng", 'ㅈ': "t", 'ㅊ': "t", 'ㅋ': "k", 'ㅌ': "t",
	'ㅍ': "p", 'ㅎ': "h",
}
