package knowledge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const MaxDocumentBytes = 1 << 20

var markdownLink = regexp.MustCompile(`\[[^\]]*\]\(([^\s)]+)(?:\s+[^)]*)?\)`)

type Relation struct {
	Kind             string         `json:"kind"`
	Predicate        string         `json:"predicate"`
	TargetRef        string         `json:"target_ref"`
	TargetIsExternal bool           `json:"target_is_external"`
	Cardinality      string         `json:"cardinality,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
}

type Grounding struct {
	EntityDefinitionKey string         `json:"entity_definition"`
	Predicate           string         `json:"predicate"`
	Mapping             map[string]any `json:"mapping"`
}

type Document struct {
	ConceptID       string
	ConceptType     string
	Title           string
	Description     string
	Resource        string
	Frontmatter     map[string]any
	FrontmatterJSON string
	BodyMarkdown    string
	SourceMarkdown  string
	ContentHash     string
	Relations       []Relation
	Groundings      []Grounding
}

func Parse(sourcePath, source string) (Document, error) {
	if len(source) > MaxDocumentBytes {
		return Document{}, fmt.Errorf("OKF document exceeds %d bytes", MaxDocumentBytes)
	}
	if !utf8.ValidString(source) {
		return Document{}, errors.New("OKF document must be valid UTF-8")
	}
	conceptID, err := NormalizeConceptID(sourcePath)
	if err != nil {
		return Document{}, err
	}
	normalized := strings.ReplaceAll(source, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	if len(lines) < 3 || lines[0] != "---" {
		return Document{}, errors.New("OKF document must start with YAML frontmatter")
	}
	closing := -1
	for i := 1; i < len(lines); i++ {
		if lines[i] == "---" {
			closing = i
			break
		}
	}
	if closing < 0 {
		return Document{}, errors.New("OKF frontmatter closing delimiter is missing")
	}

	frontmatter := map[string]any{}
	if err := yaml.Unmarshal([]byte(strings.Join(lines[1:closing], "\n")), &frontmatter); err != nil {
		return Document{}, fmt.Errorf("parse OKF frontmatter: %w", err)
	}
	if nestingDepth(frontmatter) > 32 {
		return Document{}, errors.New("OKF frontmatter depth exceeds 32")
	}
	conceptType, err := optionalString(frontmatter, "type", true)
	if err != nil {
		return Document{}, err
	}
	title, err := optionalString(frontmatter, "title", false)
	if err != nil {
		return Document{}, err
	}
	description, err := optionalString(frontmatter, "description", false)
	if err != nil {
		return Document{}, err
	}
	resource, err := optionalString(frontmatter, "resource", false)
	if err != nil {
		return Document{}, err
	}
	frontmatterRaw, err := json.Marshal(frontmatter)
	if err != nil {
		return Document{}, fmt.Errorf("frontmatter must be JSON-compatible: %w", err)
	}

	body := strings.Join(lines[closing+1:], "\n")
	relations, groundings, err := extensions(frontmatter)
	if err != nil {
		return Document{}, err
	}
	relations = append(relations, markdownRelations(body, conceptID)...)
	return Document{
		ConceptID: conceptID, ConceptType: conceptType, Title: title,
		Description: description, Resource: resource, Frontmatter: frontmatter,
		FrontmatterJSON: string(frontmatterRaw), BodyMarkdown: body,
		SourceMarkdown: source, ContentHash: RevisionID(source),
		Relations: relations, Groundings: groundings,
	}, nil
}

func RevisionID(source string) string {
	sum := sha256.Sum256([]byte(source))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func NormalizeConceptID(value string) (string, error) {
	value = strings.ReplaceAll(strings.TrimSpace(value), "\\", "/")
	if value == "" || strings.HasPrefix(value, "/") || strings.ContainsAny(value, "?#|") {
		return "", errors.New("concept path must be relative and contain no query, fragment, or reserved delimiter")
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("concept path contains an invalid segment")
		}
	}
	if path.Ext(value) != ".md" {
		return "", errors.New("concept path must end in .md")
	}
	if base := path.Base(value); base == "index.md" || base == "log.md" {
		return "", errors.New("reserved OKF file is not a Concept")
	}
	conceptID := strings.TrimSuffix(path.Clean(value), ".md")
	if conceptID == "" {
		return "", errors.New("concept path must name a Concept")
	}
	return conceptID, nil
}

func optionalString(values map[string]any, key string, required bool) (string, error) {
	value, exists := values[key]
	if !exists {
		if required {
			return "", fmt.Errorf("OKF frontmatter %q is required", key)
		}
		return "", nil
	}
	text, ok := value.(string)
	if !ok || (required && strings.TrimSpace(text) == "") {
		return "", fmt.Errorf("OKF frontmatter %q must be %s", key, map[bool]string{true: "a non-empty string", false: "a string"}[required])
	}
	return text, nil
}

func extensions(frontmatter map[string]any) ([]Relation, []Grounding, error) {
	value, exists := frontmatter["compos"]
	if !exists {
		return nil, nil, nil
	}
	compos, ok := value.(map[string]any)
	if !ok {
		return nil, nil, errors.New("OKF compos extension must be an object")
	}
	var relations []Relation
	if raw, exists := compos["relations"]; exists {
		items, ok := raw.([]any)
		if !ok {
			return nil, nil, errors.New("compos.relations must be an array")
		}
		for i, rawItem := range items {
			item, ok := rawItem.(map[string]any)
			if !ok {
				return nil, nil, fmt.Errorf("compos.relations[%d] must be an object", i)
			}
			predicate, err := requiredExtensionString(item, "predicate", "relations", i)
			if err != nil {
				return nil, nil, err
			}
			target, err := requiredExtensionString(item, "target", "relations", i)
			if err != nil {
				return nil, nil, err
			}
			ref, external, err := normalizeReference(target, "")
			if err != nil {
				return nil, nil, fmt.Errorf("compos.relations[%d].target: %w", i, err)
			}
			cardinality, _ := item["cardinality"].(string)
			if cardinality != "" && !validCardinality(cardinality) {
				return nil, nil, fmt.Errorf("compos.relations[%d].cardinality is invalid", i)
			}
			metadata := map[string]any{}
			for key, value := range item {
				if key != "predicate" && key != "target" && key != "cardinality" {
					metadata[key] = value
				}
			}
			relations = append(relations, Relation{Kind: "ontology", Predicate: predicate, TargetRef: ref, TargetIsExternal: external, Cardinality: cardinality, Metadata: metadata})
		}
	}

	var groundings []Grounding
	if raw, exists := compos["grounds"]; exists {
		items, ok := raw.([]any)
		if !ok {
			return nil, nil, errors.New("compos.grounds must be an array")
		}
		for i, rawItem := range items {
			item, ok := rawItem.(map[string]any)
			if !ok {
				return nil, nil, fmt.Errorf("compos.grounds[%d] must be an object", i)
			}
			key, err := requiredExtensionString(item, "entity_definition", "grounds", i)
			if err != nil {
				return nil, nil, err
			}
			predicate, _ := item["predicate"].(string)
			if predicate == "" {
				predicate = "represented_by"
			}
			mapping := map[string]any{}
			if rawMapping, exists := item["mapping"]; exists {
				var ok bool
				mapping, ok = rawMapping.(map[string]any)
				if !ok {
					return nil, nil, fmt.Errorf("compos.grounds[%d].mapping must be an object", i)
				}
			}
			groundings = append(groundings, Grounding{EntityDefinitionKey: key, Predicate: predicate, Mapping: mapping})
		}
	}
	return relations, groundings, nil
}

func requiredExtensionString(item map[string]any, key, collection string, index int) (string, error) {
	value, ok := item[key].(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("compos.%s[%d].%s must be a non-empty string", collection, index, key)
	}
	return value, nil
}

func markdownRelations(body, conceptID string) []Relation {
	seen := map[string]bool{}
	var relations []Relation
	for _, match := range markdownLink.FindAllStringSubmatch(body, -1) {
		ref, external, err := normalizeReference(strings.Trim(match[1], "<>"), path.Dir(conceptID))
		if err != nil || seen[ref] {
			continue
		}
		seen[ref] = true
		relations = append(relations, Relation{Kind: "markdown", Predicate: "links_to", TargetRef: ref, TargetIsExternal: external})
	}
	return relations
}

func normalizeReference(raw, base string) (string, bool, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false, errors.New("invalid reference")
	}
	if u.IsAbs() || u.Host != "" || strings.HasPrefix(raw, "//") {
		return raw, true, nil
	}
	if u.Path == "" {
		return "", false, errors.New("reference path is empty")
	}
	ext := path.Ext(u.Path)
	if ext != "" && ext != ".md" {
		return "", false, errors.New("internal reference must target a Markdown Concept")
	}
	target := u.Path
	if base != "" {
		target = path.Join(base, target)
	}
	if ext == "" {
		target += ".md"
	}
	conceptID, err := NormalizeConceptID(target)
	return conceptID, false, err
}

func validCardinality(value string) bool {
	return value == "one_to_one" || value == "one_to_many" || value == "many_to_one" || value == "many_to_many"
}

func nestingDepth(value any) int {
	max := 0
	switch value := value.(type) {
	case map[string]any:
		for _, child := range value {
			if depth := nestingDepth(child); depth > max {
				max = depth
			}
		}
	case []any:
		for _, child := range value {
			if depth := nestingDepth(child); depth > max {
				max = depth
			}
		}
	default:
		return 0
	}
	return max + 1
}
