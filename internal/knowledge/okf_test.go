package knowledge

import (
	"strings"
	"testing"
)

func TestParseOKFDocument(t *testing.T) {
	source := "---\r\ntype: Ontology Concept\r\ntitle: Customer\r\nunknown: keep-me\r\ncompos:\r\n  relations:\r\n    - predicate: hasOwner\r\n      target: people/person\r\n      cardinality: many_to_one\r\n  grounds:\r\n    - entity_definition: customers\r\n      mapping:\r\n        name: display_name\r\n---\r\nSee [Person](../people/person.md) and [website](https://example.com).\r\n"
	doc, err := Parse("sales/customer.md", source)
	if err != nil {
		t.Fatal(err)
	}
	if doc.ConceptID != "sales/customer" || doc.ConceptType != "Ontology Concept" || doc.SourceMarkdown != source {
		t.Fatalf("unexpected document: %#v", doc)
	}
	if doc.Frontmatter["unknown"] != "keep-me" || !strings.HasPrefix(doc.ContentHash, "sha256:") {
		t.Fatalf("frontmatter/hash not preserved: %#v", doc.Frontmatter)
	}
	if len(doc.Relations) != 3 || doc.Relations[0].Predicate != "hasOwner" || doc.Relations[1].TargetRef != "people/person" || !doc.Relations[2].TargetIsExternal {
		t.Fatalf("relations=%#v", doc.Relations)
	}
	if len(doc.Groundings) != 1 || doc.Groundings[0].Predicate != "represented_by" || doc.Groundings[0].Mapping["name"] != "display_name" {
		t.Fatalf("groundings=%#v", doc.Groundings)
	}
}

func TestParseOKFRejectsInvalidInput(t *testing.T) {
	deep := "type: concept\nnested: " + strings.Repeat("[", 34) + "x" + strings.Repeat("]", 34)
	for name, input := range map[string][2]string{
		"path traversal": {"../customer.md", "---\ntype: concept\n---\n"},
		"path delimiter": {"customer|person.md", "---\ntype: concept\n---\n"},
		"missing type":   {"customer.md", "---\ntitle: Customer\n---\n"},
		"bad yaml":       {"customer.md", "---\ntype: [\n---\n"},
		"cardinality":    {"customer.md", "---\ntype: concept\ncompos:\n  relations:\n    - predicate: owns\n      target: person\n      cardinality: sometimes\n---\n"},
		"deep":           {"customer.md", "---\n" + deep + "\n---\n"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(input[0], input[1]); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
