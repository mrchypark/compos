package kernel

import (
	"encoding/json"
	"fmt"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var tokenPattern = regexp.MustCompile(`\{\{(today:[A-Za-z0-9_-]+|sequence:[1-9][0-9]*|field:[A-Za-z_][A-Za-z0-9_]*|relation:[A-Za-z_][A-Za-z0-9_.]*)\}\}`)
var relationTokenPattern = regexp.MustCompile(`\{\{relation:([A-Za-z_][A-Za-z0-9_.]*)\}\}`)

func validateFieldSchema(raw json.RawMessage) (map[string]any, error) {
	var s map[string]any
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, reject("schema_mismatch", "field_schema must be a JSON object")
	}
	if _, exists := s["type"]; !exists {
		s["type"] = "object"
	}
	if s["type"] != "object" {
		return nil, reject("schema_mismatch", "field_schema.type must be object")
	}
	if err := validateRegisteredSchema("field_schema", s, 0); err != nil {
		return nil, err
	}
	if machine, exists := s["state_machine"]; exists {
		if err := validateStateMachine(machine); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func validateRegisteredSchema(path string, s map[string]any, depth int) error {
	if depth > 32 {
		return reject("schema_mismatch", path+" exceeds maximum nesting depth")
	}
	allowed := map[string]bool{"type": true, "properties": true, "required": true, "items": true, "additionalProperties": true, "enum": true, "minLength": true, "maxLength": true, "minimum": true, "maximum": true, "pattern": true, "format": true, "readOnly": true, "state_machine": true, "x-compos-generation": true}
	for k := range s {
		if !allowed[k] {
			return reject("schema_mismatch", path+" uses unsupported keyword "+k)
		}
	}
	if path != "field_schema" {
		if _, exists := s["state_machine"]; exists {
			return reject("schema_mismatch", path+" cannot declare a state_machine")
		}
	}
	types, err := schemaTypes(s["type"])
	if err != nil {
		return reject("schema_mismatch", path+" "+err.Error())
	}
	has := func(kind string) bool {
		for _, candidate := range types {
			if candidate == kind {
				return true
			}
		}
		return false
	}
	if gen, exists := s["x-compos-generation"]; exists {
		if !has("string") {
			return reject("schema_mismatch", path+" generation is supported only for string fields")
		}
		g, ok := gen.(map[string]any)
		if !ok {
			return reject("schema_mismatch", path+" generation must be an object")
		}
		for k := range g {
			if k != "version" && k != "on" && k != "template" && k != "timezone" && k != "reset" && k != "unique" {
				return reject("schema_mismatch", path+" generation uses unsupported property "+k)
			}
		}
		template, ok := g["template"].(string)
		if !ok || template == "" {
			return reject("schema_mismatch", path+" generation template is required")
		}
		if reset, _ := g["reset"].(string); reset != "" && reset != "never" && reset != "day" && reset != "month" && reset != "year" {
			return reject("schema_mismatch", path+" generation reset must be one of never, day, month, year")
		}
		if version, exists := g["version"]; exists {
			n, ok := number(version)
			if !ok || n != 1 {
				return reject("schema_mismatch", path+" generation version must be 1")
			}
		}
		if on, exists := g["on"]; exists && on != "create" {
			return reject("schema_mismatch", path+" generation on must be create")
		}
		if unique, exists := g["unique"]; exists {
			if _, ok := unique.(bool); !ok {
				return reject("schema_mismatch", path+" generation unique must be boolean")
			}
		}
		if zone, _ := g["timezone"].(string); zone != "" {
			if _, err := time.LoadLocation(zone); err != nil {
				return reject("schema_mismatch", path+" generation timezone is invalid")
			}
		}
		if strings.Contains(template, "{{") {
			stripped := tokenPattern.ReplaceAllString(template, "")
			if strings.Contains(stripped, "{{") || strings.Contains(stripped, "}}") {
				return reject("schema_mismatch", path+" generation template contains an unsupported token")
			}
		}
	}
	if pattern, ok := s["pattern"].(string); ok {
		if _, err := regexp.Compile(pattern); err != nil {
			return reject("schema_mismatch", path+" has invalid pattern")
		}
	}
	if format, ok := s["format"].(string); ok && strings.HasPrefix(format, "regex:") {
		if _, err := regexp.Compile(strings.TrimPrefix(format, "regex:")); err != nil {
			return reject("schema_mismatch", path+" has invalid regex format")
		}
	}
	if format, ok := s["format"].(string); ok && !strings.HasPrefix(format, "regex:") && !map[string]bool{"date": true, "date-time": true, "email": true, "uri": true}[format] {
		return reject("schema_mismatch", path+" has unsupported format "+format)
	}
	if additional, exists := s["additionalProperties"]; exists {
		if _, ok := additional.(bool); !ok {
			return reject("schema_mismatch", path+" additionalProperties must be boolean")
		}
	}
	for _, key := range []string{"minLength", "maxLength"} {
		if raw, exists := s[key]; exists {
			n, ok := integer(raw)
			if !ok || n < 0 {
				return reject("schema_mismatch", path+" "+key+" must be a non-negative integer")
			}
		}
	}
	if min, minOK := integer(s["minLength"]); minOK {
		if max, maxOK := integer(s["maxLength"]); maxOK && min > max {
			return reject("schema_mismatch", path+" minLength exceeds maxLength")
		}
	}
	if min, minOK := number(s["minimum"]); minOK {
		if max, maxOK := number(s["maximum"]); maxOK && min > max {
			return reject("schema_mismatch", path+" minimum exceeds maximum")
		}
	}
	if values, ok := s["enum"].([]any); ok && len(values) == 0 {
		return reject("schema_mismatch", path+" enum must not be empty")
	}
	if has("object") {
		props, _ := s["properties"].(map[string]any)
		for name, raw := range props {
			child, ok := raw.(map[string]any)
			if !ok {
				return reject("schema_mismatch", path+".properties."+name+" must be an object")
			}
			if err := validateRegisteredSchema(path+".properties."+name, child, depth+1); err != nil {
				return err
			}
		}
		if req, ok := s["required"].([]any); ok {
			for _, raw := range req {
				name, ok := raw.(string)
				if !ok {
					return reject("schema_mismatch", path+".required must contain strings")
				}
				if _, exists := props[name]; !exists {
					return reject("schema_mismatch", path+".required names an undeclared property "+name)
				}
			}
		}
	}
	if has("array") {
		child, ok := s["items"].(map[string]any)
		if !ok {
			return reject("schema_mismatch", path+".items is required for arrays")
		}
		return validateRegisteredSchema(path+".items", child, depth+1)
	}
	return nil
}

func validateValues(values map[string]any, schema map[string]any, path string) error {
	props, _ := schema["properties"].(map[string]any)
	if required, ok := schema["required"].([]any); ok {
		for _, raw := range required {
			name, _ := raw.(string)
			if _, exists := values[name]; !exists {
				if field, ok := props[name].(map[string]any); ok {
					if _, generated := field["x-compos-generation"]; generated {
						continue
					}
				}
				return reject("schema_mismatch", path+"."+name+" is required")
			}
		}
	}
	if schema["additionalProperties"] == false {
		for name := range values {
			if _, exists := props[name]; !exists {
				return reject("schema_mismatch", path+"."+name+" is not declared")
			}
		}
	}
	for name, value := range values {
		raw, exists := props[name]
		if !exists {
			continue
		}
		field, _ := raw.(map[string]any)
		if err := validateFieldValue(value, field, path+"."+name); err != nil {
			return err
		}
	}
	return nil
}

func validateFieldValue(value any, field map[string]any, path string) error {
	if !matchesType(value, field["type"]) {
		return reject("schema_mismatch", path+" has the wrong type")
	}
	if enums, ok := field["enum"].([]any); ok {
		found := false
		for _, candidate := range enums {
			if equalJSON(value, candidate) {
				found = true
				break
			}
		}
		if !found {
			return reject("schema_mismatch", path+" is not an allowed value")
		}
	}
	if text, ok := value.(string); ok {
		if min, ok := integer(field["minLength"]); ok && int64(len([]rune(text))) < min {
			return reject("schema_mismatch", path+" is shorter than minLength")
		}
		if max, ok := integer(field["maxLength"]); ok && int64(len([]rune(text))) > max {
			return reject("schema_mismatch", path+" is longer than maxLength")
		}
		if pattern, ok := field["pattern"].(string); ok {
			re, _ := regexp.Compile(pattern)
			if !re.MatchString(text) {
				return reject("schema_mismatch", path+" does not match pattern")
			}
		}
		if format, ok := field["format"].(string); ok && strings.HasPrefix(format, "regex:") {
			re, _ := regexp.Compile(strings.TrimPrefix(format, "regex:"))
			if !re.MatchString(text) {
				return reject("schema_mismatch", path+" does not match format")
			}
		}
		if format, ok := field["format"].(string); ok && !strings.HasPrefix(format, "regex:") {
			valid := true
			switch format {
			case "date":
				_, err := time.Parse("2006-01-02", text)
				valid = err == nil
			case "date-time":
				_, err := time.Parse(time.RFC3339, text)
				valid = err == nil
			case "email":
				_, err := mail.ParseAddress(text)
				valid = err == nil
			case "uri":
				u, err := url.ParseRequestURI(text)
				valid = err == nil && u.Scheme != ""
			}
			if !valid {
				return reject("schema_mismatch", path+" does not match format")
			}
		}
	}
	if n, ok := number(value); ok {
		if min, ok := number(field["minimum"]); ok && n < min {
			return reject("schema_mismatch", path+" is below minimum")
		}
		if max, ok := number(field["maximum"]); ok && n > max {
			return reject("schema_mismatch", path+" exceeds maximum")
		}
	}
	if nested, ok := value.(map[string]any); ok {
		return validateValues(nested, field, path)
	}
	if list, ok := value.([]any); ok {
		item, _ := field["items"].(map[string]any)
		for i, v := range list {
			if err := validateFieldValue(v, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func schemaTypes(raw any) ([]string, error) {
	allowed := map[string]bool{"object": true, "array": true, "string": true, "number": true, "integer": true, "boolean": true, "null": true}
	if kind, ok := raw.(string); ok {
		if !allowed[kind] {
			return nil, fmt.Errorf("has unsupported type %s", kind)
		}
		return []string{kind}, nil
	}
	values, ok := raw.([]any)
	if !ok || len(values) == 0 {
		return nil, fmt.Errorf("type must be a string or non-empty array")
	}
	out := make([]string, 0, len(values))
	for _, raw := range values {
		kind, ok := raw.(string)
		if !ok || !allowed[kind] {
			return nil, fmt.Errorf("has an unsupported type")
		}
		out = append(out, kind)
	}
	return out, nil
}
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), n == float64(int64(n))
	default:
		return 0, false
	}
}

func validateStateMachine(raw any) error {
	machine, ok := raw.(map[string]any)
	if !ok {
		return reject("schema_mismatch", "state_machine must be an object")
	}
	initial, ok := machine["initial"].(string)
	if !ok || strings.TrimSpace(initial) == "" {
		return reject("schema_mismatch", "state_machine.initial must be a non-empty string")
	}
	transitions, ok := machine["transitions"].(map[string]any)
	if !ok {
		return reject("schema_mismatch", "state_machine.transitions must be an object")
	}
	if _, ok := transitions[initial]; !ok {
		return reject("schema_mismatch", "state_machine.initial must be listed in transitions")
	}
	for from, rawTargets := range transitions {
		if strings.TrimSpace(from) == "" || from == "*" {
			return reject("schema_mismatch", "state_machine transition keys must be concrete states")
		}
		targets, ok := rawTargets.([]any)
		if !ok {
			return reject("schema_mismatch", "state_machine transition values must be arrays")
		}
		seen := map[string]bool{}
		for _, rawTarget := range targets {
			target, ok := rawTarget.(string)
			if !ok || target == "" {
				return reject("schema_mismatch", "state_machine targets must be non-empty strings")
			}
			if target != "*" {
				if _, ok := transitions[target]; !ok {
					return reject("schema_mismatch", "state_machine target must be listed in transitions")
				}
			}
			if seen[target] {
				return reject("schema_mismatch", "state_machine targets must not repeat")
			}
			seen[target] = true
		}
	}
	return nil
}

type generation struct{ Field, Template, Timezone, Reset string }

func generations(schema map[string]any) []generation {
	props, _ := schema["properties"].(map[string]any)
	out := []generation{}
	for name, raw := range props {
		field, _ := raw.(map[string]any)
		g, ok := field["x-compos-generation"].(map[string]any)
		if !ok {
			continue
		}
		zone, _ := g["timezone"].(string)
		if zone == "" {
			zone = "UTC"
		}
		reset, _ := g["reset"].(string)
		if reset == "" {
			reset = "never"
		}
		out = append(out, generation{name, g["template"].(string), zone, reset})
	}
	return out
}

func renderGenerated(g generation, now time.Time, values map[string]any, sequence int64) (string, string, error) {
	loc, err := time.LoadLocation(g.Timezone)
	if err != nil {
		return "", "", err
	}
	local := now.In(loc)
	scope := "never"
	switch g.Reset {
	case "day":
		scope = local.Format("2006-01-02")
	case "month":
		scope = local.Format("2006-01")
	case "year":
		scope = local.Format("2006")
	}
	result := tokenPattern.ReplaceAllStringFunc(g.Template, func(token string) string {
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(token, "{{"), "}}"), ":")
		switch parts[0] {
		case "today":
			format := strings.NewReplacer("YYYY", "2006", "YY", "06", "MM", "01", "DD", "02").Replace(parts[1])
			return local.Format(format)
		case "sequence":
			n, _ := strconv.Atoi(parts[1])
			return fmt.Sprintf("%0*d", n, sequence)
		case "field":
			return fmt.Sprint(values[parts[1]])
		case "relation":
			return fmt.Sprint(values["relation:"+parts[1]])
		}
		return token
	})
	return result, scope, nil
}
