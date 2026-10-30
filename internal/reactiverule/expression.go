package reactiverule

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const maxDepth = 32

func ValidatePredicate(value map[string]any) error {
	raw, _ := json.Marshal(value)
	if len(raw) > 64<<10 {
		return errors.New("predicate exceeds 64 KiB")
	}
	return validateExpression(value, 0)
}

func Match(value map[string]any, data map[string]any) (bool, error) {
	result, err := evaluate(value, data, 0)
	matched, ok := result.(bool)
	return matched && ok, err
}

func ValidateTemplate(value any) error {
	raw, _ := json.Marshal(value)
	if len(raw) > 64<<10 {
		return errors.New("template exceeds 64 KiB")
	}
	return validateTemplate(value, 0)
}

func validateTemplate(value any, depth int) error {
	if depth > maxDepth {
		return errors.New("template depth exceeds 32")
	}
	switch value := value.(type) {
	case []any:
		for _, item := range value {
			if err := validateTemplate(item, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		if reference, exists := value["$from"]; exists {
			if len(value) != 1 {
				return errors.New("$from must be the only template property")
			}
			ref, ok := reference.(map[string]any)
			if !ok || len(ref) != 2 {
				return errors.New("$from requires source and path")
			}
			source, sourceOK := ref["source"].(string)
			path, pathOK := ref["path"].(string)
			if !sourceOK || !pathOK || (source != "event.payload" && source != "event.target" && source != "event.metadata") {
				return errors.New("$from source or path is invalid")
			}
			if path != "" && !strings.HasPrefix(path, "/") {
				return errors.New("$from path must be a JSON Pointer")
			}
			return nil
		}
		for _, item := range value {
			if err := validateTemplate(item, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateExpression(value any, depth int) error {
	if depth > maxDepth {
		return errors.New("predicate depth exceeds 32")
	}
	switch value := value.(type) {
	case []any:
		for _, item := range value {
			if err := validateExpression(item, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		if len(value) != 1 {
			return errors.New("predicate objects must contain one JsonLogic operator")
		}
		for operator, raw := range value {
			args, array := raw.([]any)
			switch operator {
			case "var":
				if _, ok := raw.(string); !ok && (!array || len(args) < 1 || len(args) > 2) {
					return errors.New("var requires a path or [path, default]")
				}
			case "!":
				if !array || len(args) != 1 {
					return errors.New("! requires one argument")
				}
			case "and", "or":
				if !array || len(args) == 0 {
					return fmt.Errorf("%s requires at least one argument", operator)
				}
			case "===", "==", "!==", "!=", ">", ">=", "<", "<=", "in":
				if !array || len(args) != 2 {
					return fmt.Errorf("%s requires two arguments", operator)
				}
			default:
				return fmt.Errorf("unsupported JsonLogic operator %q", operator)
			}
			if operator != "var" {
				if err := validateExpression(raw, depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func evaluate(value any, data map[string]any, depth int) (any, error) {
	if depth > maxDepth {
		return nil, errors.New("predicate depth exceeds 32")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return value, nil
	}
	if len(object) != 1 {
		return nil, errors.New("predicate objects must contain one operator")
	}
	for operator, raw := range object {
		if operator == "var" {
			path, fallback := "", any(nil)
			switch value := raw.(type) {
			case string:
				path = value
			case []any:
				if len(value) != 0 {
					path, _ = value[0].(string)
				}
				if len(value) == 2 {
					fallback = value[1]
				}
			}
			if result, found := dotted(data, path); found {
				return result, nil
			}
			return fallback, nil
		}
		args, _ := raw.([]any)
		values := make([]any, len(args))
		for i := range args {
			result, err := evaluate(args[i], data, depth+1)
			if err != nil {
				return nil, err
			}
			values[i] = result
		}
		switch operator {
		case "!":
			return !truthy(values[0]), nil
		case "and":
			for _, value := range values {
				if !truthy(value) {
					return value, nil
				}
			}
			return values[len(values)-1], nil
		case "or":
			for _, value := range values {
				if truthy(value) {
					return value, nil
				}
			}
			return values[len(values)-1], nil
		case "===", "==":
			return reflect.DeepEqual(values[0], values[1]), nil
		case "!==", "!=":
			return !reflect.DeepEqual(values[0], values[1]), nil
		case ">", ">=", "<", "<=":
			left, leftOK := numeric(values[0])
			right, rightOK := numeric(values[1])
			if !leftOK || !rightOK {
				return false, nil
			}
			return map[string]bool{">": left > right, ">=": left >= right, "<": left < right, "<=": left <= right}[operator], nil
		case "in":
			switch container := values[1].(type) {
			case string:
				text, ok := values[0].(string)
				return ok && strings.Contains(container, text), nil
			case []any:
				for _, item := range container {
					if reflect.DeepEqual(item, values[0]) {
						return true, nil
					}
				}
			}
			return false, nil
		}
	}
	return nil, errors.New("invalid predicate")
}

func dotted(value any, path string) (any, bool) {
	if path == "" {
		return value, true
	}
	current := value
	for _, part := range strings.Split(path, ".") {
		switch value := current.(type) {
		case map[string]any:
			current, _ = value[part]
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(value) {
				return nil, false
			}
			current = value[index]
		default:
			return nil, false
		}
		if current == nil {
			return nil, false
		}
	}
	return current, true
}

func truthy(value any) bool {
	switch value := value.(type) {
	case nil:
		return false
	case bool:
		return value
	case string:
		return value != ""
	case float64:
		return value != 0
	case int:
		return value != 0
	case int64:
		return value != 0
	default:
		return true
	}
}

func numeric(value any) (float64, bool) {
	switch value := value.(type) {
	case float64:
		return value, true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	default:
		return 0, false
	}
}
