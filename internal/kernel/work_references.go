package kernel

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// workPendingObject is a submission-local identity.  It is never persisted;
// the deterministic object ID is what makes a retried parent command converge.
type workPendingObject struct {
	ClientRef   string
	ID          string
	TypeID      string
	TypeKey     string
	ChangeIndex int
	Values      map[string]any
	Relations   map[string]any
}

type workReferencePlan struct {
	changes []workChange
	objects map[string]*workPendingObject
}

func (p workReferencePlan) objectsByID() map[string]*workPendingObject {
	objects := make(map[string]*workPendingObject, len(p.objects))
	for _, object := range p.objects {
		objects[object.ID] = object
	}
	return objects
}

func planWorkReferences(parent Command, changes []workChange) (workReferencePlan, error) {
	objects := make(map[string]*workPendingObject)
	for i, change := range changes {
		if change.ClientRef == "" {
			continue
		}
		if change.CommandType != "object.create" || !workflowIdentifier(change.ClientRef) {
			return workReferencePlan{}, workReferenceError(i, change, "client_ref", reject("schema_mismatch", fmt.Sprintf("payload.changes[%d].client_ref is only valid for object.create", i)))
		}
		if _, duplicate := objects[change.ClientRef]; duplicate {
			return workReferencePlan{}, workReferenceError(i, change, "client_ref", reject("validation_failed", "payload.changes has a duplicate client_ref: "+change.ClientRef))
		}
		child, err := childWorkChangeCommand(parent, i, change)
		if err != nil {
			return workReferencePlan{}, workReferenceError(i, change, "client_ref", err)
		}
		values, _ := change.Payload["values"].(map[string]any)
		relations, _ := change.Payload["relations"].(map[string]any)
		objects[change.ClientRef] = &workPendingObject{ClientRef: change.ClientRef, ID: aggregateID("obj", child.CommandID), TypeKey: stringValue(change.Payload["object_type_key"]), ChangeIndex: i, Values: values, Relations: relations}
	}
	if len(objects) == 0 {
		return workReferencePlan{changes: changes, objects: objects}, nil
	}

	dependencies := make(map[string]map[string]bool, len(objects))
	for ref := range objects {
		dependencies[ref] = map[string]bool{}
	}
	for i, change := range changes {
		if change.CommandType != "object.create" {
			continue
		}
		refs, err := workClientRefs(change.Payload["relations"])
		if err != nil {
			return workReferencePlan{}, workReferenceError(i, change, "relations", err)
		}
		if change.ClientRef == "" && len(refs) > 0 {
			return workReferencePlan{}, workReferenceError(i, change, "client_ref", reject("validation_failed", fmt.Sprintf("payload.changes[%d] references a new record but has no client_ref", i)))
		}
		for ref := range refs {
			if _, found := objects[ref]; !found {
				return workReferencePlan{}, workReferenceError(i, change, "relations", reject("validation_failed", fmt.Sprintf("payload.changes[%d] references unknown client_ref: %s", i, ref)))
			}
			dependencies[change.ClientRef][ref] = true
		}
	}

	orderedCreates, err := orderWorkCreates(objects, dependencies)
	if err != nil {
		return workReferencePlan{}, err
	}
	byIndex := make(map[int]workChange, len(changes))
	for i, change := range changes {
		resolved, err := resolveWorkChangeRefs(change, objects)
		if err != nil {
			return workReferencePlan{}, workReferenceError(i, change, "relations", err)
		}
		byIndex[i] = resolved
		if pending := objects[change.ClientRef]; pending != nil {
			pending.Values, _ = resolved.Payload["values"].(map[string]any)
			pending.Relations, _ = resolved.Payload["relations"].(map[string]any)
		}
	}
	ordered := make([]workChange, 0, len(changes))
	for _, ref := range orderedCreates {
		ordered = append(ordered, byIndex[objects[ref].ChangeIndex])
	}
	for i := range changes {
		if changes[i].CommandType == "object.create" && changes[i].ClientRef == "" {
			ordered = append(ordered, byIndex[i])
		}
	}
	for i := range changes {
		if changes[i].CommandType != "object.create" {
			ordered = append(ordered, byIndex[i])
		}
	}
	return workReferencePlan{changes: ordered, objects: objects}, nil
}

func workReferenceError(index int, change workChange, field string, err error) error {
	return workChangeError(err, index, change, field)
}

func orderWorkCreates(objects map[string]*workPendingObject, dependencies map[string]map[string]bool) ([]string, error) {
	remaining := make(map[string]map[string]bool, len(dependencies))
	for ref, dependencies := range dependencies {
		remaining[ref] = make(map[string]bool, len(dependencies))
		for dependency := range dependencies {
			remaining[ref][dependency] = true
		}
	}
	ordered := make([]string, 0, len(objects))
	for len(remaining) > 0 {
		ready := make([]string, 0)
		for ref, dependencies := range remaining {
			if len(dependencies) == 0 {
				ready = append(ready, ref)
			}
		}
		if len(ready) == 0 {
			return nil, reject("validation_failed", "payload.changes has cyclic new-record client_ref relations")
		}
		sort.Slice(ready, func(i, j int) bool { return objects[ready[i]].ChangeIndex < objects[ready[j]].ChangeIndex })
		for _, ref := range ready {
			ordered = append(ordered, ref)
			delete(remaining, ref)
			for _, dependencies := range remaining {
				delete(dependencies, ref)
			}
		}
	}
	return ordered, nil
}

func resolveWorkChangeRefs(change workChange, objects map[string]*workPendingObject) (workChange, error) {
	copy, err := copyWorkChange(change)
	if err != nil {
		return workChange{}, err
	}
	if copy.CommandType == "object.create" {
		if relations, exists := copy.Payload["relations"]; exists {
			resolved, err := resolveWorkClientRefs(relations, objects)
			if err != nil {
				return workChange{}, err
			}
			copy.Payload["relations"] = resolved
		}
	}
	if copy.CommandType == "relation.link" || copy.CommandType == "relation.unlink" {
		for _, key := range []string{"from_object_id", "to_object_id"} {
			if raw, exists := copy.Payload[key]; exists {
				resolved, err := resolveWorkClientRefs(raw, objects)
				if err != nil {
					return workChange{}, err
				}
				copy.Payload[key] = resolved
				if id, ok := resolved.(string); ok && copy.CommandType == "relation.link" {
					for _, object := range objects {
						if object.ID == id {
							copy.Preconditions = append(copy.Preconditions, Precondition{AggregateType: "object_instance", AggregateID: id, ExpectedVersion: 1})
							break
						}
					}
				}
			}
		}
	}
	return copy, nil
}

func resolveWorkSubmissionRefs(value any, objects map[string]*workPendingObject) (any, error) {
	return resolveWorkClientRefs(value, objects)
}

func resolveWorkClientRefs(value any, objects map[string]*workPendingObject) (any, error) {
	switch value := value.(type) {
	case map[string]any:
		if raw, exists := value["client_ref"]; exists {
			if len(value) != 1 {
				return nil, reject("schema_mismatch", "client_ref reference must be a singleton object")
			}
			ref, ok := raw.(string)
			if !ok || !workflowIdentifier(ref) {
				return nil, reject("schema_mismatch", "client_ref reference is invalid")
			}
			pending := objects[ref]
			if pending == nil {
				return nil, reject("validation_failed", "unknown client_ref: "+ref)
			}
			return pending.ID, nil
		}
		result := make(map[string]any, len(value))
		for key, item := range value {
			resolved, err := resolveWorkClientRefs(item, objects)
			if err != nil {
				return nil, err
			}
			result[key] = resolved
		}
		return result, nil
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			resolved, err := resolveWorkClientRefs(item, objects)
			if err != nil {
				return nil, err
			}
			result[i] = resolved
		}
		return result, nil
	default:
		return value, nil
	}
}

func workClientRefs(value any) (map[string]bool, error) {
	refs := map[string]bool{}
	var visit func(any) error
	visit = func(value any) error {
		switch value := value.(type) {
		case map[string]any:
			if raw, exists := value["client_ref"]; exists {
				if len(value) != 1 {
					return reject("schema_mismatch", "client_ref reference must be a singleton object")
				}
				ref, ok := raw.(string)
				if !ok || !workflowIdentifier(ref) {
					return reject("schema_mismatch", "client_ref reference is invalid")
				}
				refs[ref] = true
				return nil
			}
			for _, item := range value {
				if err := visit(item); err != nil {
					return err
				}
			}
		case []any:
			for _, item := range value {
				if err := visit(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(value); err != nil {
		return nil, err
	}
	return refs, nil
}

func copyWorkChange(change workChange) (workChange, error) {
	raw, err := json.Marshal(change.Payload)
	if err != nil {
		return workChange{}, err
	}
	copy := change
	if err := json.Unmarshal(raw, &copy.Payload); err != nil {
		return workChange{}, err
	}
	return copy, nil
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
