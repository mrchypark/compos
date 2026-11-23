PRAGMA foreign_keys = ON;

CREATE TABLE schema_metadata (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  identity TEXT NOT NULL,
  applied_at TEXT NOT NULL CHECK (applied_at GLOB '????-??-??T*Z')
) STRICT;

CREATE TABLE workspaces (
  id TEXT PRIMARY KEY,
  key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z')
) STRICT;

CREATE TABLE object_types (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  key TEXT NOT NULL,
  singular TEXT NOT NULL,
  plural TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
  field_schema_json TEXT NOT NULL CHECK (json_valid(field_schema_json) AND json_type(field_schema_json) = 'object'),
  relation_schema_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(relation_schema_json) AND json_type(relation_schema_json) = 'object'),
  state_machine_id TEXT,
  archived_at TEXT CHECK (archived_at IS NULL OR archived_at GLOB '????-??-??T*Z'),
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  updated_by_principal_id TEXT NOT NULL,
  UNIQUE (workspace_id, key),
  UNIQUE (workspace_id, id)
) STRICT;

CREATE TABLE object_type_schema_revisions (
  workspace_id TEXT NOT NULL,
  object_type_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
  field_schema_json TEXT NOT NULL CHECK (json_valid(field_schema_json) AND json_type(field_schema_json) = 'object'),
  relation_schema_json TEXT NOT NULL CHECK (json_valid(relation_schema_json) AND json_type(relation_schema_json) = 'object'),
  state_machine_id TEXT,
  digest TEXT NOT NULL CHECK (digest GLOB 'sha256:*'),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  PRIMARY KEY (workspace_id, object_type_id, schema_version),
  FOREIGN KEY (workspace_id, object_type_id) REFERENCES object_types(workspace_id, id)
) STRICT;

CREATE TRIGGER trg_object_type_schema_revisions_immutable_update
BEFORE UPDATE ON object_type_schema_revisions
BEGIN SELECT RAISE(ABORT, 'object type schema revisions are immutable'); END;
CREATE TRIGGER trg_object_type_schema_revisions_immutable_delete
BEFORE DELETE ON object_type_schema_revisions
BEGIN SELECT RAISE(ABORT, 'object type schema revisions are immutable'); END;

CREATE TABLE object_instances (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  object_type_id TEXT NOT NULL,
  object_type_schema_version INTEGER NOT NULL CHECK (object_type_schema_version >= 1),
  title TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json) AND json_type(payload_json) = 'object'),
  state_key TEXT,
  archived_at TEXT CHECK (archived_at IS NULL OR archived_at GLOB '????-??-??T*Z'),
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  updated_by_principal_id TEXT NOT NULL,
  UNIQUE (workspace_id, id),
  FOREIGN KEY (workspace_id, object_type_id) REFERENCES object_types(workspace_id, id),
  FOREIGN KEY (workspace_id, object_type_id, object_type_schema_version)
    REFERENCES object_type_schema_revisions(workspace_id, object_type_id, schema_version)
) STRICT;
CREATE INDEX idx_object_instances_workspace_type ON object_instances(workspace_id, object_type_id);
CREATE INDEX idx_object_instances_active_type ON object_instances(workspace_id, object_type_id, updated_at, id) WHERE archived_at IS NULL;

CREATE TRIGGER trg_object_instances_schema_pin_current_insert
BEFORE INSERT ON object_instances
WHEN NOT EXISTS (
  SELECT 1 FROM object_types t
  WHERE t.workspace_id = NEW.workspace_id AND t.id = NEW.object_type_id
    AND t.schema_version = NEW.object_type_schema_version
)
BEGIN SELECT RAISE(ABORT, 'object instance schema pin is not current'); END;

CREATE TRIGGER trg_object_instances_schema_pin_current_update
BEFORE UPDATE OF workspace_id, object_type_id, object_type_schema_version ON object_instances
WHEN NOT EXISTS (
  SELECT 1 FROM object_types t
  WHERE t.workspace_id = NEW.workspace_id AND t.id = NEW.object_type_id
    AND t.schema_version = NEW.object_type_schema_version
)
BEGIN SELECT RAISE(ABORT, 'object instance schema pin is not current'); END;

CREATE TABLE relation_definitions (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  key TEXT NOT NULL,
  from_object_type_id TEXT NOT NULL,
  to_object_type_id TEXT NOT NULL,
  cardinality TEXT NOT NULL CHECK (cardinality IN ('one_to_one', 'one_to_many', 'many_to_one', 'many_to_many')),
  required INTEGER NOT NULL DEFAULT 0 CHECK (required IN (0, 1)),
  archived_at TEXT CHECK (archived_at IS NULL OR archived_at GLOB '????-??-??T*Z'),
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  updated_by_principal_id TEXT NOT NULL,
  UNIQUE (workspace_id, key),
  UNIQUE (workspace_id, id),
  FOREIGN KEY (workspace_id, from_object_type_id) REFERENCES object_types(workspace_id, id),
  FOREIGN KEY (workspace_id, to_object_type_id) REFERENCES object_types(workspace_id, id)
) STRICT;

CREATE TABLE object_relations (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  relation_definition_id TEXT NOT NULL,
  from_object_instance_id TEXT NOT NULL,
  to_object_instance_id TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  updated_by_principal_id TEXT NOT NULL,
  UNIQUE (workspace_id, relation_definition_id, from_object_instance_id, to_object_instance_id),
  UNIQUE (workspace_id, id),
  FOREIGN KEY (workspace_id, relation_definition_id) REFERENCES relation_definitions(workspace_id, id),
  FOREIGN KEY (workspace_id, from_object_instance_id) REFERENCES object_instances(workspace_id, id),
  FOREIGN KEY (workspace_id, to_object_instance_id) REFERENCES object_instances(workspace_id, id)
) STRICT;
CREATE INDEX idx_object_relations_from ON object_relations(workspace_id, from_object_instance_id);
CREATE INDEX idx_object_relations_to ON object_relations(workspace_id, to_object_instance_id);
CREATE INDEX idx_object_relations_definition_from ON object_relations(workspace_id, relation_definition_id, from_object_instance_id);
CREATE INDEX idx_object_relations_definition_to ON object_relations(workspace_id, relation_definition_id, to_object_instance_id);

CREATE TABLE relation_cardinality_slots (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  relation_definition_id TEXT NOT NULL,
  object_relation_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('from', 'to')),
  object_instance_id TEXT NOT NULL,
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  UNIQUE (workspace_id, relation_definition_id, side, object_instance_id),
  FOREIGN KEY (workspace_id, relation_definition_id) REFERENCES relation_definitions(workspace_id, id),
  FOREIGN KEY (workspace_id, object_relation_id) REFERENCES object_relations(workspace_id, id),
  FOREIGN KEY (workspace_id, object_instance_id) REFERENCES object_instances(workspace_id, id)
) STRICT;
CREATE INDEX idx_relation_cardinality_slots_relation ON relation_cardinality_slots(workspace_id, object_relation_id);

CREATE TRIGGER trg_relation_cardinality_slots_allowed_side
BEFORE INSERT ON relation_cardinality_slots
BEGIN
  SELECT RAISE(ABORT, 'relation cardinality slot side does not match definition')
  WHERE NOT EXISTS (
    SELECT 1 FROM relation_definitions r
    WHERE r.workspace_id = NEW.workspace_id AND r.id = NEW.relation_definition_id
      AND ((r.cardinality = 'one_to_one' AND NEW.side IN ('from','to'))
        OR (r.cardinality = 'one_to_many' AND NEW.side = 'to')
        OR (r.cardinality = 'many_to_one' AND NEW.side = 'from'))
  );
END;

CREATE TABLE object_field_index (
  workspace_id TEXT NOT NULL,
  object_type_id TEXT NOT NULL,
  object_instance_id TEXT NOT NULL,
  field_name TEXT NOT NULL,
  value_kind TEXT NOT NULL CHECK (value_kind IN ('string','number','boolean','null')),
  value_text TEXT,
  value_number REAL,
  value_boolean INTEGER CHECK (value_boolean IN (0, 1)),
  PRIMARY KEY (workspace_id, object_instance_id, field_name),
  FOREIGN KEY (workspace_id, object_type_id) REFERENCES object_types(workspace_id, id),
  FOREIGN KEY (workspace_id, object_instance_id) REFERENCES object_instances(workspace_id, id)
) STRICT;
CREATE INDEX idx_object_field_text ON object_field_index(workspace_id, object_type_id, field_name, value_text) WHERE value_kind = 'string';
CREATE INDEX idx_object_field_number ON object_field_index(workspace_id, object_type_id, field_name, value_number) WHERE value_kind = 'number';
CREATE INDEX idx_object_field_boolean ON object_field_index(workspace_id, object_type_id, field_name, value_boolean) WHERE value_kind = 'boolean';

CREATE TABLE field_sequence_counters (
  workspace_id TEXT NOT NULL,
  object_type_id TEXT NOT NULL,
  field_name TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  counter INTEGER NOT NULL DEFAULT 0 CHECK (counter >= 0),
  PRIMARY KEY (workspace_id, object_type_id, field_name, scope_key),
  FOREIGN KEY (workspace_id, object_type_id) REFERENCES object_types(workspace_id, id)
) STRICT;

CREATE TABLE generated_field_values (
  workspace_id TEXT NOT NULL,
  object_type_id TEXT NOT NULL,
  field_name TEXT NOT NULL,
  generated_value TEXT NOT NULL,
  object_instance_id TEXT NOT NULL,
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  PRIMARY KEY (workspace_id, object_type_id, field_name, generated_value),
  UNIQUE (workspace_id, object_instance_id, field_name),
  FOREIGN KEY (workspace_id, object_type_id) REFERENCES object_types(workspace_id, id),
  FOREIGN KEY (workspace_id, object_instance_id) REFERENCES object_instances(workspace_id, id)
) STRICT;

CREATE TABLE reactive_rules (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  automation_id TEXT NOT NULL,
  rule_key TEXT NOT NULL,
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('draft','active','disabled','retired')),
  current_revision_id TEXT,
  archived_at TEXT CHECK (archived_at IS NULL OR archived_at GLOB '????-??-??T*Z'),
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  enabled_at TEXT CHECK (enabled_at IS NULL OR enabled_at GLOB '????-??-??T*Z'),
  disabled_at TEXT CHECK (disabled_at IS NULL OR disabled_at GLOB '????-??-??T*Z'),
  activation_event_cursor TEXT,
  circuit_breaker_status TEXT NOT NULL DEFAULT 'closed' CHECK (circuit_breaker_status IN ('closed','open')),
  last_disabled_reason TEXT,
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  updated_by_principal_id TEXT NOT NULL,
  UNIQUE (workspace_id, rule_key),
  UNIQUE (workspace_id, id)
) STRICT;

CREATE TABLE reactive_rule_revisions (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  rule_id TEXT NOT NULL,
  automation_id TEXT NOT NULL,
  definition_schema_version INTEGER NOT NULL CHECK (definition_schema_version >= 1),
  definition_hash TEXT NOT NULL CHECK (definition_hash GLOB 'sha256:*'),
  requested_actor_json TEXT NOT NULL CHECK (json_valid(requested_actor_json) AND json_type(requested_actor_json) = 'object'),
  trigger_json TEXT NOT NULL CHECK (json_valid(trigger_json) AND json_type(trigger_json) = 'object'),
  predicates_json TEXT NOT NULL CHECK (json_valid(predicates_json) AND json_type(predicates_json) = 'object'),
  action_json TEXT NOT NULL CHECK (json_valid(action_json) AND json_type(action_json) = 'object'),
  idempotency_json TEXT NOT NULL CHECK (json_valid(idempotency_json) AND json_type(idempotency_json) = 'object'),
  limits_json TEXT NOT NULL CHECK (json_valid(limits_json) AND json_type(limits_json) = 'object'),
  supersedes_revision_id TEXT,
  published_at TEXT CHECK (published_at IS NULL OR published_at GLOB '????-??-??T*Z'),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  UNIQUE (workspace_id, rule_id, definition_hash),
  UNIQUE (workspace_id, id),
  FOREIGN KEY (workspace_id, rule_id) REFERENCES reactive_rules(workspace_id, id),
  FOREIGN KEY (workspace_id, supersedes_revision_id) REFERENCES reactive_rule_revisions(workspace_id, id)
) STRICT;
CREATE INDEX idx_reactive_rule_revisions_rule ON reactive_rule_revisions(workspace_id, rule_id, created_at);

CREATE TRIGGER trg_reactive_rule_revisions_immutable_update
BEFORE UPDATE ON reactive_rule_revisions
WHEN NOT (
  OLD.published_at IS NULL AND NEW.published_at IS NOT NULL
  AND OLD.id = NEW.id
  AND OLD.workspace_id = NEW.workspace_id
  AND OLD.rule_id = NEW.rule_id
  AND OLD.automation_id = NEW.automation_id
  AND OLD.definition_schema_version = NEW.definition_schema_version
  AND OLD.definition_hash = NEW.definition_hash
  AND OLD.requested_actor_json = NEW.requested_actor_json
  AND OLD.trigger_json = NEW.trigger_json
  AND OLD.predicates_json = NEW.predicates_json
  AND OLD.action_json = NEW.action_json
  AND OLD.idempotency_json = NEW.idempotency_json
  AND OLD.limits_json = NEW.limits_json
  AND OLD.supersedes_revision_id IS NEW.supersedes_revision_id
  AND OLD.created_at = NEW.created_at
  AND OLD.created_by_principal_id = NEW.created_by_principal_id
)
BEGIN SELECT RAISE(ABORT, 'ReactiveRule revisions are immutable after publication'); END;
CREATE TRIGGER trg_reactive_rule_revisions_immutable_delete
BEFORE DELETE ON reactive_rule_revisions
BEGIN SELECT RAISE(ABORT, 'ReactiveRule revisions are immutable'); END;

CREATE TABLE adapter_inbound_events (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  adapter_id TEXT NOT NULL,
  inbound_event_id TEXT NOT NULL,
  payload_hash TEXT NOT NULL CHECK (payload_hash GLOB 'sha256:*'),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  status TEXT NOT NULL CHECK (status IN ('appended','reduced','rejected')),
  received_at TEXT NOT NULL CHECK (received_at GLOB '????-??-??T*Z'),
  UNIQUE (workspace_id, adapter_id, inbound_event_id)
) STRICT;
CREATE INDEX idx_adapter_inbound_events_workspace_adapter ON adapter_inbound_events(workspace_id, adapter_id, received_at);
CREATE INDEX idx_adapter_inbound_events_unprocessed ON adapter_inbound_events(workspace_id, received_at) WHERE status = 'appended';

CREATE TABLE reactive_rule_evaluations (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  automation_id TEXT NOT NULL,
  rule_id TEXT,
  rule_revision_id TEXT,
  triggering_event_id TEXT NOT NULL,
  action_id TEXT,
  recursion_depth INTEGER NOT NULL CHECK (recursion_depth >= 0),
  predicate_result TEXT NOT NULL CHECK (predicate_result IN ('matched','skipped','failed')),
  emitted_command_id TEXT,
  rendered_command_hash TEXT,
  idempotency_namespace TEXT,
  idempotency_key TEXT,
  dispatch_status TEXT NOT NULL DEFAULT 'pending' CHECK (dispatch_status IN ('pending','dispatched','applied','rejected','retryable_failed','dead_lettered')),
  dispatch_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (dispatch_attempt_count >= 0),
  last_dispatch_error_code TEXT,
  last_dispatch_error_message TEXT,
  payload_hash TEXT NOT NULL CHECK (payload_hash GLOB 'sha256:*'),
  evaluation_json TEXT NOT NULL CHECK (json_valid(evaluation_json) AND json_type(evaluation_json) = 'object'),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z'),
  FOREIGN KEY (workspace_id, rule_id) REFERENCES reactive_rules(workspace_id, id),
  FOREIGN KEY (workspace_id, rule_revision_id) REFERENCES reactive_rule_revisions(workspace_id, id)
) STRICT;
CREATE INDEX idx_reactive_rule_evaluations_workspace_automation ON reactive_rule_evaluations(workspace_id, automation_id, created_at);
CREATE INDEX idx_reactive_rule_evaluations_dispatch_recovery ON reactive_rule_evaluations(workspace_id, created_at, id)
  WHERE dispatch_status IN ('pending','dispatched','retryable_failed') AND idempotency_key IS NOT NULL AND rule_revision_id IS NOT NULL;
CREATE UNIQUE INDEX idx_reactive_rule_evaluations_unique_action ON reactive_rule_evaluations(workspace_id, automation_id, rule_revision_id, triggering_event_id, action_id)
  WHERE rule_revision_id IS NOT NULL AND action_id IS NOT NULL;

CREATE TABLE spec_revisions (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  family_id TEXT NOT NULL CHECK (trim(family_id) <> ''),
  revision INTEGER NOT NULL CHECK (revision >= 1),
  kind TEXT NOT NULL CHECK (kind IN ('task','function','workflow','participant','event','ontology')),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json) AND json_type(payload_json) = 'object'),
  digest TEXT NOT NULL CHECK (digest GLOB 'sha256:*'),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  created_by_principal_id TEXT NOT NULL,
  UNIQUE (workspace_id, family_id, revision),
  UNIQUE (workspace_id, kind, digest),
  UNIQUE (workspace_id, id)
) STRICT;
CREATE INDEX idx_spec_revisions_family ON spec_revisions(workspace_id, family_id, revision DESC);

CREATE TRIGGER trg_spec_revisions_immutable_update
BEFORE UPDATE ON spec_revisions
BEGIN SELECT RAISE(ABORT, 'spec revisions are immutable'); END;
CREATE TRIGGER trg_spec_revisions_immutable_delete
BEFORE DELETE ON spec_revisions
BEGIN SELECT RAISE(ABORT, 'spec revisions are immutable'); END;

CREATE TABLE command_ledger (
  command_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  principal_id TEXT NOT NULL,
  requested_actor_json TEXT NOT NULL CHECK (json_valid(requested_actor_json)),
  effective_actor_json TEXT NOT NULL CHECK (json_valid(effective_actor_json) AND json_type(effective_actor_json) = 'object'),
  actor_context_hash TEXT NOT NULL CHECK (actor_context_hash GLOB 'sha256:*'),
  idempotency_key TEXT NOT NULL,
  command_type TEXT NOT NULL,
  target_json TEXT NOT NULL CHECK (json_valid(target_json) AND json_type(target_json) = 'object'),
  preconditions_json TEXT NOT NULL CHECK (json_valid(preconditions_json) AND json_type(preconditions_json) = 'array'),
  causation_json TEXT NOT NULL CHECK (json_valid(causation_json) AND json_type(causation_json) = 'object'),
  payload_hash TEXT NOT NULL CHECK (payload_hash GLOB 'sha256:*'),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json) AND json_type(payload_json) = 'object'),
  outcome TEXT NOT NULL CHECK (outcome IN ('applied','duplicate','idempotency_conflict','precondition_failed','forbidden','validation_failed','schema_mismatch','quorum_unavailable','automation_recursion_limit')),
  result_json TEXT NOT NULL CHECK (json_valid(result_json) AND json_type(result_json) = 'object'),
  effective_at TEXT NOT NULL CHECK (effective_at GLOB '????-??-??T*Z'),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  UNIQUE (workspace_id, principal_id, idempotency_key),
  UNIQUE (workspace_id, command_id)
) STRICT;
CREATE INDEX idx_command_ledger_workspace_created ON command_ledger(workspace_id, created_at, command_id);

CREATE TABLE journal_records (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  stream_id TEXT NOT NULL,
  stream_sequence INTEGER NOT NULL CHECK (stream_sequence >= 1),
  kind TEXT NOT NULL CHECK (kind IN (
    'work.opened','work.routed','work.submitted','work.satisfied','work.cancelled','work.deadline.reached',
    'function.requested','function.succeeded','function.failed',
    'agent.requested','agent.succeeded','agent.failed',
    'workflow.started','workflow.completed','workflow.failed',
    'domain.event.emitted',
    'knowledge.proposed','knowledge.proposal.accepted','knowledge.proposal.rejected'
  )),
  spec_revision_id TEXT,
  participant_kind TEXT CHECK (participant_kind IS NULL OR participant_kind IN ('human','agent','automation')),
  participant_id TEXT,
  principal_id TEXT NOT NULL,
  correlation_id TEXT,
  causation_id TEXT,
  command_id TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json) AND json_type(payload_json) = 'object'),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  UNIQUE (workspace_id, stream_id, stream_sequence),
  UNIQUE (workspace_id, id),
  FOREIGN KEY (workspace_id, spec_revision_id) REFERENCES spec_revisions(workspace_id, id),
  FOREIGN KEY (workspace_id, command_id) REFERENCES command_ledger(workspace_id, command_id),
  CHECK ((participant_kind IS NULL) = (participant_id IS NULL))
) STRICT;
CREATE INDEX idx_journal_records_stream ON journal_records(workspace_id, stream_id, stream_sequence);
CREATE INDEX idx_journal_records_correlation ON journal_records(workspace_id, correlation_id, stream_sequence) WHERE correlation_id IS NOT NULL;
CREATE INDEX idx_journal_records_kind ON journal_records(workspace_id, kind, created_at);

CREATE TRIGGER trg_journal_records_immutable_update
BEFORE UPDATE ON journal_records
BEGIN SELECT RAISE(ABORT, 'journal records are immutable'); END;
CREATE TRIGGER trg_journal_records_immutable_delete
BEFORE DELETE ON journal_records
BEGIN SELECT RAISE(ABORT, 'journal records are immutable'); END;

CREATE TABLE activities (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  command_id TEXT NOT NULL,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json) AND json_type(payload_json) = 'object'),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  FOREIGN KEY (workspace_id, command_id) REFERENCES command_ledger(workspace_id, command_id)
) STRICT;
CREATE INDEX idx_activities_aggregate ON activities(workspace_id, aggregate_type, aggregate_id, created_at);
CREATE INDEX idx_activities_command ON activities(workspace_id, command_id);

CREATE TABLE outbox_events (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  command_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json) AND json_type(payload_json) = 'object'),
  status TEXT NOT NULL CHECK (status IN ('pending','published','failed')),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  updated_at TEXT NOT NULL CHECK (updated_at GLOB '????-??-??T*Z'),
  FOREIGN KEY (workspace_id, command_id) REFERENCES command_ledger(workspace_id, command_id)
) STRICT;
CREATE INDEX idx_outbox_events_pending ON outbox_events(workspace_id, created_at) WHERE status = 'pending';
CREATE INDEX idx_outbox_events_command ON outbox_events(workspace_id, command_id);

CREATE TABLE projection_events (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  command_id TEXT NOT NULL,
  projection_sequence INTEGER NOT NULL CHECK (projection_sequence >= 1),
  snapshot_id TEXT,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version >= 1),
  payload_hash TEXT NOT NULL CHECK (payload_hash GLOB 'sha256:*'),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json) AND json_type(payload_json) = 'object'),
  signature TEXT,
  signing_key_id TEXT,
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  UNIQUE (workspace_id, projection_sequence),
  FOREIGN KEY (workspace_id, command_id) REFERENCES command_ledger(workspace_id, command_id)
) STRICT;
CREATE INDEX idx_projection_events_command ON projection_events(workspace_id, command_id);

CREATE TABLE workspace_creation_requests (
  principal_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL CHECK (request_hash GLOB 'sha256:*'),
  request_payload_json TEXT NOT NULL CHECK (json_valid(request_payload_json) AND json_type(request_payload_json) = 'object'),
  result_json TEXT NOT NULL CHECK (json_valid(result_json) AND json_type(result_json) = 'object'),
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  created_at TEXT NOT NULL CHECK (created_at GLOB '????-??-??T*Z'),
  PRIMARY KEY (principal_id, idempotency_key),
  UNIQUE (workspace_id)
) STRICT;
