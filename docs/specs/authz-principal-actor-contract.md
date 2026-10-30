# Authorization Principal And Actor Contract

The API server derives identity from credentials. Clients cannot assert
authenticated identity.

## Definitions

- `authenticated_principal`: identity proven by credentials.
- `requested_actor`: actor requested by the command envelope.
- `effective_actor`: actor accepted by Kernel authorization for domain audit and
  policy evaluation.

## Rule

The Kernel authorizes:

```text
authenticated_principal
effective_actor
workspace_id
command_type
target
preconditions
payload
```

The client may request an actor, but the Kernel decides whether the
authenticated principal may act as that actor.

## Service Principals

Service principals have explicit scoped capabilities. A service principal is not
a workspace member unless explicitly bound.

Capability grants are administrative control-plane operations. The server
accepts them only from authenticated principals configured in
`COMPOS_ADMIN_PRINCIPALS`, or from cluster operator calls carrying
`COMPOS_ADMIN_BEARER_TOKEN`.

The first executable capability scope is:

- authenticated principal id;
- requested actor type and id;
- workspace id;
- command type;
- causation source;
- relation definition id, when the command is `relation.link` or
  `relation.unlink`;
- idempotency namespace prefix.

If any part of the scope does not match, the command fails closed before ledger
or aggregate writes.
No non-relation command type may carry a relation definition id in an actor
capability grant.

## Automation Authority

Automation dispatch commands include:

- `automation_id`
- evaluation record id
- triggering event id
- recursion depth
- effective permission policy
- idempotency namespace

Automation may emit command intents only. It cannot write canonical rows.
Dispatch runs with the ReactiveRule creator as `authenticated_principal`.
The requested actor remains `{type: "automation", id: automation_id}` for audit
and causation.

The Kernel rejects an automation command when the envelope is missing automation
causation metadata, the automation id does not match the requested actor, the
evaluation record id is missing, the idempotency namespace does not match the
command idempotency key, or the recursion depth exceeds the rule limit.

## Adapter Reducers

Adapters append inbound events. Kernel-owned reducers decide whether inbound
events become observations or command intents.

Adapters cannot move objects or mutate canonical rows.
