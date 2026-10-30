# Opt-in GoAuthy local pilot

This is an authentication compatibility harness, not a production authentication
switch. It reuses `AuthModeRauthy` and `/api/v1/auth/rauthy` with GoAuthy as issuer.
No PocketBase login is involved. The harness starts an actual Compos HTTP handler
and a standalone Rhiza store in a temporary directory, bootstraps its own workspace,
then removes the directory when the test finishes. It does not start GoAuthy,
change an existing checkout, touch a cluster, or use production configuration.

## Issuer fixture contract

GoAuthy owns the host-local issuer, confidential issuing/introspection client,
test users, and token issuance. Proposed issuer: `http://localhost:17889`.
Register logical resource `https://compos.local.test` and request scopes
`openid email profile`. The issuing client and HTTP Basic introspection client
are initially the same. Compos requires `openid`; it separately checks the exact
resource audience, active/expiry/not-before claims, matching UserInfo subject,
verified email, and configured allowed email domain. Token expiry must leave
enough time for two logins and the remaining checks.

The harness does not implement authorization redirects, PKCE, Device Flow, or
refresh. Backoffice owns direct OIDC browser login and its callback. GoAuthy owns
fixture issuance. Supply already-issued access tokens, not ID tokens. The four
negative fixtures must represent the advertised condition: wrong audience only,
missing `openid` only, expired, and already revoked. For the first two, keep the
other claims valid. The issuer fixture owner must establish those conditions;
a 401 alone does not establish why an arbitrary token was rejected.

## Execution

Each `*_FILE` contains one credential, optionally followed by a newline. Use
owner-only (0600), regular non-symlink files in a private directory. The loader
checks permissions and file identity before reading. Do not put credential values in
command arguments, shell history, messages, or logs. Files are read directly in
the test process with a 64 KiB limit. No response body, upstream error detail,
provider token, client secret, or Compos session token is included in failures.

From the Go repository root:

```sh
COMPOS_E2E_GOAUTHY=1 \
COMPOS_E2E_GOAUTHY_ISSUER=http://localhost:17889 \
COMPOS_E2E_GOAUTHY_RESOURCE=https://compos.local.test \
COMPOS_E2E_GOAUTHY_CLIENT_ID=compos-local-e2e \
COMPOS_E2E_GOAUTHY_EMAIL_DOMAIN=example.test \
COMPOS_E2E_GOAUTHY_CLIENT_SECRET_FILE=/private/pilot/client-secret \
COMPOS_E2E_GOAUTHY_TOKEN_FILE=/private/pilot/access-token \
COMPOS_E2E_GOAUTHY_WRONG_AUD_TOKEN_FILE=/private/pilot/wrong-aud \
COMPOS_E2E_GOAUTHY_WRONG_SCOPE_TOKEN_FILE=/private/pilot/wrong-scope \
COMPOS_E2E_GOAUTHY_EXPIRED_TOKEN_FILE=/private/pilot/expired \
COMPOS_E2E_GOAUTHY_REVOKED_TOKEN_FILE=/private/pilot/revoked \
go test ./internal/server -run '^TestGoAuthyLiveE2E$' -count=1 -v
```

Issuer, resource, client ID, email domain, client secret file and positive token
file are required when enabled. The four negative token files are optional for
the initial positive pilot; each omitted scenario reports SKIP / NOT VERIFIED.
A positive-only pass is not a complete negative matrix pass. The entire test
skips when the opt-in flag is not `1`. Issuer must be loopback HTTP(S), without URL credentials,
query or fragment. Resource must be an absolute HTTPS URL. The harness disables
IdP redirects and uses request timeouts. It configures Compos programmatically;
ordinary production `COMPOS_AUTH_*`, PocketBase, cloud storage and conductor
variables are not consumed. No persistent Compos server or fixed Compos port is
needed. The proposed issuer port is owned by the GoAuthy task.

Assertions: positive exchange; stable principal and Person on repeated login;
exactly one member enrollment and principal–Person binding; authenticated session
identity/membership; refusal of a non-member workspace; supplied negative exchanges;
logout followed by 401 for that session; second session remains usable and is then
logged out. The DB and returned tokens are disposable.

## Boundaries and verification

`TestGoAuthyPilotHarnessContract` uses an HTTP fake issuer and real local Rhiza to
check the harness. `TestGoAuthyPilotFixtureValidation` checks fixture validation
and redacted loader errors. Neither is live GoAuthy evidence. A skipped opt-in
test is not a live pass. Record a live result only after the actual issuer and
issuer-owned fixtures are supplied and the command above succeeds.

### Actual standalone pilot — 2026-09-07

The GoAuthy integration task reported exit 0 for
`/tmp/goauthy-compos-live-20260907.log`. The log records a passing
`TestConsumerTokenFixture` (package 1.388s) and `TestGoAuthyLiveE2E`
(test 0.88s, package 1.479s). This run used actual host-local GoAuthy standalone
authorization-code login with S256 and a resource-audience access token, followed
by the real Compos HTTP handler and disposable Rhiza store.

Verified: token exchange, repeat-login principal/Person and member uniqueness,
session identity/membership, non-member workspace 403, local-session logout and
subsequent 401, and the second independent session remaining usable before its
own logout. `WRONG_AUD`, `WRONG_SCOPE`, `EXPIRED`, and `REVOKED` were explicitly
**SKIP / NOT VERIFIED** because their fixtures were not supplied. This is a
positive-path live pass, not a full negative matrix or global revocation pass.

The first fixture-issuance attempt failed user activation with HTTP 400 because
required profile fields were omitted. GoAuthy reused its existing
`createCatalogSessionUser` helper and reran successfully; that initial failure is
not counted as a pass. The GoAuthy task reports cleanup of the disposable server,
DB and credential files. No long-lived endpoint or production deployment was
created. The expanded run below adds three negative fixtures without changing
the scope of this earlier positive-only run.

### Expanded actual standalone pilot — 2026-09-07

The GoAuthy task reported exit 0 for
`/tmp/goauthy-compos-negative-20260907.log`. The log confirms
`TestConsumerTokenFixture` PASS (test 2.46s, package 3.185s) and
`TestGoAuthyLiveE2E` PASS (test 1.49s, package 2.093s).

GoAuthy issued the fixtures through actual authorization-code + PKCE flows:
the wrong-audience token was active with another resource audience; the
wrong-scope token was active without `openid`; the revoked token was first
issued, then revoked through `/oidc/revoke` and confirmed inactive. Compos
rejected all three exchanges with 401. This run also executed the added 401
assertion after the second session's logout, alongside the positive-path checks.
`EXPIRED` alone remained **SKIP / NOT VERIFIED**. This is not a complete
four-negative matrix pass, and the new assertion is not retroactive evidence
for the earlier run.

The GoAuthy task also reported fixture-file unit tests, vet, shell syntax and
diff checks passing. The process remained disposable; it did not establish an
always-running service or implement immediate IdP-to-Compos session revocation.

### Session and migration limits

Compos retains its own sessions, capped at the smaller of provider-token expiry
and five minutes in this pilot. GoAuthy revocation/disabling **does not currently
revoke an already-issued Compos session immediately**. The revoked fixture checks
new exchange rejection, not back-channel logout. Compos logout revokes one local
session only; it neither revokes the provider token nor logs out GoAuthy. Refresh
at the RP followed by a new session exchange is a separate future live scenario.

The intended application direction is direct Backoffice OIDC login and replacing
PocketBase authentication, not migrating the business database here. Changing
issuer or provider hashing creates a different Compos principal. Preserve legacy
user references through explicit issuer+subject ↔ legacy-ID migration mapping;
never auto-merge accounts by email. Production mode removal/migration and stronger
cross-system session revocation semantics require a separate reviewed change.
