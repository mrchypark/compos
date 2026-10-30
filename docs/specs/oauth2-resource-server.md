# OAuth2 resource server (optional OIDC end-user profile)

`COMPOS_AUTH_MODE=oauth2` accepts the caller's access token directly in
`Authorization: Bearer ...` on Compos APIs. It is **not** a session/token-exchange
grant. The existing `rauthy` and `pocketbase` modes retain their legacy session
exchange behavior; no production configuration is changed by this addition.

This mode combines [OIDC discovery](https://openid.net/specs/openid-connect-discovery-1_0.html)
with [RFC 7662 introspection](https://www.rfc-editor.org/rfc/rfc7662.html) and
OIDC UserInfo. At startup, discovery issuer must exactly match configuration.
Each request introspects without a token cache and validates active, subject,
expiration, resource audience and exact application scope plus `openid`.
Optional issuer/not-before claims are checked when supplied. Matching UserInfo
subject establishes end-user access-token usability: introspection's
`token_type_hint` is advisory and must not alone distinguish refresh tokens.
No profile/email claim grants membership or links identities.

## Configuration

| Variable | Required meaning |
| --- | --- |
| `COMPOS_AUTH_MODE` | `oauth2` |
| `COMPOS_OAUTH2_ISSUER` | Exact issuer, including any path/trailing slash |
| `COMPOS_OAUTH2_RESOURCE` | Exact allowed access-token audience for this Compos API |
| `COMPOS_OAUTH2_REQUIRED_SCOPE` | One application scope, e.g. `compos.api`, distinct from `openid` |
| `COMPOS_OAUTH2_INTROSPECTION_CLIENT_ID` | Registered confidential resource client |
| `COMPOS_OAUTH2_INTROSPECTION_CLIENT_SECRET` | Private server-side client credential; never forward to the browser |

Compos sends these raw registered values through `client_secret_basic`, URL-encoding
each component as required by [RFC 6749 §2.3.1](https://www.rfc-editor.org/rfc/rfc6749.html#section-2.3.1)
before constructing the HTTP Basic header.

Discovery is fetched from issuer (without its trailing slash) plus
`/.well-known/openid-configuration`; an issuer path stays before that suffix,
such as `https://idp.example/tenant/.well-known/openid-configuration`.
`introspection_endpoint` and
`userinfo_endpoint` are required. HTTPS is required except host-local HTTP
testing. HTTP endpoints must share the loopback issuer origin. URL credentials,
query/fragment and redirects are rejected. Discovery failure prevents startup;
upstream outages fail requests closed (503), malformed responses are 502,
invalid tokens/claims are 401, and missing workspace membership is 403.
Introspection client rejection also fails closed; it is not a valid empty result.

The RP/BFF uses authorization code + S256 PKCE with scopes
`openid email profile compos.api` and its registered Compos resource. For local
tests only, `https://compos.local.test` is the agreed logical audience; production
values are deployment-specific. The IdP administrator must register/approve the
application scope, RP resource permission and resource-client introspection
permission before the consumer runs. The BFF must not forward an ID token,
refresh token, PocketBase token or another application's access token here.
The current GoAuthy fixture supports a separate confidential introspection
client but does not enforce an audience-specific introspection ACL. Compos's
exact audience/scope checks remain mandatory; a separate client secret alone
must not be described as provider-side audience isolation.

## Identity and membership

`POST /api/v1/auth/identity` authenticates the direct bearer and registers only
a minimal identity. It accepts no client identity/role override and returns:

```json
{"data":{"principal_id":"rpr_...","workspace_memberships":[]}}
```

No Person, workspace membership, workspace or local session is created by this
operation. Registration is optional and idempotent. Issuer+subject derive the principal using
the existing `compos:rauthy-principal:v1` hash namespace for storage continuity,
not because the IdP must be Rauthy. Existing provider-key conflicts fail closed.
The identity row has no asserted email and does not overwrite existing profile
data. Invitation/admin or an explicit bootstrap policy must grant membership;
ordinary authenticated identity creation never grants access to an existing
workspace. A direct bearer may explicitly create a workspace through the existing
workspace-creation API before calling this endpoint; that creation grants its
deterministic principal an owner membership, but does not itself write an identity
row. A later identity registration returns that existing membership. This is not
implicit enrollment into an existing workspace. Existing explicit
workspace-creation authorization remains unchanged.

`GET /api/v1/auth/session` retains `{data:{principal_id,workspace_memberships}}`
but validates the access token again rather than looking up a Compos session.
An authenticated non-member can receive an empty membership list; authentication
or storage errors must never be turned into an empty success response.

PB legacy principal migration remains a separate, explicit issuer+subject to
legacy-ID mapping owned by Compos. There is no email/password auto-linking and no
implemented migration command in this slice. Existing membership/Kernel actor
capability checks remain authoritative. SearchSyncer and internal callback auth
are separate service contracts; the existing configured operator bearer remains
limited to the actor-capabilities control plane, not ordinary user APIs.

## Logout, revocation and unsupported features

`POST /api/v1/auth/logout` returns 405 in direct mode: Compos issued no session to
revoke. The RP owns its session/token removal and the IdP's supported standard
logout/revocation flow. An inactive token is rejected on the next introspection;
this does not retroactively cancel commands already executing or guarantee a
stronger revocation consistency model than the IdP provides. Legacy Compos
sessions retain their documented TTL/revocation behavior in legacy modes.

JWT/JWKS-only verification, introspection-only providers without OIDC UserInfo,
machine-only client credentials, DPoP/mTLS sender-constrained tokens and RFC 8693
delegation are not implemented here. This is independent of any optional
external-credential/profile extension; Compos holds no end-user connector credentials.
The configured confidential introspection client secret remains server-side.

## Independent and optional live tests

`go test ./internal/server -run '^TestOAuth2' -count=1` runs fake-IdP + actual
standalone Rhiza tests without another project/service. They cover scope/audience,
expiry/nbf/issuer/subject, refresh-token/UserInfo confusion, revocation, outage,
resource-client rejection, identity-only registration, explicit membership and
cross-workspace refusal. The optional actual-IdP test is skipped by default.

To run it, set `COMPOS_E2E_OAUTH2=1` and
`COMPOS_E2E_OAUTH2_{ISSUER,RESOURCE,CLIENT_ID,EMAIL_DOMAIN,CLIENT_SECRET_FILE,TOKEN_FILE}`.
Only file paths—not credential values—are inputs for secret/token files. Files
must be regular non-symlinks with 0600 permissions. The domain variable is kept
for fixture compatibility and does not grant membership or validate email in
direct mode. Supply optional
`COMPOS_E2E_OAUTH2_{WRONG_AUD,WRONG_SCOPE,EXPIRED,REVOKED}_TOKEN_FILE`; omitted
scenarios explicitly SKIP. The positive token must include `openid compos.api`.

Run `go test ./internal/server -run '^TestOAuth2LiveE2E$' -count=1 -v`.
It creates disposable local Rhiza/Compos HTTP endpoints, verifies no automatic
membership, explicitly grants a test-only membership then removes it, and checks
cross-workspace refusal plus zero Compos sessions. This test does not start the
IdP, deploy anything, revoke provider tokens itself, or replace BFF browser E2E.
On 2026-09-07 the actual GoAuthy direct-mode fixture passed with a dedicated RP
and separate introspection client. The local log
`/tmp/goauthy-compos-oauth2-audience-20260907.log` records
`TestOAuth2ConsumerFixture` PASS (1.25s) and `TestOAuth2LiveE2E` PASS (0.71s;
Compos package 1.499s). The runner reported exit 0; PASS/SKIP lines were also
checked locally. Wrong audience, wrong scope and revoked tokens were rejected;
expired-token coverage explicitly SKIPPED. The harness also verified membership
absence/removal and cross-workspace 403, explicit fixture membership access,
logout 405 and zero local sessions.

Both repositories used uncommitted source changes, so commit IDs alone cannot
reproduce this result. The disposable issuer, database and credential files were
cleaned up; this is not an always-on endpoint or production rollout. Earlier
generic fixture attempts failed before Compos invocation because permission-only
scope registration was unavailable; GoAuthy subsequently added that support and
authenticated DCR audience registration. The earlier GoAuthy pilot document
records the separate legacy session-exchange mode, not this mode.
