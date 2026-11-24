#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
cat > "$tmp/gh" <<'STUB'
#!/bin/sh
[ "${GH_FAIL:-}" != yes ] || exit 1
case "$*" in
  *--paginate*) printf '%s\n' '[[{"id":1,"target":"tag","enforcement":"active"}]]';;
  *) cat "$RULESET_FIXTURE";;
esac
STUB
chmod +x "$tmp/gh"
PATH="$tmp:$PATH"
RULESET_FIXTURE="$tmp/ruleset.json"
export PATH RULESET_FIXTURE
cat > "$RULESET_FIXTURE" <<'JSON'
{"target":"tag","enforcement":"active","bypass_actors":[],"conditions":{"ref_name":{"include":["refs/tags/v0.1.1"],"exclude":[]}},"rules":[{"type":"update"},{"type":"deletion"}]}
JSON
check() { sh "$root/scripts/release/tag-protection.sh" owner/repo "$1" >/dev/null 2>&1; }
check v0.1.1
if check v0.1.2; then echo 'unprotected tag accepted' >&2; exit 1; fi
if GH_FAIL=yes check v0.1.1; then echo 'API failure accepted' >&2; exit 1; fi
unset GH_FAIL
for filter in 'del(.bypass_actors)' '.bypass_actors = [{"actor_type":"RepositoryRole"}]' '.enforcement = "evaluate"' '.rules = [{"type":"update"}]' '.rules = [{"type":"deletion"}]' '.conditions.ref_name.exclude = ["refs/tags/v0.1.1"]' '.conditions.ref_name.include = ["refs/tags/v*"]'; do
  jq "$filter" "$RULESET_FIXTURE" > "$tmp/modified.json"
  mv "$RULESET_FIXTURE" "$tmp/original.json"
  mv "$tmp/modified.json" "$RULESET_FIXTURE"
  if check v0.1.1; then echo "invalid ruleset accepted: $filter" >&2; exit 1; fi
  mv "$tmp/original.json" "$RULESET_FIXTURE"
done
jq '.conditions.ref_name.include = ["~ALL"]' "$RULESET_FIXTURE" > "$tmp/all.json"
mv "$tmp/all.json" "$RULESET_FIXTURE"
check v0.1.2
