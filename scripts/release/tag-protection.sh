#!/bin/sh
set -eu
repo=$1
tag=$2
git check-ref-format "refs/tags/$tag"
list=$(gh api --paginate --slurp "repos/$repo/rulesets?includes_parents=true&per_page=100")
ids=$(printf '%s\n' "$list" | jq -er '[.[][] | select(.target == "tag" and .enforcement == "active") | .id | select(type == "number")] | unique | .[]') || {
  echo 'No active tag ruleset could be verified' >&2
  exit 1
}
for id in $ids; do
  ruleset=$(gh api "repos/$repo/rulesets/$id")
  if printf '%s\n' "$ruleset" | jq -e --arg ref "refs/tags/$tag" '
    .target == "tag" and .enforcement == "active" and
    (.bypass_actors | type == "array" and length == 0) and
    (.conditions.ref_name.exclude == []) and
    ([.conditions.ref_name.include[]? | select(. == $ref or . == "~ALL")] | length > 0) and
    ([.rules[]?.type] | index("update") != null and index("deletion") != null)
  ' >/dev/null; then
    exit 0
  fi
done
echo "No non-bypass update and deletion ruleset verified for refs/tags/$tag" >&2
exit 1
