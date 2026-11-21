# Standalone source copy

Prepared from the original working tree, including its pending typed-object API
routes and replay fixes. Files were selected with git ls-files --cached --others
--exclude-standard and copied with tar; Git metadata, Finder metadata, environment
files, and credential/key files were excluded. No Git repository was initialized.

## Identity and deployment boundaries

- Main and export-tool modules use github.com/mrchypark/compos.
- Publishing targets mrchypark/compos and COMPOS_PUBLISH_TOKEN. Both publishing
  jobs are explicitly disabled pending private-repository setup.
- Organization-bound image locations use registry.example.invalid/compos;
  the previous email-domain binding uses example.invalid (fail closed).
- Other deployment project, bucket, cluster, and service references remain
  historical context. They were not redirected or validated. These manifests
  are not configured for deployment to a new environment.
- Historical CI run IDs remain as text; their original links were removed.
  Those past results are not validation of this copy.

## Licensing blocker

The selected source list contained no LICENSE, COPYING, NOTICE, AUTHORS, or
COPYRIGHT file. No new grant, copyright ownership claim, or replacement license
was invented. Resolve original copyright/license provenance and retain all
required notices before redistribution. Existing third-party source notices
were retained.

## Local verification

Formatting and source whitespace checks completed. Focused API replay and kernel
tests passed under the renamed module with GOPROXY=off and GOSUMDB=off.
Search-syncer validation was blocked by an uncached meilisearch-go v0.36.3
module; no network fallback was used. No cluster, cloud, image publishing, or
live service verification was performed.
