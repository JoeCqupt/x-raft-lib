# Releasing

The exact recipe used for every public release. The first execution
of this recipe was `0.1.0-RC1` on 2026-06-02 — the steps below are
the reproducible version of what was done then. Anyone listed in
[`MAINTAINERS.md`](MAINTAINERS.md) should be able to cut a release
from a clean checkout in under 30 minutes (most of which is the
Maven Central deploy itself waiting for indexing).

## Versioning policy

Pre-`1.0` line:

- `0.x.0-alpha[N]` — exploratory, no API stability promise
- `0.x.0-RC[N]` — release candidate, API frozen for the `0.x.0` line
- `0.x.0` — stable on the `0.x` line; minor versions may break across
  `0.x` boundaries
- `0.x.y` — patch releases against an `0.x.0` line; no breaking
  changes

The aggregate parent and every published module share one version.
`raft-examples` and `raft-tests` set `<maven.deploy.skip>true</>` and
`<skipPublishing>true</>` — they exist in the reactor for build /
integration testing but never reach Central.

The `main` branch always carries the **next** version as
`-SNAPSHOT`. Tags carry the release version literal.

## Prerequisites (one-time)

These are configured at the GitHub repository level, not per release.
Validate before triggering a release if anything has rotated.

- **Maven Central namespace** — `io.github.x-infra-lab` namespace
  must be claimed on the Sonatype Central Portal and verified by the
  `x-infra-lab` GitHub org or a single verifying repository.
- **GitHub Actions secrets** (Settings → Secrets and variables →
  Actions):
  - `CENTRAL_USER` — Sonatype Central user token name.
  - `CENTRAL_PASSWD` — Sonatype Central user token password.
  - `GPG_PRIVATE_KEY` — ASCII-armored private key whose public half
    is uploaded to a public keyserver
    (`gpg --keyserver keyserver.ubuntu.com --send-keys <fpr>`).
  - `GPG_PASSPHRASE` — passphrase for the above key.
- **Branch protection on `main`** — required status checks include
  the `ci` workflow; admin override available for emergency releases
  is documented but discouraged.

## Release checklist

### 1. Confirm `main` is releasable

```bash
git checkout main && git pull
mvn -B -ntp install            # full reactor: unit, integration, jacoco, spotless
```

All matrix legs of the [`ci`](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml)
workflow on the head commit must be green. If
[`chaos-soak-weekly`](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/chaos-soak-weekly.yml)
fired since the last release, its latest run should also be green or
have a triaged failure.

### 2. Decide the version

| Bumping from | Typical next |
|---|---|
| `0.1.0-SNAPSHOT` (first cut) | `0.1.0-alpha1` → `0.1.0-RC1` → `0.1.0` |
| Released `0.1.0-RC1` | `0.1.0-RC2` (if RC needed iterations) or `0.1.0` (GA) |
| Released `0.1.0` | next minor `0.2.0-SNAPSHOT` on main; patches `0.1.1` from a maintenance branch |

If shipping an RC or GA, the API surface in the published module set
must match what's documented in the corresponding `## [version]`
heading of [`CHANGELOG.md`](CHANGELOG.md).

### 3. Bump versions across the reactor

```bash
# replace 0.1.0-SNAPSHOT (or whatever's current) with the release version,
# in every pom + every README that pins a version. Use the literal value;
# don't introduce a Maven property — the central-publishing-maven-plugin
# resolves before the flatten plugin runs.
NEXT=0.1.0-RC2     # example
find . -type f \( -name pom.xml -o -name "*.md" \) \
    -not -path "*/target/*" \
    -exec sed -i '' "s/0\.1\.0-SNAPSHOT/$NEXT/g" {} +
```

Manually re-review:

- `README.md` — the "Latest release" callout box, the `<version>` in
  install examples, the install snippet's coordinates.
- `raft-core/README.md` and `raft-core/README.zh.md` — same callouts.
- `SECURITY.md` — Supported versions table.
- `CHANGELOG.md` — move accumulated `## [Unreleased]` content under
  a fresh `## [$NEXT] - YYYY-MM-DD` heading; leave `## [Unreleased]`
  as an empty placeholder.

### 4. Local dry-run

```bash
mvn -B -ntp -Prelease verify -DskipTests -Djacoco.skip=true -Dgpg.skip=true
```

Verifies that the release profile produces a `*-sources.jar` and
`*-javadoc.jar` for every publishable module, that the flatten
plugin runs, and that Spotless still passes. `-Dgpg.skip=true`
because most local machines don't have the release GPG key — the
runner does.

### 5. Commit and push

```bash
git add -A
git commit -m "Release $NEXT"
git push origin main
```

Wait for the `ci` workflow on this commit to go green across all six
matrix legs.

### 6. Trigger the publish workflow

```bash
gh workflow run maven-publish.yml --ref main
gh run watch $(gh run list --limit 1 --workflow=maven-publish.yml --json databaseId --jq '.[0].databaseId')
```

The workflow runs `mvn -U -B -ntp deploy -Prelease`, which:

1. Re-runs the integration suite (failsafe against the runner
   environment differing from the local one).
2. GPG-signs every artefact using the secret-injected key.
3. Uploads the deployment bundle to the Sonatype Central Portal via
   the `central-publishing-maven-plugin`.
4. Auto-publishes (`<autoPublish>true</>` in the parent pom) and
   waits until Central confirms the bundle is published
   (`<waitUntil>published</>`).

Total time: ~30 minutes (most of it integration tests + Central
indexing). If the workflow fails before the publish step the
bundle never reaches Central; if it fails during publish the bundle
is left in `FAILED` status on the Central Portal and can be inspected
there.

**Once a version is published it cannot be removed or republished.**
If a release goes out broken, the recovery path is a new patch
version (`0.1.0-RC2`, `0.1.1`, etc.), not a re-publish.

### 7. Tag the release commit

```bash
RELEASE_SHA=$(git rev-parse HEAD)   # or the exact commit that was published
git tag -a "v$NEXT" -m "Release $NEXT" "$RELEASE_SHA"
git push origin "v$NEXT"
```

Tag annotated, not lightweight — annotated tags carry the message
that shows up in `git for-each-ref --format='%(contents)'` and the
GitHub Releases UI.

### 8. Create the GitHub Release

```bash
gh release create "v$NEXT" \
    --title "$NEXT — <short headline>" \
    --prerelease \                  # drop for GA
    --notes-file <path-or-stdin>
```

The release notes should include:

- A one-line summary.
- The Maven coordinates (raft-core / raft-transport-grpc /
  raft-storage-rocksdb / raft-proto), so a copy-paste from the
  Release page is enough to add the dependency.
- Direct links to the Central artefact pages
  (`https://repo1.maven.org/maven2/io/github/x-infra-lab/<artifactId>/$NEXT/`).
- A "what this release means" paragraph (alpha / RC / GA, what's
  frozen, what's still ahead).
- The CHANGELOG anchor link, e.g.
  `https://github.com/x-infra-lab/x-raft-lib/blob/v$NEXT/CHANGELOG.md#$NEXT---YYYY-MM-DD`.

See the [`v0.1.0-RC1` release](https://github.com/x-infra-lab/x-raft-lib/releases/tag/v0.1.0-RC1)
for the established format.

### 9. Bump main back to `-SNAPSHOT`

Releases other than patch releases bump the next development version:

```bash
NEXT_DEV=0.2.0-SNAPSHOT  # if $NEXT was an RC, NEXT_DEV stays in same line: 0.1.0-SNAPSHOT
find . -type f -name pom.xml -not -path "*/target/*" \
    -exec sed -i '' "s/$NEXT/$NEXT_DEV/g" {} +
# Also touch the README's "Latest release / main is" callout boxes to
# point at $NEXT (released) and $NEXT_DEV (main).
git commit -am "Bump main to $NEXT_DEV"
git push origin main
```

### 10. Announce (optional)

For RCs and GAs:

- Post to the project's GitHub Discussions (if enabled) with the
  Release link.
- Cross-link from the parent project README if applicable.
- File a tracking issue for the *next* release if there's a clear
  scope (e.g. "`0.1.0` GA — track production-mileage evidence").

## Patch releases (`0.x.y`)

Patch releases come from a maintenance branch, not from `main`. For
example, fixing a bug in `0.1.0`:

```bash
git checkout -b release-0.1.x v0.1.0      # or the latest 0.1.x tag
# cherry-pick the fix commits from main:
git cherry-pick <sha>...<sha>
# Run the checklist above from step 3, with NEXT=0.1.1.
# Push the branch + tag; main does not move.
```

The maintenance branch should run the same `ci` workflow. The release
workflow is the same one (`maven-publish.yml`) triggered from the
branch ref instead of `main`.

## Yanking a release

You cannot pull a release from Maven Central. The two recovery
levers are:

1. **Publish a follow-up version** that supersedes the broken one,
   plus a Security Advisory describing the regression.
2. **Update the README + GitHub Release notes** for the broken
   version to warn users away from it.

## Cheat sheet

```bash
# release a new version end-to-end
NEXT=0.1.0-RC2
git pull && find . -type f \( -name pom.xml -o -name "*.md" \) \
    -not -path "*/target/*" \
    -exec sed -i '' "s/0\.1\.0-SNAPSHOT/$NEXT/g" {} +
# ... manually fix CHANGELOG / README callouts ...
mvn -Prelease verify -DskipTests -Djacoco.skip=true -Dgpg.skip=true
git commit -am "Release $NEXT" && git push origin main
# (wait for ci on HEAD to go green)
gh workflow run maven-publish.yml --ref main
# (wait ~30 min)
git tag -a "v$NEXT" -m "Release $NEXT" HEAD && git push origin "v$NEXT"
gh release create "v$NEXT" --title "$NEXT — ..." --prerelease --notes-file RELEASE_NOTES.md
# bump back to dev:
NEXT_DEV=0.1.0-SNAPSHOT
find . -type f -name pom.xml -not -path "*/target/*" \
    -exec sed -i '' "s/$NEXT/$NEXT_DEV/g" {} +
git commit -am "Bump main to $NEXT_DEV" && git push origin main
```
