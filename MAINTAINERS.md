# Maintainers

Authoritative list of people with merge rights on this repository and
the right to publish releases to Maven Central. Pair this with
[`.github/CODEOWNERS`](.github/CODEOWNERS), which encodes per-area
review requirements.

## Current maintainers

| Handle | Name | Areas | Contact |
|---|---|---|---|
| [@JoeCqupt](https://github.com/JoeCqupt) | Joe | All modules, release process, infrastructure | <joe469391363@gmail.com> |

## Responsibilities

A maintainer is expected to:

- Review and merge pull requests in their area within a reasonable
  window (best-effort, no SLA pre-`1.0`).
- Triage incoming issues — at minimum label and route — within ~7
  days.
- Cut releases per [`RELEASING.md`](RELEASING.md) when the changelog
  has accumulated a coherent set of changes.
- Respond to security advisories filed via
  [GitHub Security Advisories](https://github.com/x-infra-lab/x-raft-lib/security/advisories)
  per [`SECURITY.md`](SECURITY.md).
- Uphold the [Code of Conduct](CODE_OF_CONDUCT.md) in project spaces.

Maintainers do **not** need to author code in every area they own —
the ownership is about review quality and final say, not exclusive
authorship.

## Becoming a maintainer

There is no formal application. New maintainers are added when:

1. They have contributed multiple non-trivial PRs that landed without
   needing significant rework.
2. They have demonstrated review judgement on other people's PRs.
3. An existing maintainer nominates them, and any other current
   maintainer concurs (single-maintainer projects: nomination is
   self-evident, but new maintainers are still added via PR for
   transparency).

The mechanical step is a PR that:

- Adds the new maintainer to the table above.
- Adds the new maintainer to relevant lines in `.github/CODEOWNERS`.
- Is merged by an existing maintainer.

## Emeritus

Maintainers who step back are moved to an _Emeritus_ section here
(empty for now). Their commit history stays attributed; only the
review / release rights lapse.

## Removal

Maintainers can be removed for sustained absence, code-of-conduct
violations, or by mutual agreement. The removal mechanism mirrors the
addition mechanism: a PR updating this file and `CODEOWNERS`, merged
by another maintainer.
