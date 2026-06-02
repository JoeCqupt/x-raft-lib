# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.0-RC1 (current) | Best-effort only |
| < 0.1.0 | Not supported |

This project is alpha; **do not run it in production**. Once we cut a
stable line, this section will list supported branches.

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Open a private advisory at
https://github.com/x-infra-lab/x-raft-lib/security/advisories, or email the
maintainer at `joe469391363@gmail.com`, with:

- A description of the issue.
- Steps to reproduce, ideally with a minimal raft cluster setup.
- The commit hash you tested against.
- Any suggested mitigation.

You can expect:

- An acknowledgement within 7 days.
- A decision on whether the report is in scope within 14 days.
- A coordinated disclosure window of up to 90 days.

## In scope

- Consensus safety violations (committed entries diverging across nodes,
  log corruption under correct Storage).
- Liveness regressions caused by malformed messages from a peer.
- Protocol-level DoS (resource exhaustion via well-formed messages).
- Snapshot install / membership change races leading to data loss.

## Out of scope (alpha)

- Issues caused by your `Storage` or transport implementation.
- Resource exhaustion via a single node sending an unbounded message
  stream — known issue, see `raft-core/TODO.md`.
- Anything that requires a misconfigured cluster to reproduce (e.g. all
  nodes running with `checkQuorum=false` and the same id).
