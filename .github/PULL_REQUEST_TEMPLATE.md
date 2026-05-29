## Summary

What does this PR change and why?

## Related issue

Closes #... (for non-trivial changes, please link a discussion issue — see
[CONTRIBUTING.md](../CONTRIBUTING.md)).

## Checklist

- [ ] Tests added/updated at the right level (unit / property / datadriven /
      system / integration).
- [ ] `mvn install` passes locally (JDK 17+).
- [ ] jacoco gates still pass (raft-core: instruction ≥85% / branch ≥80% /
      line ≥88% / method ≥85%); I did not lower the floor.
- [ ] New `.java` files carry the Apache-2.0 source header.
- [ ] Behaviour matches etcd-io/raft, or the deliberate divergence is
      documented in code.
- [ ] No new public API surface that we can't keep stable (or it's flagged
      for discussion).
