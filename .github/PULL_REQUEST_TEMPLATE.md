## Summary

<!-- What does this PR do, and why? One paragraph is enough. -->

## Type of change

- [ ] Bug fix
- [ ] New feature / new DuckDB type support
- [ ] Refactor (no behavior change)
- [ ] Documentation / comments
- [ ] CI / tooling
- [ ] Breaking change — describe below

## Checklist

- [ ] `cargo fmt --all` — no diffs
- [ ] `cargo clippy --workspace --all-targets --features "chrono,decimal,json,parquet,async,pool,udf,better-duck-diesel/r2d2" -- -D warnings` — clean
- [ ] Package-scoped core, Diesel, and macro tests pass (see `CONTRIBUTING.md`)
- [ ] New public API has `///` doc comments
- [ ] New `unsafe` blocks have `// SAFETY:` explanations
- [ ] Added tests for new functionality (or explained why tests aren't needed)
- [ ] Linked issue: Fixes #

## Notes for reviewers

<!-- Anything the reviewer should pay extra attention to, or context that's not obvious from the code. -->
