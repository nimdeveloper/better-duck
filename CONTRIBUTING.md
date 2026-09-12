# Contributing to better-duck

Thanks for considering a contribution. This guide covers everything you need: setting up your environment, the git workflow, how commits and PRs should look, and a walkthrough for the most common task (adding a new DuckDB type).

---

## Table of contents

1. [Getting started](#getting-started)
2. [Dev environment setup](#dev-environment-setup)
   - [Windows](#windows)
   - [macOS](#macos)
   - [Linux](#linux)
3. [Build, test, lint](#build-test-lint)
4. [Project layout](#project-layout)
5. [Git flow](#git-flow)
6. [Commit messages](#commit-messages)
7. [Pull requests](#pull-requests)
8. [Adding a new DuckDB type](#adding-a-new-duckdb-type)
9. [Code of conduct](#code-of-conduct)

---

## Getting started

```sh
git clone https://github.com/nimdeveloper/better-duck.git
cd better-duck
cargo build  # first build compiles bundled DuckDB — this takes a few minutes
cargo test
```

The `bundled` feature (on by default) compiles DuckDB from source, so you don't need anything installed beyond a Rust toolchain. Subsequent builds are fast because Cargo caches the compiled C library.

---

## Dev environment setup

### Windows

1. **Rust** — install via [rustup](https://rustup.rs/). You need the **MSVC toolchain**:
   ```sh
   rustup default stable-x86_64-pc-windows-msvc
   ```
2. **Build tools** — install [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) with the "Desktop development with C++" workload. `cargo build` handles the rest.
3. **LLVM** (only if you use `--features buildtime_bindgen`) — download from [LLVM releases](https://releases.llvm.org/) and set `LIBCLANG_PATH` to the `bin/` directory. Regular builds don't need this.

### macOS

1. **Rust** via rustup (stable, default toolchain).
2. **Xcode command-line tools:**
   ```sh
   xcode-select --install
   ```
3. **iOS cross-compilation** (optional, for the CI iOS build step):
   ```sh
   rustup target add aarch64-apple-ios x86_64-apple-ios
   ```
   Regular development on macOS doesn't require these targets.

### Linux

1. **Rust** via rustup.
2. **C build tools:**
   ```sh
   # Ubuntu / Debian
   sudo apt-get install build-essential
   
   # Fedora / RHEL
   sudo dnf install gcc gcc-c++ make
   ```
3. Everything else is handled by the `bundled` feature.

---

## Build, test, lint

These are the commands CI runs; make sure they all pass before opening a PR:

```sh
# Format (uses nightly options when available; stable silently skips them)
cargo fmt --all

# Audit the filtered DuckDB v1.5.5 API catalog against production Rust usage
cargo run --locked -p xtask -- audit-capabilities

# Lint — zero warnings allowed
cargo clippy --workspace --all-targets --features "chrono,decimal,json,parquet,async,pool,udf,better-duck-diesel/r2d2" -- -D warnings

# Run package-scoped tests (workspace-wide normal tests unify incompatible chrono paths)
cargo test -p better-duck-core
cargo test -p better-duck-core --features udf
cargo test -p better-duck-diesel
cargo test -p better-duck-diesel --features chrono
cargo test -p better-duck-macros --lib

# Generate merged coverage for testable crates only; doctests are not invoked.
cargo llvm-cov --no-report -p better-duck-core --tests --features "chrono,decimal,async,pool,udf" --exclude-from-report better-duck-sys --exclude-from-report xtask
cargo llvm-cov --no-report -p better-duck-core --tests --no-default-features --exclude-from-report better-duck-sys --exclude-from-report xtask
cargo llvm-cov --no-report -p better-duck-diesel --tests --all-features --exclude-from-report better-duck-sys --exclude-from-report xtask
cargo llvm-cov --no-report -p better-duck-macros --lib --exclude-from-report better-duck-sys --exclude-from-report xtask
cargo llvm-cov report --lcov --output-path lcov.info --ignore-filename-regex '(^|[\\/])(xtask|crates[\\/]better-duck-sys)([\\/]|$)'

# Build docs (doc warnings are errors in CI)
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --features "chrono,decimal"
```

`api-capabilities.json` is the executable support ledger for the filtered `api.md` catalog. Do not hand-edit production evidence paths: after implementing a pending capability and updating its state, run `cargo run --locked -p xtask -- audit-capabilities --refresh-evidence`, review the JSON diff, then rerun the audit without the refresh flag.

For a quick sanity check before pushing:
```sh
cargo fmt --all && cargo test -p better-duck-core && cargo test -p better-duck-diesel
```

---

## Project layout

```
better-duck/
├── crates/
│   ├── better-duck-core/        # The low-level DuckDB wrapper
│   │   ├── src/
│   │   │   ├── connection.rs    # Public Connection API
│   │   │   ├── types/           # DuckValue, DuckValueRef, type impls
│   │   │   │   ├── value.rs     # DuckValue enum + from_duckdb_vec()
│   │   │   │   ├── value_ref.rs # DuckValueRef (borrowed / zero-copy)
│   │   │   │   ├── numeric.rs   # Numeric FromSql/ToSql + AppendAble
│   │   │   │   ├── date_chrono.rs / date_native.rs
│   │   │   │   ├── duck_struct.rs / map.rs / union.rs / array.rs
│   │   │   │   └── appendable.rs  # AppendAble trait
│   │   │   └── raw/             # Low-level FFI wrappers (DuckResult, etc.)
│   │   ├── tests/               # Integration tests
│   │   └── benches/             # comparison.rs, my_benchmark.rs
│   │
│   └── better-duck-diesel/      # Diesel 2.3 backend
│       ├── src/
│       │   ├── connection.rs    # DuckDbConnection
│       │   ├── backend.rs       # DuckDb backend + HasSqlType impls
│       │   ├── types/           # FromSql/ToSql implementations
│       │   │   ├── duckdb_types.rs  # duck_sql_type! macro + type markers
│       │   │   ├── numeric.rs / varchar.rs / binary.rs / list.rs
│       │   │   └── date_chrono.rs
│       │   └── row.rs           # DieselField implementation
│       └── tests/               # Diesel integration tests
│           └── README.md        # Documents known type gaps
└── docs/benchmarks/             # Benchmark outputs (committed)
```

---

## Git flow

- **`main`** — stable, released code. Only maintainers merge here.
- **`develop`** — integration branch. All PRs target `develop`.
- **feature branches** — branch off `develop`, name them `feat/short-description` or `fix/short-description`.

```sh
git checkout develop
git pull origin develop
git checkout -b feat/uuid-type
# ... do your work ...
git push origin feat/uuid-type
# open a PR targeting develop
```

CI runs on every push to `main` and `develop`, and on every PR targeting those branches.

---

## Commit messages

This repo uses [Conventional Commits](https://www.conventionalcommits.org/) with emoji. The format is:

```
<type>: <emoji> <short description>

[optional body]

[optional footer: "Fixes #123", "BREAKING CHANGE: ..."]
```

**Types and their emoji:**

| Type | Emoji | When to use |
|---|---|---|
| `feat` | ✨ | New feature or new type support |
| `fix` | 🐛 | Bug fix |
| `refactor` | 🛠️ | Code restructuring with no behavior change |
| `test` | ✨ | Adding or improving tests |
| `docs` | 📝 | README, doc comments, CONTRIBUTING |
| `chore` | 🛠️ | Dependencies, CI, build scripts, housekeeping |
| `perf` | ⚡ | Performance improvement |
| `ci` | 👷 | CI workflow changes |

**Examples:**

```
feat: ✨ add FromSql/ToSql for UUID type

Adds DuckValue::Uuid variant, read path in value.rs, DuckValueRef::Uuid,
and diesel HasSqlType + FromSql/ToSql in better-duck-diesel.

Fixes #42
```

```
fix: 🐛 correct TIME_TZ offset decoding

The UTC offset in duckdb_time_tz was being discarded. Now it's preserved
in the TimeTz struct and round-trips correctly.
```

Keep subject lines under 72 characters. Write the body in plain English — what changed and why, not how.

---

## Pull requests

Before marking a PR ready for review, go through this checklist:

- [ ] `cargo fmt --all` produces no changes
- [ ] `cargo clippy --workspace --all-targets --features "chrono,decimal,json,parquet,async,pool,udf,better-duck-diesel/r2d2" -- -D warnings` is clean
- [ ] Package-scoped core, Diesel, and macro tests above pass
- [ ] If adding a new feature, there are tests covering the happy path and at least one error/edge case
- [ ] Public API has doc comments (`///`)
- [ ] Any `unsafe` block has a `// SAFETY:` comment explaining the invariants
- [ ] The PR description explains *why* the change is needed, not just *what* changed

Link the issue your PR addresses with `Fixes #NNN` or `Closes #NNN` in the description. Target the `develop` branch.

---

## Adding a new DuckDB type

The most common contribution is adding support for a type that's in the DuckDB type system but not yet handled. Here's the full checklist, using a hypothetical `UUID` type as the example.

### 1. Add a `DuckValue` variant (core)

In `crates/better-duck-core/src/types/value.rs`, add a variant to the `DuckValue` enum:

```rust
/// The value is a UUID.
Uuid(uuid::Uuid),  // or String if you prefer to keep it dependency-free
```

Add the matching arm in `DuckValueRef` (`value_ref.rs`).

### 2. Add the read path

In `value.rs`, in the `from_duckdb_vec` function, add an arm for the new type tag:

```rust
DUCKDB_TYPE_DUCKDB_TYPE_UUID => {
    // read the raw bytes and construct the DuckValue
    ...
    Ok(DuckValue::Uuid(uuid))
}
```

Remove (or update) the `todo!()` catch-all if you're handling a previously-unhandled tag.

### 3. Register the `Type` enum variant

In `crates/better-duck-core/src/types/mod.rs`, add a `Uuid` variant to the `Type` enum.

### 4. Implement `DuckDialect` (optional but useful)

In `types/numeric.rs` or a new `types/uuid.rs`, implement `DuckDialect<duckdb_value>` for your Rust type so it works with `execute_with`.

### 5. Wire up the Diesel side (if applicable)

In `crates/better-duck-diesel/src/types/duckdb_types.rs`, declare the SQL type marker:

```rust
duck_sql_type!(DuckUuid, "DuckDB UUID type");
```

In `src/backend.rs`, add a `HasSqlType` impl:

```rust
impl HasSqlType<DuckUuid> for DuckDb {
    fn metadata(_: &mut ()) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(Type::Uuid)
    }
}
```

Then add `FromSql` and `ToSql` impls in a suitable `src/types/` module.

### 6. Write tests

Add a round-trip test in `crates/better-duck-core/tests/types_roundtrip.rs` and a Diesel test in `crates/better-duck-diesel/tests/types_roundtrip.rs`.

---

## Code of conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). Be kind, assume good faith, and keep discussions constructive. Violations can be reported to [shakibihamidreza@gmail.com](mailto:shakibihamidreza@gmail.com).
