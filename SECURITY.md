# Security Policy

## Supported versions

We actively maintain the latest published version on crates.io. Security fixes are backported on a best-effort basis.

| Version | Supported |
|---|---|
| `0.1.x` (latest) | ✓ |
| older | best-effort |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

If you've found a security issue — a memory-safety bug, an unsafe FFI contract violation, a way to escape the safe API surface, or anything similar — please report it privately via one of these channels:

1. **GitHub private advisory** (preferred) — use [GitHub's private security advisory feature](https://github.com/nimdeveloper/better-duck/security/advisories/new). We'll acknowledge within 3 business days.
2. **Email** — send details to [shakibihamidreza@gmail.com](mailto:shakibihamidreza@gmail.com) with the subject line `[better-duck] Security`.

Please include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a minimal proof-of-concept
- Affected crate(s) and version(s)
- Any ideas you have for a fix (optional but appreciated)

We aim to triage and patch confirmed vulnerabilities within 14 days, and will coordinate a public disclosure date with you.
