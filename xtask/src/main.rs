//! Maintainer tooling for the `better-duck` workspace. Not published.
//!
//! `cargo run -p xtask -- upgrade-duckdb --tag v1.5.5` regenerates
//! `crates/better-duck-sys/vendor/duckdb.tar.gz` and
//! `crates/better-duck-sys/src/bindings.rs` from a specific upstream
//! `duckdb/duckdb` release tag — the *only* place a git checkout of DuckDB's
//! source ever touches this repo. The clone is ephemeral (a `tempfile`
//! directory, deleted when this process exits, on any exit path); nothing is
//! left behind as a submodule reference, and no consumer of
//! `better-duck-sys` ever needs network access or Python to build it.
//!
//! This calls into DuckDB's own `scripts/package_build.py`
//! (`build_package(...)`) to get the amalgamated (unity-build) source file
//! list — the same officially-provided mechanism the community
//! `libduckdb-sys` crate's own `update_sources.py` uses — rather than
//! reimplementing DuckDB's internal source-selection logic in Rust, which
//! would be fragile against upstream changes.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(name = "xtask", about = "Maintainer tooling for the better-duck workspace")]
struct Cli {
    #[command(subcommand)]
    command: Command_,
}

#[derive(Subcommand)]
enum Command_ {
    /// Regenerate the vendored DuckDB source archive and FFI bindings from a
    /// specific upstream release tag.
    UpgradeDuckdb {
        /// The `duckdb/duckdb` git tag to vendor, e.g. `v1.5.5`.
        #[arg(long)]
        tag: String,
    },
    /// Validate the DuckDB C API capability ledger against documentation and
    /// compiled production Rust sources.
    ///
    /// The upstream C API reference is fetched from `duckdb/duckdb-web` and
    /// filtered in-process, so no copy of it is committed to this repository.
    AuditCapabilities {
        /// Rewrite evidence paths for entries already marked production-used.
        #[arg(long)]
        refresh_evidence: bool,

        /// Re-download the upstream reference even if a local cache exists.
        #[arg(long)]
        refresh_api: bool,

        /// Upstream `duckdb/duckdb-web` ref to read the reference from.
        ///
        /// Defaults to a pinned commit so the audit is reproducible; pass
        /// `main` to check against the latest published documentation.
        #[arg(long, value_name = "REF", default_value = UPSTREAM_API_COMMIT)]
        api_ref: String,

        /// Use a local filtered reference instead of fetching upstream.
        #[arg(long, value_name = "PATH")]
        api_document: Option<PathBuf>,
    },
}

/// This crate's own manifest schema, written into the vendored archive for
/// `better-duck-sys/build.rs` to consume. Deliberately simpler than DuckDB's
/// own `package_build.py` output — we only need "which files, which include
/// dirs, plus one optional extra source list per Cargo feature".
#[derive(Serialize, Deserialize)]
struct Manifest {
    duckdb_version: String,
    sources: Vec<String>,
    json_sources: Vec<String>,
    parquet_sources: Vec<String>,
    include_dirs: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command_::UpgradeDuckdb { tag } => upgrade_duckdb(&tag),
        Command_::AuditCapabilities { refresh_evidence, refresh_api, api_ref, api_document } => {
            audit_capabilities(
                &workspace_root()?,
                refresh_evidence,
                refresh_api,
                &api_ref,
                api_document.as_deref(),
            )
        },
    }
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.parent().map(Path::to_path_buf).context(
        "xtask's CARGO_MANIFEST_DIR has no parent — expected xtask/ under the workspace root",
    )
}

/// Upstream C API reference, fetched rather than vendored.
///
/// `docs/current/` is the only path `duckdb/duckdb-web` publishes for the
/// in-development reference; the per-version directories (`docs/1.5/...`) do
/// not exist on `main`.
const UPSTREAM_API_PATH: &str = "docs/current/clients/c/api.md";

/// Immutable commit the audit reads by default.
///
/// `main` is a moving target: once DuckDB's in-development docs describe the
/// next release, the filtered catalog stops matching this repository's ledger
/// and CI would fail without anything here having changed. Pinning keeps the
/// audit reproducible and makes adopting a newer reference a deliberate,
/// reviewable commit. `duckdb-monitor.yml` watches `main` for drift and opens
/// an issue, so pinning does not hide upstream changes.
const UPSTREAM_API_COMMIT: &str = "49b7052be2f03f39fe8a7783fa16865d52e49136";

fn upstream_api_url(reference: &str) -> String {
    format!("https://raw.githubusercontent.com/duckdb/duckdb-web/{reference}/{UPSTREAM_API_PATH}")
}

/// Where the raw upstream document is cached between runs. Lives under
/// `target/` so it is already git-ignored and removed by `cargo clean`. The
/// reference is part of the name so distinct refs cannot reuse each other's
/// cached bytes.
fn api_cache_path(
    root: &Path,
    reference: &str,
) -> PathBuf {
    let slug: String = reference
        .chars()
        .map(|value| if value.is_ascii_alphanumeric() { value } else { '-' })
        .collect();
    root.join("target").join("xtask").join(format!("upstream-c-api-{slug}.md"))
}

/// Directory holding the exact API reference an audit validated against.
///
/// `docs/current/` is a moving target: once DuckDB's in-development docs
/// advance past the vendored version, the upstream document no longer describes
/// the release this driver targets. CI uploads this directory so every passing
/// run keeps a durable copy of the reference it actually used.
fn api_snapshot_dir(root: &Path) -> PathBuf {
    root.join("target").join("xtask").join("api-snapshot")
}

/// Records the filtered reference, the unfiltered upstream document when one
/// was fetched, and machine-readable provenance for both.
fn write_api_snapshot(
    root: &Path,
    filtered: &str,
    upstream: Option<&str>,
    source: &str,
    api_version: &str,
    retained: usize,
) -> Result<PathBuf> {
    let dir = api_snapshot_dir(root);
    fs::create_dir_all(&dir)?;

    fs::write(dir.join("api.md"), filtered)?;
    if let Some(upstream) = upstream {
        fs::write(dir.join("upstream-c-api.md"), upstream)?;
    }

    let retrieved_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();

    let provenance = serde_json::json!({
        "source": source,
        "retrievedAtUnix": retrieved_at,
        "targetDuckdbVersion": api_version,
        "retainedMethods": retained,
        "filteredDocument": "api.md",
        "upstreamDocument": upstream.map(|_| "upstream-c-api.md"),
        "note": "Deprecated and Arrow methods are removed from the upstream \
                 reference; `api.md` here is the catalog the audit validated.",
    });
    fs::write(
        dir.join("provenance.json"),
        format!("{}\n", serde_json::to_string_pretty(&provenance)?),
    )?;

    Ok(dir)
}

/// Fetches the upstream reference, falling back to the on-disk cache when the
/// network is unavailable so local runs and offline CI retries still work.
fn load_upstream_api(
    root: &Path,
    reference: &str,
    refresh: bool,
) -> Result<String> {
    let cache = api_cache_path(root, reference);
    let url = upstream_api_url(reference);

    if !refresh {
        if let Ok(cached) = fs::read_to_string(&cache) {
            if !cached.trim().is_empty() {
                return Ok(cached);
            }
        }
    }

    match fetch_url(&url) {
        Ok(body) => {
            if let Some(parent) = cache.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&cache, &body)?;
            Ok(body)
        },
        Err(error) => {
            let cached = fs::read_to_string(&cache).map_err(|_| error)?;
            eprintln!("warning: could not fetch {url}; using cached copy at {}", cache.display());
            Ok(cached)
        },
    }
}

/// Downloads a document with `curl`, which every supported CI image and the
/// documented local toolchain already provide (`upgrade-duckdb` likewise shells
/// out to `git`/`python3` rather than vendoring equivalents).
fn fetch_url(url: &str) -> Result<String> {
    let output = Command::new("curl")
        .args(["--fail", "--silent", "--show-error", "--location", "--retry", "3", url])
        .output()
        .context("failed to run `curl` — is it installed and on PATH?")?;

    if !output.status.success() {
        bail!(
            "downloading {url} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    String::from_utf8(output.stdout).context("upstream API document is not valid UTF-8")
}

/// Removes every deprecated and Arrow method from the upstream reference,
/// producing the catalog this driver actually targets.
///
/// Both the linked overview prototype and the detailed section are removed for
/// each excluded method, so the surviving document stays internally consistent.
fn filter_upstream_api(markdown: &str) -> Result<String> {
    let headings: Vec<usize> =
        markdown.match_indices("\n#### `duckdb_").map(|(index, _)| index + 1).collect();
    let first = *headings.first().context("upstream API document has no method headings")?;

    let mut excluded = BTreeSet::new();
    let mut retained_details = String::new();
    for (position, &start) in headings.iter().enumerate() {
        let end = headings.get(position + 1).copied().unwrap_or(markdown.len());
        let block = &markdown[start..end];
        let symbol = block
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("#### `"))
            .and_then(|line| line.strip_suffix('`'))
            .context("malformed method heading in upstream API document")?;

        let deprecated = block.lines().any(|line| {
            line.starts_with("> Warning Deprecation notice") || line.starts_with("> Deprecated")
        });
        let arrow = symbol.to_ascii_lowercase().contains("arrow");

        if deprecated || arrow {
            excluded.insert(symbol.to_owned());
        } else {
            retained_details.push_str(block);
        }
    }

    let mut overview = String::new();
    for line in markdown[..first].lines() {
        let anchors: Vec<&str> = line
            .match_indices("<a href=\"#duckdb_")
            .filter_map(|(index, _)| {
                let rest = &line[index + "<a href=\"#".len()..];
                rest.find('"').map(|end| &rest[..end])
            })
            .collect();

        // The generated overview lists exactly one prototype per physical line,
        // so a line can be dropped wholesale without losing a retained method.
        if !anchors.is_empty() && anchors.iter().any(|symbol| excluded.contains(*symbol)) {
            if anchors.len() != 1 {
                bail!("unexpected multi-method overview line: {line}");
            }
            continue;
        }

        overview.push_str(line);
        overview.push('\n');
    }

    // The page-level deprecation policy and the now-empty Arrow category would
    // otherwise leave excluded terminology in an otherwise filtered document.
    let overview = overview
        .lines()
        .filter(|line| !line.starts_with("> The reference contains several deprecation notices."))
        .collect::<Vec<_>>()
        .join("\n");
    let overview = overview.replace("### Arrow Interface\n\n</code></pre></div></div>\n\n", "");

    Ok(format!("{overview}{retained_details}"))
}

fn audit_capabilities(
    root: &Path,
    refresh_evidence: bool,
    refresh_api: bool,
    api_ref: &str,
    api_document: Option<&Path>,
) -> Result<()> {
    let (markdown, upstream, source) = match api_document {
        Some(path) => {
            let body = fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            (body, None, path.display().to_string())
        },
        None => {
            let upstream = load_upstream_api(root, api_ref, refresh_api)?;
            let filtered = filter_upstream_api(&upstream)?;
            (filtered, Some(upstream), upstream_api_url(api_ref))
        },
    };
    let catalog = ApiCatalog::parse(&markdown)?;
    let ledger_path = root.join("xtask").join("api-capabilities.json");
    let mut ledger: CapabilityLedger = serde_json::from_str(&fs::read_to_string(&ledger_path)?)
        .with_context(|| format!("failed to parse {}", ledger_path.display()))?;
    let production = production_symbols(root)?;
    if refresh_evidence {
        ledger.refresh_evidence(&production)?;
        fs::write(&ledger_path, format!("{}\n", serde_json::to_string_pretty(&ledger)?))?;
    }
    let bindings = fs::read_to_string(
        root.join("crates").join("better-duck-sys").join("src").join("bindings.rs"),
    )?;
    let binding_functions = binding_function_symbols(&bindings)?;
    let report = ledger.validate(&catalog, &production, &binding_functions)?;

    // Written only after validation succeeds, so the snapshot always describes a
    // reference that genuinely reconciled against the ledger.
    let snapshot = write_api_snapshot(
        root,
        &markdown,
        upstream.as_deref(),
        &source,
        &ledger.api_version,
        report.total,
    )?;

    println!(
        "capability audit passed: {} retained = {} production-used + {} pending + {} safe alternatives + {} infrastructure",
        report.total,
        report.production_used,
        report.pending,
        report.safe_alternatives,
        report.infrastructure
    );
    println!("API snapshot written to {}", snapshot.display());
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ApiMethod {
    category: String,
    symbol: String,
}

#[derive(Debug)]
struct ApiCatalog {
    methods: Vec<ApiMethod>,
}

impl ApiCatalog {
    fn parse(markdown: &str) -> Result<Self> {
        let lowercase = markdown.to_ascii_lowercase();
        if lowercase.contains("deprecated") {
            bail!("api.md still contains deprecated API documentation");
        }
        if lowercase.contains("arrow") {
            bail!("api.md still contains Arrow API documentation");
        }
        let first_detail = markdown
            .find("\n#### `duckdb_")
            .context("api.md has no detailed duckdb method headings")?;
        let overview = &markdown[..first_detail];

        let mut methods = Vec::new();
        let mut category = None;
        for line in overview.lines() {
            if let Some(value) = line.strip_prefix("### ") {
                category = Some(value.trim().to_owned());
                continue;
            }
            let Some(anchor) = line.find("<a href=\"#duckdb_") else {
                continue;
            };
            let rest = &line[anchor + "<a href=\"#".len()..];
            let end = rest.find('\"').context("unterminated API overview anchor")?;
            let symbol = &rest[..end];
            let category = category.as_ref().context("API method appears before a category")?;
            methods.push(ApiMethod { category: category.clone(), symbol: symbol.to_owned() });
        }

        let details: Vec<String> = markdown
            .lines()
            .filter_map(|line| {
                line.strip_prefix("#### `duckdb_")
                    .and_then(|rest| rest.strip_suffix('`'))
                    .map(|rest| format!("duckdb_{rest}"))
            })
            .collect();
        let overview_symbols: Vec<&str> =
            methods.iter().map(|method| method.symbol.as_str()).collect();
        let detail_symbols: Vec<&str> = details.iter().map(String::as_str).collect();
        if overview_symbols != detail_symbols {
            bail!("api.md overview and detail methods differ or are out of order");
        }
        let unique: BTreeSet<&str> = overview_symbols.iter().copied().collect();
        if unique.len() != methods.len() {
            bail!("api.md contains duplicate method symbols");
        }
        for method in &methods {
            if method.symbol.to_ascii_lowercase().contains("arrow") {
                bail!("api.md contains excluded Arrow method {}", method.symbol);
            }
        }
        Ok(Self { methods })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityLedger {
    schema_version: u32,
    api_version: String,
    expected_total: usize,
    expected_production_used: usize,
    expected_pending: usize,
    methods: Vec<CapabilityEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityEntry {
    category: String,
    symbol: String,
    current_state: CapabilityState,
    target_disposition: TargetDisposition,
    owner_task: Option<String>,
    evidence: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CapabilityState {
    ProductionUsed,
    Pending,
    SafeAlternative,
    Infrastructure,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TargetDisposition {
    ProductionUsed,
    SafeAlternative,
    Infrastructure,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct AuditReport {
    total: usize,
    production_used: usize,
    pending: usize,
    safe_alternatives: usize,
    infrastructure: usize,
}

impl CapabilityLedger {
    fn refresh_evidence(
        &mut self,
        production: &BTreeMap<String, BTreeSet<String>>,
    ) -> Result<()> {
        for entry in &mut self.methods {
            if entry.current_state == CapabilityState::ProductionUsed {
                entry.evidence = production
                    .get(&entry.symbol)
                    .filter(|paths| !paths.is_empty())
                    .with_context(|| {
                        format!(
                            "{} is marked production_used without a source reference",
                            entry.symbol
                        )
                    })?
                    .iter()
                    .cloned()
                    .collect();
            }
        }
        Ok(())
    }

    fn validate(
        &self,
        catalog: &ApiCatalog,
        production: &BTreeMap<String, BTreeSet<String>>,
        binding_functions: &BTreeSet<String>,
    ) -> Result<AuditReport> {
        if self.schema_version != 1 {
            bail!("unsupported capability ledger schema version {}", self.schema_version);
        }
        if self.api_version != "1.5.5" {
            bail!("capability ledger targets DuckDB {}, expected 1.5.5", self.api_version);
        }
        if self.expected_total != catalog.methods.len() {
            bail!(
                "ledger expected_total is {}, but api.md contains {} methods",
                self.expected_total,
                catalog.methods.len()
            );
        }
        if self.methods.len() != catalog.methods.len() {
            bail!(
                "ledger contains {} entries, but api.md contains {} methods",
                self.methods.len(),
                catalog.methods.len()
            );
        }

        let mut report = AuditReport { total: self.methods.len(), ..AuditReport::default() };
        let mut seen = BTreeSet::new();
        for (documented, entry) in catalog.methods.iter().zip(&self.methods) {
            if !seen.insert(entry.symbol.as_str()) {
                bail!("duplicate capability entry for {}", entry.symbol);
            }
            if documented.symbol != entry.symbol || documented.category != entry.category {
                bail!(
                    "capability entry {} / {} does not match api.md {} / {}",
                    entry.category,
                    entry.symbol,
                    documented.category,
                    documented.symbol
                );
            }
            if !binding_functions.contains(&entry.symbol) {
                bail!("{} is absent from generated binding functions", entry.symbol);
            }
            let references = production.get(&entry.symbol);
            match entry.current_state {
                CapabilityState::ProductionUsed => {
                    let references =
                        references.filter(|paths| !paths.is_empty()).with_context(|| {
                            format!(
                                "{} is marked production_used without a source reference",
                                entry.symbol
                            )
                        })?;
                    if entry.target_disposition != TargetDisposition::ProductionUsed {
                        bail!(
                            "{} production state disagrees with target disposition",
                            entry.symbol
                        );
                    }
                    if entry.owner_task.is_some() {
                        bail!("{} is implemented but still has an owner task", entry.symbol);
                    }
                    if entry.evidence.is_empty() {
                        bail!("{} has no production evidence", entry.symbol);
                    }
                    let evidence: BTreeSet<&str> =
                        entry.evidence.iter().map(String::as_str).collect();
                    if evidence.len() != entry.evidence.len() {
                        bail!("{} contains duplicate evidence paths", entry.symbol);
                    }
                    if !entry.evidence.iter().all(|path| references.contains(path)) {
                        bail!("{} contains stale production evidence", entry.symbol);
                    }
                    report.production_used += 1;
                },
                CapabilityState::Pending => {
                    if references.is_some_and(|paths| !paths.is_empty()) {
                        bail!(
                            "{} is pending but already referenced by production code",
                            entry.symbol
                        );
                    }
                    let task = entry.owner_task.as_deref().unwrap_or_default();
                    if task.is_empty() {
                        bail!("{} is pending without an owner task", entry.symbol);
                    }
                    if !entry.evidence.is_empty() {
                        bail!("{} is pending but has production evidence", entry.symbol);
                    }
                    report.pending += 1;
                },
                CapabilityState::SafeAlternative => {
                    if references.is_some_and(|paths| !paths.is_empty()) {
                        bail!("{} is a safe alternative but is directly referenced", entry.symbol);
                    }
                    if entry.target_disposition != TargetDisposition::SafeAlternative {
                        bail!(
                            "{} safe-alternative state disagrees with target disposition",
                            entry.symbol
                        );
                    }
                    if entry.evidence.is_empty() {
                        bail!("{} has no safe-alternative evidence", entry.symbol);
                    }
                    report.safe_alternatives += 1;
                },
                CapabilityState::Infrastructure => {
                    if references.is_some_and(|paths| !paths.is_empty()) {
                        bail!("{} is infrastructure but is directly referenced", entry.symbol);
                    }
                    if entry.target_disposition != TargetDisposition::Infrastructure {
                        bail!(
                            "{} infrastructure state disagrees with target disposition",
                            entry.symbol
                        );
                    }
                    if entry.evidence.is_empty() {
                        bail!("{} has no infrastructure rationale", entry.symbol);
                    }
                    report.infrastructure += 1;
                },
            }
        }
        if report.production_used != self.expected_production_used {
            bail!(
                "ledger production-used count is {}, expected {}",
                report.production_used,
                self.expected_production_used
            );
        }
        if report.pending != self.expected_pending {
            bail!("ledger pending count is {}, expected {}", report.pending, self.expected_pending);
        }
        Ok(report)
    }
}

fn production_symbols(root: &Path) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let crates = root.join("crates");
    let mut symbols: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in walkdir::WalkDir::new(&crates) {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type().is_file()
            || path.extension().and_then(|ext| ext.to_str()) != Some("rs")
        {
            continue;
        }
        let relative = path.strip_prefix(root)?.to_string_lossy().replace('\\', "/");
        if relative.ends_with("/better-duck-sys/src/bindings.rs")
            || relative.contains("/tests/")
            || relative.contains("/benches/")
            || relative.contains("/examples/")
        {
            continue;
        }
        let source = fs::read_to_string(path)?;
        let syntax = syn::parse_file(&source)
            .with_context(|| format!("failed to parse production source {relative}"))?;
        let mut visitor = ProductionSymbolVisitor::default();
        // Comments and string literals are not syntax-tree identifiers, so they
        // cannot make documentation examples look like production calls.
        syn::visit::Visit::visit_file(&mut visitor, &syntax);
        for symbol in visitor.symbols {
            symbols.entry(symbol).or_default().insert(relative.clone());
        }
    }
    Ok(symbols)
}

#[derive(Default)]
struct ProductionSymbolVisitor {
    test_depth: usize,
    symbols: BTreeSet<String>,
}

impl ProductionSymbolVisitor {
    fn is_test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attr| {
            attr.path().is_ident("test")
                || (attr.path().is_ident("cfg") && cfg_is_test_only(&attr.meta))
        })
    }

    fn record_ident(
        &mut self,
        ident: &syn::Ident,
    ) {
        if self.test_depth == 0 {
            let value = ident.to_string();
            if value.starts_with("duckdb_") {
                self.symbols.insert(value);
            }
        }
    }
}

impl<'ast> syn::visit::Visit<'ast> for ProductionSymbolVisitor {
    fn visit_item(
        &mut self,
        item: &'ast syn::Item,
    ) {
        let test_only = match item {
            syn::Item::Const(value) => Self::is_test_only(&value.attrs),
            syn::Item::Enum(value) => Self::is_test_only(&value.attrs),
            syn::Item::ExternCrate(value) => Self::is_test_only(&value.attrs),
            syn::Item::Fn(value) => Self::is_test_only(&value.attrs),
            syn::Item::ForeignMod(value) => Self::is_test_only(&value.attrs),
            syn::Item::Impl(value) => Self::is_test_only(&value.attrs),
            syn::Item::Macro(value) => Self::is_test_only(&value.attrs),
            syn::Item::Mod(value) => Self::is_test_only(&value.attrs),
            syn::Item::Static(value) => Self::is_test_only(&value.attrs),
            syn::Item::Struct(value) => Self::is_test_only(&value.attrs),
            syn::Item::Trait(value) => Self::is_test_only(&value.attrs),
            syn::Item::TraitAlias(value) => Self::is_test_only(&value.attrs),
            syn::Item::Type(value) => Self::is_test_only(&value.attrs),
            syn::Item::Union(value) => Self::is_test_only(&value.attrs),
            syn::Item::Use(value) => Self::is_test_only(&value.attrs),
            syn::Item::Verbatim(_) | _ => false,
        };
        self.test_depth += usize::from(test_only);
        syn::visit::visit_item(self, item);
        self.test_depth -= usize::from(test_only);
    }

    fn visit_item_use(
        &mut self,
        _item: &'ast syn::ItemUse,
    ) {
        // Deliberately NOT descending into `use` trees. A bare import like
        // `use ffi::duckdb_open;` is not a *use* of the symbol — counting it would
        // let a dead import mark a capability "production_used". A genuine call
        // site (`duckdb_open(...)`, or the bare `duckdb_open` ident in a call after
        // importing it) is still recorded by `visit_ident` elsewhere in the file.
    }

    fn visit_ident(
        &mut self,
        ident: &'ast syn::Ident,
    ) {
        self.record_ident(ident);
        syn::visit::visit_ident(self, ident);
    }

    fn visit_macro(
        &mut self,
        mac: &'ast syn::Macro,
    ) {
        if self.test_depth == 0 {
            record_token_identifiers(&mac.tokens, &mut self.symbols);
        }
        syn::visit::visit_macro(self, mac);
    }
}

fn record_token_identifiers(
    tokens: &proc_macro2::TokenStream,
    symbols: &mut BTreeSet<String>,
) {
    for token in tokens.clone() {
        match token {
            proc_macro2::TokenTree::Ident(ident) => {
                let symbol = ident.to_string();
                if symbol.starts_with("duckdb_") {
                    symbols.insert(symbol);
                }
            },
            proc_macro2::TokenTree::Group(group) => {
                record_token_identifiers(&group.stream(), symbols);
            },
            proc_macro2::TokenTree::Punct(_) | proc_macro2::TokenTree::Literal(_) => {},
        }
    }
}

fn cfg_is_test_only(meta: &syn::Meta) -> bool {
    let syn::Meta::List(cfg) = meta else {
        return false;
    };
    let Ok(predicate) = syn::parse2::<syn::Meta>(cfg.tokens.clone()) else {
        return false;
    };
    predicate_is_test_only(&predicate)
}

fn predicate_is_test_only(meta: &syn::Meta) -> bool {
    match meta {
        syn::Meta::Path(path) => path.is_ident("test"),
        syn::Meta::List(list) if list.path.is_ident("all") => {
            parse_meta_list(list).is_some_and(|items| items.iter().any(predicate_is_test_only))
        },
        syn::Meta::List(list) if list.path.is_ident("any") => parse_meta_list(list)
            .is_some_and(|items| !items.is_empty() && items.iter().all(predicate_is_test_only)),
        syn::Meta::List(list) if list.path.is_ident("not") => false,
        syn::Meta::List(_) | syn::Meta::NameValue(_) => false,
    }
}

fn parse_meta_list(list: &syn::MetaList) -> Option<Vec<syn::Meta>> {
    use syn::parse::Parser as _;
    let parser = syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated;
    parser.parse2(list.tokens.clone()).ok().map(IntoIterator::into_iter).map(Iterator::collect)
}

fn binding_function_symbols(source: &str) -> Result<BTreeSet<String>> {
    let syntax = syn::parse_file(source).context("failed to parse generated bindings")?;
    let mut symbols = BTreeSet::new();
    for item in syntax.items {
        if let syn::Item::ForeignMod(foreign) = item {
            for item in foreign.items {
                if let syn::ForeignItem::Fn(function) = item {
                    let symbol = function.sig.ident.to_string();
                    if symbol.starts_with("duckdb_") {
                        symbols.insert(symbol);
                    }
                }
            }
        }
    }
    Ok(symbols)
}

fn upgrade_duckdb(tag: &str) -> Result<()> {
    let root = workspace_root()?;
    let sys_crate = root.join("crates").join("better-duck-sys");
    if !sys_crate.is_dir() {
        bail!("expected {} to exist", sys_crate.display());
    }

    let work = tempfile_dir("better-duck-xtask-duckdb")?;
    println!("==> shallow-cloning duckdb/duckdb@{tag} into {}", work.display());
    clone_tag(tag, &work)?;

    // Deliberately NOT nested under `work` (the checkout): `package_build.py`
    // returns some paths (from `amalgamation.list_sources()`) relative to the
    // checkout root, so a staging dir inside the checkout would make
    // `collect_sources`'s prefix-stripping ambiguous between "relative to the
    // checkout" and "relative to staging".
    let staging = tempfile_dir("better-duck-xtask-staging")?;
    println!("==> collecting the amalgamated source file list via package_build.py");
    let manifest = collect_sources(&work, &staging, tag)?;

    println!("==> writing manifest.json + packaging vendor/duckdb.tar.gz");
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    fs::write(staging.join("manifest.json"), &manifest_json)?;
    let vendor_dir = sys_crate.join("vendor");
    fs::create_dir_all(&vendor_dir)?;
    package_archive(&staging, &vendor_dir.join("duckdb.tar.gz"))?;

    println!("==> regenerating src/bindings.rs via bindgen");
    let header = work.join("src").join("include").join("duckdb.h");
    if !header.is_file() {
        bail!("expected DuckDB's C header at {}", header.display());
    }
    generate_bindings(&header, &sys_crate.join("src").join("bindings.rs"))?;

    println!(
        "==> done. Review the diff, then commit crates/better-duck-sys/{{vendor/duckdb.tar.gz,src/bindings.rs}}."
    );
    // `work` (the clone) is a tempfile::TempDir — deleted automatically when
    // it drops here, on every return path including `?` early-returns above,
    // since it's owned by this function's stack frame throughout.
    Ok(())
}

/// A directory deleted on drop, regardless of how this function returns.
/// Avoids taking a `tempfile` dependency for one call site: `TempDirGuard`
/// wraps a plain path and removes it in `Drop`.
struct TempDirGuard(PathBuf);
impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
impl std::ops::Deref for TempDirGuard {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

fn tempfile_dir(prefix: &str) -> Result<TempDirGuard> {
    let base = std::env::temp_dir();
    let unique = format!("{prefix}-{}", std::process::id());
    let dir = base.join(unique);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(TempDirGuard(dir))
}

fn clone_tag(
    tag: &str,
    dest: &Path,
) -> Result<()> {
    let status = Command::new("git")
        .args(["clone", "--depth", "1", "--branch", tag, "https://github.com/duckdb/duckdb.git"])
        .arg(dest)
        .status()
        .context("failed to run `git clone` — is git installed and on PATH?")?;
    if !status.success() {
        bail!("git clone of duckdb/duckdb@{tag} failed (exit code {status})");
    }
    Ok(())
}

/// Runs a small embedded Python helper (mirroring the relevant part of
/// `libduckdb-sys`'s own `update_sources.py`) that imports DuckDB's
/// `scripts/package_build.py` and calls `build_package(...)` to get the
/// amalgamated source/include lists, then copies exactly those files into
/// `staging`.
fn collect_sources(
    duckdb_checkout: &Path,
    staging: &Path,
    tag: &str,
) -> Result<Manifest> {
    fs::create_dir_all(staging)?;
    let helper = duckdb_checkout.join("_better_duck_collect_sources.py");
    fs::write(&helper, COLLECT_SOURCES_PY)?;

    // Outside `staging` deliberately: everything under `staging` gets tar'd up
    // verbatim as the vendored archive (see `package_archive`), and this file
    // is just an intermediate handoff to the Rust side, not part of the manifest.
    let output_json = duckdb_checkout.join("_raw_sources.json");
    let status = Command::new("python3")
        .arg(&helper)
        .arg(duckdb_checkout)
        .arg(staging)
        .arg(&output_json)
        .status()
        .context("failed to run python3 — is Python 3 installed and on PATH?")?;
    if !status.success() {
        bail!("package_build.py source collection failed (exit code {status})");
    }

    #[derive(Deserialize)]
    struct RawSources {
        base_cpp_files: Vec<String>,
        base_include_dirs: Vec<String>,
        json_cpp_files: Vec<String>,
        parquet_cpp_files: Vec<String>,
    }
    let raw: RawSources = serde_json::from_str(
        &fs::read_to_string(&output_json).context("reading intermediate sources JSON")?,
    )?;

    Ok(Manifest {
        duckdb_version: tag.to_owned(),
        sources: raw.base_cpp_files,
        json_sources: raw.json_cpp_files,
        parquet_sources: raw.parquet_cpp_files,
        include_dirs: raw.base_include_dirs,
    })
}

/// Copies `staging`'s contents into a `tar.gz` at `dest`, sorted, with a
/// fixed mtime — reproducible archive bytes for a given source tree, so an
/// unrelated re-run doesn't churn the committed archive.
fn package_archive(
    staging: &Path,
    dest: &Path,
) -> Result<()> {
    let file = fs::File::create(dest)?;
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::best());
    let mut builder = tar::Builder::new(encoder);
    let mut entries: Vec<PathBuf> = walkdir::WalkDir::new(staging)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.path().to_path_buf())
        .collect();
    entries.sort();
    for path in entries {
        let rel = path.strip_prefix(staging)?;
        builder.append_path_with_name(&path, rel)?;
    }
    builder.into_inner()?.finish()?;
    Ok(())
}

fn generate_bindings(
    header: &Path,
    dest: &Path,
) -> Result<()> {
    let bindings = bindgen::Builder::default()
        .generate_comments(true)
        .raw_line("#![allow(rustdoc::invalid_html_tags)]")
        .raw_line("#![allow(rustdoc::broken_intra_doc_links)]")
        .raw_line("#![allow(rustdoc::invalid_rust_codeblocks)]")
        .raw_line("#![allow(rustdoc::bare_urls)]")
        .header(header.to_string_lossy().to_string())
        .allowlist_function("duckdb_.*")
        .allowlist_type("duckdb_.*")
        .allowlist_var("DUCKDB_.*")
        .generate()
        .map_err(|e| anyhow::anyhow!("bindgen failed: {e}"))?;
    bindings.write_to_file(dest).context("writing generated bindings")?;
    Ok(())
}

/// Embedded helper — kept as an external-process Python script (not
/// reimplemented in Rust) so it can call DuckDB's own
/// `scripts/package_build.py` directly, exactly like `libduckdb-sys`'s own
/// `update_sources.py` does. Only used by this xtask, at maintainer-upgrade
/// time; never runs on a consumer's machine.
const COLLECT_SOURCES_PY: &str = r#"
import json
import shutil
import sys
from pathlib import Path

duckdb_checkout = Path(sys.argv[1]).resolve()
staging = Path(sys.argv[2]).resolve()
output_json = Path(sys.argv[3]).resolve()

# `package_build.build_package(target_dir, ...)` (default `folder_name="duckdb"`)
# returns two different path shapes in its source list: ordinary (non-unity-
# build) files come back as `"duckdb/<relative-to-checkout>"` — meant to be
# resolved against `target_dir`'s *parent* — while unity-build (`ub_*.cpp`)
# files it generates itself come back as absolute paths directly under
# `target_dir`. Passing `target_dir = staging/"duckdb"` makes both shapes
# consistent once normalized below: everything ends up expressed relative to
# `staging`, matching where the files were actually written on disk (which is
# what gets tar'd up as the vendored archive).
target_dir = staging / "duckdb"
staging_prefix = staging.as_posix() + "/"

sys.path.append(str(duckdb_checkout / "scripts"))
import package_build

def normalize(path_str):
    p = path_str.replace("\\", "/")
    if p.startswith(staging_prefix):
        return p[len(staging_prefix):]
    return p  # already "duckdb/<relative>" per folder_name

# `core_functions` (basic scalar/aggregate functions like `sum`, `abs`, ...)
# is unconditionally required — DuckDB's own `extension/extension_config.cmake`
# calls it out by name as "loaded by default on every build as [it is] an
# essential part of DuckDB", unlike `json`/`parquet` which really are optional
# file-format support. It's always in `default_linked_extensions` below so its
# `DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED` fallback in the generated loader is
# unconditionally 1 — no Cargo feature gates it.
extensions = ["core_functions", "json", "parquet"]
# `default_linked_extensions` matters: `build_package` bakes each extension's
# "is it linked" flag into the *content* of the single shared
# `generated_extension_loader_package_build.cpp` file as
# `#ifndef DUCKDB_EXTENSION_X_LINKED / #define ... <default> / #endif`. For
# `json`/`parquet` that fallback is 0 (off), and the `#ifndef` guard means our
# own `-D DUCKDB_EXTENSION_X_LINKED` (added by `better-duck-sys/build.rs` only
# when the matching Cargo feature is on) wins whenever it's present. This file
# is compiled unconditionally as a "base" source, so json/parquet's behavior
# must be controlled entirely by our own `-D` flags, not by whatever extension
# set happened to be passed to `build_package` here.
(source_list, include_list, _) = package_build.build_package(
    str(target_dir), extensions, False, default_linked_extensions=["core_functions"]
)
loader = duckdb_checkout / "generated_extension_loader_package_build.cpp"
loader.unlink(missing_ok=True)

sources = {normalize(s) for s in source_list}
# `include_list` isn't run through the `folder_name` convention by
# `build_package` itself (unlike `source_list`) even though the actual
# header files live under `target_dir` just like the sources — prefix it
# the same way by hand so `-I` dirs resolve consistently with `sources`.
includes = {f"duckdb/{i}" for i in include_list}

# A single `build_package` call (rather than one call per extension
# combination) avoids each call clobbering the previous one's copy of the
# shared loader file on disk. Split the one unified list back apart by each
# extension's own source directory instead.
json_sources = {s for s in sources if "duckdb/extension/json/" in s}
parquet_sources = {s for s in sources if "duckdb/extension/parquet/" in s}
base_sources = sources - json_sources - parquet_sources

result = {
    "base_cpp_files": sorted(base_sources),
    "base_include_dirs": sorted(includes),
    "json_cpp_files": sorted(json_sources),
    "parquet_cpp_files": sorted(parquet_sources),
}
with output_json.open("w") as f:
    json.dump(result, f, indent=2, sort_keys=True)
"#;

#[cfg(test)]
mod capability_tests {
    use super::*;

    const API: &str = r##"## API Reference Overview

### Open Connect
<div><a href="#duckdb_open"><span>duckdb_open</span></a>();
<a href="#duckdb_interrupt"><span>duckdb_interrupt</span></a>();</div>

#### `duckdb_open`

Open.

#### `duckdb_interrupt`

Interrupt.
"##;

    fn ledger(methods: Vec<CapabilityEntry>) -> CapabilityLedger {
        CapabilityLedger {
            schema_version: 1,
            api_version: "1.5.5".to_owned(),
            expected_total: methods.len(),
            expected_production_used: 1,
            expected_pending: 1,
            methods,
        }
    }

    fn used_entry() -> CapabilityEntry {
        CapabilityEntry {
            category: "Open Connect".to_owned(),
            symbol: "duckdb_open".to_owned(),
            current_state: CapabilityState::ProductionUsed,
            target_disposition: TargetDisposition::ProductionUsed,
            owner_task: None,
            evidence: vec!["crates/core/src/open.rs".to_owned()],
        }
    }

    fn pending_entry() -> CapabilityEntry {
        CapabilityEntry {
            category: "Open Connect".to_owned(),
            symbol: "duckdb_interrupt".to_owned(),
            current_state: CapabilityState::Pending,
            target_disposition: TargetDisposition::ProductionUsed,
            owner_task: Some("E5".to_owned()),
            evidence: Vec::new(),
        }
    }

    #[test]
    fn api_catalog_requires_matching_unique_overview_and_details() {
        let catalog = ApiCatalog::parse(API).unwrap();
        assert_eq!(catalog.methods.len(), 2);
        assert_eq!(catalog.methods[1].symbol, "duckdb_interrupt");

        let duplicate = API.replace("#### `duckdb_interrupt`", "#### `duckdb_open`");
        assert!(ApiCatalog::parse(&duplicate).is_err());
        assert!(ApiCatalog::parse(&API.replace("Interrupt.", "Arrow interface.")).is_err());
        assert!(ApiCatalog::parse(&API.replace("Interrupt.", "Deprecated method.")).is_err());
    }

    #[test]
    fn ledger_reconciles_current_and_pending_capabilities() {
        let catalog = ApiCatalog::parse(API).unwrap();
        let ledger = ledger(vec![used_entry(), pending_entry()]);
        let production = BTreeMap::from([(
            "duckdb_open".to_owned(),
            BTreeSet::from(["crates/core/src/open.rs".to_owned()]),
        )]);
        let bindings = BTreeSet::from(["duckdb_open".to_owned(), "duckdb_interrupt".to_owned()]);
        let report = ledger.validate(&catalog, &production, &bindings).unwrap();
        assert_eq!(
            report,
            AuditReport {
                total: 2,
                production_used: 1,
                pending: 1,
                safe_alternatives: 0,
                infrastructure: 0,
            }
        );
    }

    #[test]
    fn ledger_rejects_stale_states_evidence_and_bindings() {
        let catalog = ApiCatalog::parse(API).unwrap();
        let production = BTreeMap::from([(
            "duckdb_open".to_owned(),
            BTreeSet::from(["crates/core/src/open.rs".to_owned()]),
        )]);

        let bindings = BTreeSet::from(["duckdb_open".to_owned(), "duckdb_interrupt".to_owned()]);

        let mut entries = vec![used_entry(), pending_entry()];
        entries[0].evidence.clear();
        assert!(ledger(entries).validate(&catalog, &production, &bindings).is_err());

        let mut entries = vec![used_entry(), pending_entry()];
        entries[0].evidence = vec!["crates/core/src/stale.rs".to_owned()];
        assert!(ledger(entries).validate(&catalog, &production, &bindings).is_err());

        let mut entries = vec![used_entry(), pending_entry()];
        entries[1].owner_task = None;
        assert!(ledger(entries).validate(&catalog, &production, &bindings).is_err());

        let incomplete_bindings = BTreeSet::from(["duckdb_open".to_owned()]);
        assert!(ledger(vec![used_entry(), pending_entry()])
            .validate(&catalog, &production, &incomplete_bindings)
            .is_err());
    }

    #[test]
    fn production_visitor_excludes_only_provably_test_only_items_and_reads_macros() {
        let file = syn::parse_file(
            r#"
            use ffi::duckdb_open;
            use ffi::duckdb_dead_import;
            #[cfg(test)]
            mod tests { use ffi::duckdb_interrupt; }
            #[test]
            fn test_function() { ffi::duckdb_free(); }
            #[cfg(any(test, feature = "udf"))]
            fn maybe_production() { ffi::duckdb_query(); }
            #[cfg(all(test, feature = "udf"))]
            fn test_only_all() { ffi::duckdb_connect(); }
            fn uses_the_import() { duckdb_open(); }
            bind_function!(duckdb_bind_int32);
            "#,
        )
        .unwrap();
        let mut visitor = ProductionSymbolVisitor::default();
        syn::visit::Visit::visit_file(&mut visitor, &file);
        assert_eq!(
            visitor.symbols,
            BTreeSet::from([
                // recorded via the macro token walk
                "duckdb_bind_int32".to_owned(),
                // imported AND called -> recorded via the call-site ident, not the `use`
                "duckdb_open".to_owned(),
                // called in a non-test, feature-gated fn
                "duckdb_query".to_owned(),
            ]),
            "a bare `use` (duckdb_dead_import) must NOT count; a called import (duckdb_open) must"
        );
        assert!(
            !visitor.symbols.contains("duckdb_dead_import"),
            "a symbol only ever imported, never called, must not be production_used"
        );
    }

    #[test]
    fn binding_parser_requires_foreign_functions() {
        let bindings = binding_function_symbols(
            r#"
            // duckdb_comment_only
            pub type duckdb_type_only = *mut ::std::os::raw::c_void;
            unsafe extern "C" { pub fn duckdb_open(); }
            "#,
        )
        .unwrap();
        assert_eq!(bindings, BTreeSet::from(["duckdb_open".to_owned()]));
    }
}
