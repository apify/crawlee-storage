//! Generates and post-processes the `.pyi` type stubs.
//!
//! `pyo3-stub-gen` produces most of the stub, but it can't express two things on
//! its own, so this binary patches them in:
//!
//! 1. **`TypedDict` definitions** for the camelCase dicts the bindings return.
//!    A method returning a `dict` is just `dict[str, Any]` to the generator, so
//!    the real shapes are declared as `TypedDict`s. Their field lists come
//!    *directly* from the rustc-checked `TypedDictModel` specs in
//!    `crate::models` (see that module for why) — no JSON-shape guessing, no
//!    per-field override table.
//!
//! 2. **`async def` markers**: methods built on
//!    `pyo3_async_runtimes::tokio::future_into_py` appear as plain sync `fn` to
//!    `pyo3-stub-gen`, so we rewrite them to `async def` here (everything is
//!    async except a small, explicit sync list).
//!
//! Plus housekeeping: inject module constants, ensure `import datetime`, append
//! TypedDict names to `__all__`, and run `ruff` to format + PEP 604-ify
//! (`Optional[X]` → `X | None`) the result.

use _crawlee_storage::models::{self, TypedDictField};
use pyo3_stub_gen::Result;

/// Method names that should remain synchronous (not marked async).
const SYNC_METHODS: &[&str] = &[
    "iterate_items",
    "iterate_keys",
    // advance_clock_for_testing is a plain sync PyO3 method — it doesn't
    // go through `future_into_py`, so its stub must not be `async`.
    "advance_clock_for_testing",
];

/// Dunder methods that ARE async (all other dunders stay sync).
const ASYNC_DUNDERS: &[&str] = &["__anext__", "__aenter__", "__aexit__"];

/// Module-level constants exported via `m.add(...)` in the `#[pymodule]` init.
/// pyo3-stub-gen does not track runtime `m.add` calls, so the generated `.pyi`
/// omits them — we inject the declarations (and `__all__` entries) here.
/// Maps constant name → Python type annotation.
const MODULE_CONSTANTS: &[(&str, &str)] = &[("NONE_CONTENT_TYPE", "builtins.str")];

// ─── TypedDict generation (from rustc-checked model specs) ──────────────────

/// Render one `TypedDict` class body from a model's `(name, fields)` spec.
fn render_typed_dict(name: &str, fields: &[TypedDictField]) -> String {
    let mut out = format!("class {name}(typing.TypedDict):\n");
    for field in fields {
        out.push_str(&format!("    {}: {}\n", field.key, field.py_type));
    }
    out
}

/// All TypedDict definitions as a single string block, in `all_specs()` order.
fn generate_typed_dicts() -> String {
    let mut out = String::new();
    for (name, fields) in models::all_specs() {
        out.push('\n');
        out.push_str(&render_typed_dict(name, fields));
    }
    out
}

/// TypedDict class names, sorted (for `__all__` injection).
fn typed_dict_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = models::all_specs().into_iter().map(|(n, _)| n).collect();
    names.sort_unstable();
    names
}

// ─── Stub file post-processing ──────────────────────────────────────────────

/// Post-process a generated `.pyi` stub file:
/// 1. Inject `TypedDict` definitions (and module constants) before the first class.
/// 2. Append TypedDict + constant names to `__all__`.
/// 3. Mark `future_into_py`-based methods as `async def`.
/// 4. Ensure `import datetime` is present (the metadata TypedDicts reference it).
///
/// PEP 604 rewriting (`Optional[X]` → `X | None`) and formatting are handled by
/// `ruff` in `format_stubs`, not here.
fn fixup_stubs(path: &std::path::Path, typed_dicts: &str) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut output = String::with_capacity(content.len() + typed_dicts.len());

    let lines: Vec<&str> = content.lines().collect();
    let names = typed_dict_names();

    // Ensure `import datetime` is present — the metadata TypedDicts reference
    // `datetime.datetime`. pyo3_stub_gen only adds it when a method signature
    // references it directly, so if `set_expected_request_processing_time`
    // ever loses its timedelta arg, we'd still need it for the TypedDicts.
    let has_datetime_import = lines.iter().any(|l| l.trim() == "import datetime");

    // Find the insertion point: after imports and __all__, before the first class.
    let insert_before = lines
        .iter()
        .position(|line| line.starts_with("@typing.final") || line.starts_with("class "))
        .unwrap_or(lines.len());

    // Find the last `import`/`from` line — where we splice in `import datetime`.
    let last_import_idx = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("import ") || l.starts_with("from "))
        .map(|(i, _)| i)
        .next_back();

    // Track whether we're inside the __all__ block so we can append names.
    let mut in_all_block = false;

    for (i, line) in lines.iter().enumerate() {
        // Inject TypedDicts (then module constants) right before the first class.
        if i == insert_before {
            output.push_str(typed_dicts);
            output.push('\n');
            for (const_name, const_type) in MODULE_CONSTANTS {
                output.push_str(&format!("{const_name}: {const_type}\n"));
            }
            output.push('\n');
        }

        // Detect __all__ = [ ... ] and inject names before the closing `]`.
        if line.contains("__all__") && line.contains('[') {
            in_all_block = true;
        }
        if in_all_block && line.trim_start().starts_with(']') {
            for name in &names {
                output.push_str(&format!("    \"{name}\",\n"));
            }
            for (const_name, _) in MODULE_CONSTANTS {
                output.push_str(&format!("    \"{const_name}\",\n"));
            }
            in_all_block = false;
        }

        let trimmed = line.trim_start();

        if let Some(after_def) = trimmed.strip_prefix("def ") {
            // Extract method name: "foo(" -> "foo"
            let method_name = after_def.split('(').next().unwrap_or("");

            // Check if the previous non-empty line is a @property decorator.
            let is_property = (0..i)
                .rev()
                .find(|&j| !lines[j].trim().is_empty())
                .is_some_and(|j| lines[j].trim() == "@property");

            let is_dunder = method_name.starts_with("__");
            let is_sync = SYNC_METHODS.contains(&method_name)
                || (is_dunder && !ASYNC_DUNDERS.contains(&method_name))
                || is_property;

            if !is_sync {
                // Replace "def " with "async def ", preserving indentation.
                let indent = &line[..line.len() - trimmed.len()];
                output.push_str(indent);
                output.push_str("async def ");
                output.push_str(after_def);
                output.push('\n');
                continue;
            }
        }

        output.push_str(line);
        output.push('\n');

        // After the last existing import, splice in `import datetime` if missing.
        if !has_datetime_import && last_import_idx == Some(i) {
            output.push_str("import datetime\n");
        }
    }

    std::fs::write(path, output)?;
    Ok(())
}

/// Append TypedDict + constant names to the `__all__` list in a re-export stub.
fn fixup_reexport_stubs(path: &std::path::Path) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut output = String::with_capacity(content.len());

    let lines: Vec<&str> = content.lines().collect();
    let names = typed_dict_names();
    let mut in_all_block = false;

    for line in &lines {
        if line.contains("__all__") && line.contains('[') {
            in_all_block = true;
        }
        if in_all_block && line.trim_start().starts_with(']') {
            for name in &names {
                output.push_str(&format!("    \"{name}\",\n"));
            }
            for (const_name, _) in MODULE_CONSTANTS {
                output.push_str(&format!("    \"{const_name}\",\n"));
            }
            in_all_block = false;
        }

        output.push_str(line);
        output.push('\n');
    }

    std::fs::write(path, output)?;
    Ok(())
}

/// Format the generated stubs with `ruff`: sort imports (`check --fix --select
/// I`), upgrade typing syntax to PEP 604 (`--select UP` rewrites
/// `Optional[X]` → `X | None`), then `ruff format`. Best-effort: a missing
/// `ruff` only warns, since the stubs are otherwise valid.
fn format_stubs(paths: &[&std::path::Path]) {
    let existing: Vec<&std::path::Path> = paths.iter().copied().filter(|p| p.exists()).collect();
    if existing.is_empty() {
        return;
    }

    // 1. Sort imports and modernize typing syntax (Optional[X] -> X | None).
    //    Doing this before `format` avoids fighting over blank-line groupings.
    let mut fix = std::process::Command::new("ruff");
    fix.arg("check")
        .arg("--fix")
        .arg("--select")
        .arg("I,UP")
        .args(&existing);
    match fix.status() {
        Ok(status) if status.success() => {
            eprintln!("Applied `ruff check --fix --select I,UP` to stubs");
        }
        Ok(status) => {
            eprintln!("Warning: `ruff check --fix` exited with status {status}");
        }
        Err(err) => {
            eprintln!("Warning: could not run `ruff check --fix` ({err}); stubs left unfixed");
            return;
        }
    }

    // 2. Format.
    let mut fmt = std::process::Command::new("ruff");
    fmt.arg("format").args(&existing);
    match fmt.status() {
        Ok(status) if status.success() => {
            eprintln!("Formatted stubs with `ruff format`");
        }
        Ok(status) => {
            eprintln!("Warning: `ruff format` exited with status {status}; stubs left unformatted");
        }
        Err(err) => {
            eprintln!("Warning: could not run `ruff format` ({err}); stubs left unformatted.");
        }
    }
}

fn main() -> Result<()> {
    let stub = _crawlee_storage::stub_info()?;
    stub.generate()?;

    let typed_dicts = generate_typed_dicts();

    let manifest_dir: &std::path::Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let python_dir = manifest_dir.join("python").join("crawlee_storage");

    // Post-process _native/__init__.pyi: inject TypedDicts and async markers.
    let native_stub_path = python_dir.join("_native").join("__init__.pyi");
    if native_stub_path.exists() {
        fixup_stubs(&native_stub_path, &typed_dicts).expect("Failed to post-process native stubs");
        eprintln!(
            "Post-processed stubs: injected TypedDicts and async markers into {}",
            native_stub_path.display()
        );
    }

    // Post-process top-level __init__.pyi: add TypedDict names to __all__.
    let toplevel_stub_path = python_dir.join("__init__.pyi");
    if toplevel_stub_path.exists() {
        fixup_reexport_stubs(&toplevel_stub_path).expect("Failed to post-process top-level stubs");
        eprintln!(
            "Post-processed stubs: added TypedDict names to __all__ in {}",
            toplevel_stub_path.display()
        );
    }

    // Format the generated stubs so they match the project's ruff conventions.
    format_stubs(&[&native_stub_path, &toplevel_stub_path]);

    Ok(())
}
