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

/// Append TypedDict + module-constant names to the native stub's `__all__`.
///
/// Just before the closing `]` of the `__all__` block, splices in every
/// TypedDict name (sorted) followed by every module constant. Idempotent: names
/// already present are skipped, so re-running over a patched stub is a no-op.
fn append_to_all_block(lines: &[&str], output: &mut String) {
    let names = typed_dict_names();

    // Collect the entries already present in the `__all__` block so we don't
    // re-add them. Entries look like `    "Foo",` — pull the quoted name out.
    let existing: std::collections::HashSet<String> = {
        let mut set = std::collections::HashSet::new();
        let mut scanning = false;
        for line in lines {
            if line.contains("__all__") && line.contains('[') {
                scanning = true;
            }
            if scanning {
                if line.trim_start().starts_with(']') {
                    break;
                }
                if let Some(name) = line.trim().trim_end_matches(',').strip_prefix('"') {
                    if let Some(name) = name.strip_suffix('"') {
                        set.insert(name.to_string());
                    }
                }
            }
        }
        set
    };

    let mut in_all_block = false;

    for line in lines {
        if line.contains("__all__") && line.contains('[') {
            in_all_block = true;
        }
        if in_all_block && line.trim_start().starts_with(']') {
            for name in &names {
                if !existing.contains(*name) {
                    output.push_str(&format!("    \"{name}\",\n"));
                }
            }
            for (const_name, _) in MODULE_CONSTANTS {
                if !existing.contains(*const_name) {
                    output.push_str(&format!("    \"{const_name}\",\n"));
                }
            }
            in_all_block = false;
        }

        output.push_str(line);
        output.push('\n');
    }
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

    let lines: Vec<&str> = content.lines().collect();

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

    // Pass 1: inject TypedDicts + module constants, splice `import datetime`,
    // and rewrite `def` → `async def`. The `__all__` splicing is left to pass 2
    // (`append_to_all_block`) so it isn't duplicated here — the `__all__` block
    // always precedes `insert_before`, so the two passes don't fight.
    let mut pass1 = String::with_capacity(content.len() + typed_dicts.len());

    for (i, line) in lines.iter().enumerate() {
        // Inject TypedDicts (then module constants) right before the first class.
        if i == insert_before {
            pass1.push_str(typed_dicts);
            pass1.push('\n');
            for (const_name, const_type) in MODULE_CONSTANTS {
                pass1.push_str(&format!("{const_name}: {const_type}\n"));
            }
            pass1.push('\n');
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
                pass1.push_str(indent);
                pass1.push_str("async def ");
                pass1.push_str(after_def);
                pass1.push('\n');
                continue;
            }
        }

        pass1.push_str(line);
        pass1.push('\n');

        // After the last existing import, splice in `import datetime` if missing.
        if !has_datetime_import && last_import_idx == Some(i) {
            pass1.push_str("import datetime\n");
        }
    }

    // Pass 2: append the TypedDict + constant names to `__all__`.
    let pass1_lines: Vec<&str> = pass1.lines().collect();
    let mut output = String::with_capacity(pass1.len());
    append_to_all_block(&pass1_lines, &mut output);

    std::fs::write(path, output)?;
    Ok(())
}

/// Module docstring restored onto the generated top-level `__init__.py`.
const TOPLEVEL_DOCSTRING: &str =
    "\"\"\"Python bindings for crawlee-storage (Rust-powered filesystem storage clients).\"\"\"";

/// Post-process the generated top-level `crawlee_storage/__init__.py`.
///
/// The generator emits a runtime `__init__.py` (re-exports + `__all__`) from
/// the native module's classes, but can't reproduce two things, so we patch
/// them back in:
///
/// 1. the module docstring (placed first so it counts as `__doc__`), and
/// 2. the `NONE_CONTENT_TYPE` constant — it's added at runtime via `m.add(...)`
///    (see the `#[pymodule]` init), which the generator doesn't track, so it's
///    neither imported nor listed in `__all__`.
///
/// TypedDict names are deliberately **not** added here: they're type-only (no
/// runtime binding), so listing them in the runtime `__all__` would break
/// `from crawlee_storage import *`. Type checkers see them via the companion
/// `__init__.pyi` from `write_toplevel_pyi`.
fn fixup_init_py(path: &std::path::Path) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;

    let mut output = String::with_capacity(content.len() + 256);
    // Module docstring must be the first statement to count as `__doc__`.
    output.push_str(TOPLEVEL_DOCSTRING);
    output.push_str("\n\n");

    for line in content.lines() {
        // Add the runtime-only constant to the native re-export import if absent.
        if line.starts_with("from crawlee_storage._native import ")
            && !line.contains("NONE_CONTENT_TYPE")
        {
            output.push_str(&line.replacen("import ", "import NONE_CONTENT_TYPE, ", 1));
            output.push('\n');
            continue;
        }

        // Add NONE_CONTENT_TYPE to __all__ if it's missing (the generator omits
        // runtime `m.add` constants). Splice it just before the closing bracket.
        if line.trim_start().starts_with(']') && !output.contains("\"NONE_CONTENT_TYPE\"") {
            output.push_str("    \"NONE_CONTENT_TYPE\",\n");
        }

        output.push_str(line);
        output.push('\n');
    }

    std::fs::write(path, output)?;
    Ok(())
}

/// Write the top-level `crawlee_storage/__init__.pyi` type stub.
///
/// A `.pyi` alongside the runtime `__init__.py` lets `crawlee_storage` (not just
/// `crawlee_storage._native`) expose the TypedDict names to type checkers. It
/// re-exports everything from the native module — the runtime classes, the
/// constant, and all the TypedDicts.
fn write_toplevel_pyi(path: &std::path::Path) -> std::io::Result<()> {
    let mut out = String::new();
    out.push_str("# This file is automatically generated by stub_gen\n");
    out.push_str("# ruff: noqa: E501, F401, F403, F405\n\n");
    out.push_str("from crawlee_storage._native import *\n");
    out.push_str("from crawlee_storage._native import NONE_CONTENT_TYPE as NONE_CONTENT_TYPE\n\n");

    out.push_str("__all__ = [\n");
    for name in [
        "DatasetItemIterator",
        "FileSystemDatasetClient",
        "FileSystemKeyValueStoreClient",
        "FileSystemRequestQueueClient",
        "KvsKeyIterator",
        "NONE_CONTENT_TYPE",
    ] {
        out.push_str(&format!("    \"{name}\",\n"));
    }
    for name in typed_dict_names() {
        out.push_str(&format!("    \"{name}\",\n"));
    }
    out.push_str("]\n");

    std::fs::write(path, out)?;
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

    // Patch the generated runtime __init__.py (docstring + NONE_CONTENT_TYPE).
    let toplevel_init_py = python_dir.join("__init__.py");
    if toplevel_init_py.exists() {
        fixup_init_py(&toplevel_init_py).expect("Failed to post-process top-level __init__.py");
        eprintln!(
            "Post-processed top-level __init__.py (docstring, NONE_CONTENT_TYPE, __all__) at {}",
            toplevel_init_py.display()
        );
    }

    // Write the companion top-level type stub (re-exports the TypedDicts).
    let toplevel_pyi = python_dir.join("__init__.pyi");
    write_toplevel_pyi(&toplevel_pyi).expect("Failed to write top-level __init__.pyi");
    eprintln!("Wrote top-level type stub {}", toplevel_pyi.display());

    // Format the generated files so they match the project's ruff conventions.
    format_stubs(&[&native_stub_path, &toplevel_init_py, &toplevel_pyi]);

    Ok(())
}
