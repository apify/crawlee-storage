use pyo3_stub_gen::Result;
use std::path::PathBuf;

/// Method names that should remain synchronous (not marked async).
const SYNC_METHODS: &[&str] = &["get_public_url"];

/// Post-process a generated `.pyi` stub file to mark methods as `async def`
/// where appropriate. pyo3_stub_gen cannot detect async methods that use
/// `pyo3_async_runtimes::tokio::future_into_py` (they appear as sync `fn` in Rust),
/// so we fix them up here.
fn fixup_async_stubs(path: &std::path::Path) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut output = String::with_capacity(content.len());

    for line in content.lines() {
        let trimmed = line.trim_start();

        if let Some(after_def) = trimmed.strip_prefix("def ") {
            // Extract method name: "foo(" -> "foo"
            let method_name = after_def.split('(').next().unwrap_or("");

            let is_sync = SYNC_METHODS.contains(&method_name) || method_name.starts_with("__");

            if !is_sync {
                // Replace "def " with "async def " preserving indentation
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
    }

    std::fs::write(path, output)?;
    Ok(())
}

fn main() -> Result<()> {
    let stub = _crawlee_storage::stub_info()?;
    stub.generate()?;

    // Post-process: add `async` to methods that return coroutines
    let manifest_dir: &std::path::Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let stub_path: PathBuf = manifest_dir
        .join("python")
        .join("crawlee_storage")
        .join("_native")
        .join("__init__.pyi");

    if stub_path.exists() {
        fixup_async_stubs(&stub_path).expect("Failed to post-process stubs");
        eprintln!(
            "Post-processed stubs: added async markers to {}",
            stub_path.display()
        );
    }

    Ok(())
}
