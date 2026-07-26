//! Dependency-baseline generation for the current workspace.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;

/// License expressions deliberately admitted by the dependency policy.
///
/// This list is reviewed in source. Baseline regeneration must never widen it
/// based on the packages that happen to resolve in the current checkout.
pub const CURATED_ALLOWED_LICENSES: &[&str] = &[
    "(MIT OR Apache-2.0) AND Unicode-3.0",
    "AGPL-3.0-only",
    "Apache-2.0",
    "Apache-2.0 OR BSL-1.0",
    "Apache-2.0 OR MIT",
    "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT",
    "Apache-2.0/MIT",
    "MIT",
    "MIT OR Apache-2.0",
    "MIT OR Apache-2.0 OR LGPL-2.1-or-later",
    "MIT/Apache-2.0",
    "Unlicense OR MIT",
    "Unlicense/MIT",
    "Zlib",
];

/// A dependency-baseline generation failure.
#[derive(Debug, Error)]
pub enum BaselineError {
    /// A manifest or output file could not be read or written.
    #[error("could not access {path}: {source}")]
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// A Cargo manifest was not valid TOML.
    #[error("could not parse {path}: {source}")]
    Toml {
        /// Manifest that could not be parsed.
        path: PathBuf,
        /// TOML parser failure.
        #[source]
        source: toml::de::Error,
    },
    /// A required manifest field was absent or malformed.
    #[error("invalid workspace manifest {path}: {message}")]
    Manifest {
        /// Manifest containing the invalid field.
        path: PathBuf,
        /// Explanation of the invalid field.
        message: String,
    },
    /// The generated baseline could not be serialized.
    #[error("could not serialize dependency baseline: {0}")]
    Serialize(#[from] toml::ser::Error),
}

#[derive(Debug, Serialize)]
struct GeneratedBaseline {
    schema_version: u32,
    allowed_licenses: Vec<String>,
    direct_dependencies: Vec<GeneratedDependency>,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Serialize)]
pub(crate) struct GeneratedDependency {
    pub(crate) package: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) source: String,
    pub(crate) kind: String,
}

/// Renders the dependency baseline for `repo_root`.
pub fn render(repo_root: &Path) -> Result<String, BaselineError> {
    let direct_dependencies = collect_direct_dependencies(repo_root)?;
    let baseline = GeneratedBaseline {
        schema_version: 1,
        allowed_licenses: CURATED_ALLOWED_LICENSES
            .iter()
            .map(|license| (*license).to_owned())
            .collect(),
        direct_dependencies,
    };
    let mut rendered = toml::to_string_pretty(&baseline)?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    Ok(rendered)
}

/// Writes a rendered baseline to its canonical repository path.
pub fn write(repo_root: &Path, rendered: &str) -> Result<(), BaselineError> {
    let path = repo_root.join("docs/days-executor/dependency-baseline.toml");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| BaselineError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    fs::write(&path, rendered).map_err(|source| BaselineError::Io { path, source })
}

pub(crate) fn collect_direct_dependencies(
    repo_root: &Path,
) -> Result<Vec<GeneratedDependency>, BaselineError> {
    let root_manifest = repo_root.join("Cargo.toml");
    let root_value = read_manifest(&root_manifest)?;
    let patched_sources = collect_patched_sources(&root_manifest, &root_value)?;
    let mut manifests = vec![(root_manifest.clone(), root_value.clone())];

    if let Some(members) = root_value
        .get("workspace")
        .and_then(|value| value.get("members"))
        .and_then(toml::Value::as_array)
    {
        for member in members {
            let member = member.as_str().ok_or_else(|| BaselineError::Manifest {
                path: root_manifest.clone(),
                message: "workspace.members entries must be strings".to_owned(),
            })?;
            if member.contains('*') || member.contains('?') || member.contains('[') {
                return Err(BaselineError::Manifest {
                    path: root_manifest.clone(),
                    message: format!(
                        "workspace member globs are not supported by the baseline generator: {member}"
                    ),
                });
            }
            let path = repo_root.join(member).join("Cargo.toml");
            let value = read_manifest(&path)?;
            manifests.push((path, value));
        }
    }

    let mut dependencies = BTreeSet::new();
    for (path, value) in manifests {
        let package = value
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
            .ok_or_else(|| BaselineError::Manifest {
                path: path.clone(),
                message: "package.name is required".to_owned(),
            })?;
        for (table_name, kind) in [
            ("dependencies", "normal"),
            ("dev-dependencies", "dev"),
            ("build-dependencies", "build"),
        ] {
            if let Some(table) = value.get(table_name).and_then(toml::Value::as_table) {
                for (name, declaration) in table {
                    let (version, mut source) = dependency_coordinates(&path, name, declaration)?;
                    if source == "crates.io" {
                        if let Some(patched_source) = patched_sources.get(name) {
                            source.clone_from(patched_source);
                        }
                    }
                    dependencies.insert(GeneratedDependency {
                        package: package.to_owned(),
                        name: name.to_owned(),
                        version,
                        source,
                        kind: kind.to_owned(),
                    });
                }
            }
        }
    }
    Ok(dependencies.into_iter().collect())
}

fn collect_patched_sources(
    manifest_path: &Path,
    manifest: &toml::Value,
) -> Result<BTreeMap<String, String>, BaselineError> {
    let mut sources = BTreeMap::new();
    let Some(registries) = manifest.get("patch").and_then(toml::Value::as_table) else {
        return Ok(sources);
    };
    for patches in registries.values().filter_map(toml::Value::as_table) {
        for (name, declaration) in patches {
            let (_, source) = dependency_coordinates(manifest_path, name, declaration)?;
            sources.insert(name.to_owned(), source);
        }
    }
    Ok(sources)
}

fn dependency_coordinates(
    manifest_path: &Path,
    name: &str,
    declaration: &toml::Value,
) -> Result<(String, String), BaselineError> {
    if let Some(version) = declaration.as_str() {
        return Ok((version.to_owned(), "crates.io".to_owned()));
    }

    let table = declaration
        .as_table()
        .ok_or_else(|| BaselineError::Manifest {
            path: manifest_path.to_path_buf(),
            message: format!("dependency {name} must be a version string or table"),
        })?;
    let version = table
        .get("version")
        .and_then(toml::Value::as_str)
        .unwrap_or("*")
        .to_owned();

    let source = if let Some(path) = table.get("path").and_then(toml::Value::as_str) {
        format!("path:{path}")
    } else if let Some(git) = table.get("git").and_then(toml::Value::as_str) {
        let mut source = format!("git:{git}");
        for selector in ["rev", "tag", "branch"] {
            if let Some(value) = table.get(selector).and_then(toml::Value::as_str) {
                source.push('#');
                source.push_str(selector);
                source.push('=');
                source.push_str(value);
            }
        }
        source
    } else if let Some(registry) = table.get("registry").and_then(toml::Value::as_str) {
        format!("registry:{registry}")
    } else if table
        .get("workspace")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false)
    {
        "workspace".to_owned()
    } else {
        "crates.io".to_owned()
    };

    Ok((version, source))
}

fn read_manifest(path: &Path) -> Result<toml::Value, BaselineError> {
    let contents = fs::read_to_string(path).map_err(|source| BaselineError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&contents).map_err(|source| BaselineError::Toml {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_uses_curated_licenses_and_records_dependency_coordinates() {
        let temp = tempfile::tempdir().expect("temp directory");
        fs::write(
            temp.path().join("Cargo.toml"),
            r#"
[workspace]
members = ["member"]

[package]
name = "root"
version = "0.1.0"
edition = "2021"

[dependencies]
registry_dep = "1.2"
path_dep = { path = "vendor/path-dep", version = "2" }
git_dep = { git = "https://example.invalid/repo", rev = "abc123" }

[patch.crates-io]
registry_dep = { path = "vendor/registry-dep" }
"#,
        )
        .expect("root manifest");
        fs::create_dir(temp.path().join("member")).expect("member directory");
        fs::write(
            temp.path().join("member/Cargo.toml"),
            r#"
[package]
name = "member"
version = "0.1.0"
edition = "2021"

[dev-dependencies]
dev_dep = { version = "3", registry = "private" }
"#,
        )
        .expect("member manifest");

        let rendered = render(temp.path()).expect("render baseline");
        let value: toml::Value = toml::from_str(&rendered).expect("parse baseline");
        let licenses: Vec<&str> = value["allowed_licenses"]
            .as_array()
            .expect("license array")
            .iter()
            .map(|value| value.as_str().expect("license string"))
            .collect();
        assert_eq!(licenses, CURATED_ALLOWED_LICENSES);

        let dependencies = value["direct_dependencies"]
            .as_array()
            .expect("dependency array");
        assert!(dependencies.iter().any(|dependency| {
            dependency["name"].as_str() == Some("registry_dep")
                && dependency["version"].as_str() == Some("1.2")
                && dependency["source"].as_str() == Some("path:vendor/registry-dep")
        }));
        assert!(dependencies.iter().any(|dependency| {
            dependency["name"].as_str() == Some("path_dep")
                && dependency["version"].as_str() == Some("2")
                && dependency["source"].as_str() == Some("path:vendor/path-dep")
        }));
        assert!(dependencies.iter().any(|dependency| {
            dependency["name"].as_str() == Some("git_dep")
                && dependency["version"].as_str() == Some("*")
                && dependency["source"].as_str()
                    == Some("git:https://example.invalid/repo#rev=abc123")
        }));
        assert!(dependencies.iter().any(|dependency| {
            dependency["name"].as_str() == Some("dev_dep")
                && dependency["version"].as_str() == Some("3")
                && dependency["source"].as_str() == Some("registry:private")
                && dependency["kind"].as_str() == Some("dev")
        }));
    }
}
