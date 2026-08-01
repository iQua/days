use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{Expr, Lit, Meta, Token};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AllowedDevDependency {
    pub package: &'static str,
    pub dependency: &'static str,
    pub purpose: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AllowedFeatureGate {
    pub path: &'static str,
    pub predicate: &'static str,
    pub count: usize,
    pub purpose: &'static str,
}

#[derive(Deserialize)]
struct Metadata {
    workspace_members: Vec<String>,
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    manifest_path: String,
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    kind: Option<String>,
}

pub fn audit_boundary_metadata(
    metadata_json: &str,
    allow_list: &[AllowedDevDependency],
) -> Result<(), String> {
    let metadata: Metadata = serde_json::from_str(metadata_json)
        .map_err(|error| format!("failed to parse cargo metadata: {error}"))?;
    let workspace_members = metadata
        .workspace_members
        .into_iter()
        .collect::<BTreeSet<_>>();
    let packages = metadata
        .packages
        .into_iter()
        .filter(|package| workspace_members.contains(&package.id))
        .collect::<Vec<_>>();

    let legacy_packages = packages
        .iter()
        .filter(|package| package.name == "days-legacy")
        .count();
    if legacy_packages != 1 {
        return Err(format!(
            "boundary audit requires exactly one workspace package named `days-legacy`; found {legacy_packages}"
        ));
    }

    let mut allowed = BTreeMap::new();
    for entry in allow_list {
        if entry.purpose.trim().is_empty() {
            return Err(format!(
                "boundary allow-list entry {} -> {} has no documented purpose",
                entry.package, entry.dependency
            ));
        }
        let key = (entry.package, entry.dependency);
        if allowed.insert(key, entry.purpose).is_some() {
            return Err(format!(
                "duplicate boundary allow-list entry {} -> {}",
                entry.package, entry.dependency
            ));
        }
    }

    let mut seen_allowed = BTreeSet::new();
    let mut errors = Vec::new();
    for package in &packages {
        if package.name == "days-legacy" {
            continue;
        }
        for dependency in &package.dependencies {
            if dependency.name != "days-legacy" {
                continue;
            }

            let kind = dependency.kind.as_deref().unwrap_or("normal");
            let key = (package.name.as_str(), dependency.name.as_str());
            if kind == "dev" && allowed.contains_key(&key) {
                seen_allowed.insert((package.name.clone(), dependency.name.clone()));
                continue;
            }

            errors.push(format!(
                "{} ({}) -> days-legacy has forbidden {kind} dependency",
                package.name, package.manifest_path
            ));
        }
    }

    for &(package, dependency) in allowed.keys() {
        if !seen_allowed.contains(&(package.to_owned(), dependency.to_owned())) {
            errors.push(format!(
                "stale boundary allow-list entry {package} -> {dependency}: expected an exact dev-dependency"
            ));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

pub fn audit_semantic_feature_gates(
    executor_src: &Path,
    allow_list: &[AllowedFeatureGate],
) -> Result<(), String> {
    let observed = observed_feature_gates(executor_src)?;
    let mut expected = BTreeMap::new();
    for entry in allow_list {
        if entry.purpose.trim().is_empty() {
            return Err(format!(
                "semantic feature allow-list entry {} `{}` has no documented purpose",
                entry.path, entry.predicate
            ));
        }
        let key = (PathBuf::from(entry.path), entry.predicate.to_owned());
        if expected.insert(key.clone(), entry.count).is_some() {
            return Err(format!(
                "duplicate semantic feature allow-list entry {} `{}`",
                entry.path, entry.predicate
            ));
        }
    }

    if observed == expected {
        return Ok(());
    }

    let mut errors = Vec::new();
    for (key, observed_count) in &observed {
        match expected.get(key) {
            Some(expected_count) if expected_count == observed_count => {}
            Some(expected_count) => errors.push(format!(
                "{} `{}` count changed: expected {expected_count}, observed {observed_count}",
                key.0.display(),
                key.1
            )),
            None => errors.push(format!(
                "{} contains unapproved semantic feature gate `{}` ({observed_count} occurrence(s))",
                key.0.display(),
                key.1
            )),
        }
    }
    for (key, expected_count) in &expected {
        if !observed.contains_key(key) {
            errors.push(format!(
                "{} is missing approved backend gate `{}` ({expected_count} expected occurrence(s))",
                key.0.display(),
                key.1
            ));
        }
    }
    Err(errors.join("\n"))
}

pub fn observed_feature_gates(
    executor_src: &Path,
) -> Result<BTreeMap<(PathBuf, String), usize>, String> {
    if !executor_src.is_dir() {
        return Err(format!(
            "executor semantic source directory is missing: {}",
            executor_src.display()
        ));
    }

    let mut files = Vec::new();
    collect_rust_files(executor_src, &mut files)?;
    files.sort();

    let mut observed = BTreeMap::new();
    for file in files {
        let source = fs::read_to_string(&file)
            .map_err(|error| format!("failed to read {}: {error}", file.display()))?;
        let syntax = syn::parse_file(&source)
            .map_err(|error| format!("failed to parse {}: {error}", file.display()))?;
        let relative = file
            .strip_prefix(executor_src)
            .map_err(|error| format!("failed to relativize {}: {error}", file.display()))?
            .to_owned();
        let mut collector = FeatureGateCollector::default();
        collector.visit_file(&syntax);
        for predicate in collector.predicates {
            *observed.entry((relative.clone(), predicate)).or_insert(0) += 1;
        }
    }
    Ok(observed)
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("failed to read {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("failed to read entry in {}: {error}", directory.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
        if file_type.is_dir() {
            collect_rust_files(&path, files)?;
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            files.push(path);
        }
    }
    Ok(())
}

#[derive(Default)]
struct FeatureGateCollector {
    predicates: Vec<String>,
}

impl<'ast> Visit<'ast> for FeatureGateCollector {
    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        if attribute.path().is_ident("cfg") {
            if let Ok(predicate) = attribute.parse_args::<Meta>() {
                if contains_feature(&predicate) {
                    self.predicates.push(canonical_meta(&predicate));
                }
            }
        } else if attribute.path().is_ident("cfg_attr") {
            let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
            if let Ok(arguments) =
                parser.parse2(attribute.meta.require_list().unwrap().tokens.clone())
            {
                for argument in arguments.iter().filter(|meta| contains_feature(meta)) {
                    self.predicates.push(canonical_meta(argument));
                }
            }
        }
        syn::visit::visit_attribute(self, attribute);
    }

    fn visit_macro(&mut self, macro_: &'ast syn::Macro) {
        if macro_.path.is_ident("cfg") {
            if let Ok(predicate) = syn::parse2::<Meta>(macro_.tokens.clone()) {
                if contains_feature(&predicate) {
                    self.predicates.push(canonical_meta(&predicate));
                }
            }
        }
        syn::visit::visit_macro(self, macro_);
    }
}

fn contains_feature(meta: &Meta) -> bool {
    match meta {
        Meta::Path(_) => false,
        Meta::NameValue(value) => value.path.is_ident("feature"),
        Meta::List(list) => {
            let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
            parser
                .parse2(list.tokens.clone())
                .is_ok_and(|items| items.iter().any(contains_feature))
        }
    }
}

fn canonical_meta(meta: &Meta) -> String {
    match meta {
        Meta::Path(path) => canonical_path(path),
        Meta::NameValue(value) => {
            format!(
                "{} = {}",
                canonical_path(&value.path),
                canonical_expr(&value.value)
            )
        }
        Meta::List(list) => {
            let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
            let arguments = parser
                .parse2(list.tokens.clone())
                .expect("validated cfg predicate should contain meta arguments");
            let arguments = arguments
                .iter()
                .map(canonical_meta)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({arguments})", canonical_path(&list.path))
        }
    }
}

fn canonical_path(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

fn canonical_expr(expression: &Expr) -> String {
    match expression {
        Expr::Lit(literal) => match &literal.lit {
            Lit::Str(value) => format!("\"{}\"", value.value()),
            Lit::Bool(value) => value.value.to_string(),
            Lit::Int(value) => value.base10_digits().to_owned(),
            _ => panic!("unsupported cfg literal"),
        },
        _ => panic!("unsupported cfg expression"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALIDATION_ALLOW: &[AllowedDevDependency] = &[AllowedDevDependency {
        package: "days-validation",
        dependency: "days-legacy",
        purpose: "cross-engine trajectory validation",
    }];

    fn metadata(source_name: &str, kind: Option<&str>) -> String {
        let kind = kind
            .map(|kind| format!(r#""kind":"{kind}""#))
            .unwrap_or_else(|| r#""kind":null"#.to_owned());
        format!(
            r#"{{"workspace_members":["path+file:///repo#{source_name}@0.1.0","path+file:///repo/legacy#days-legacy@0.1.0"],"packages":[{{"id":"path+file:///repo#{source_name}@0.1.0","name":"{source_name}","manifest_path":"/repo/{source_name}/Cargo.toml","dependencies":[{{"name":"days-legacy",{kind},"path":"/repo/legacy"}}]}},{{"id":"path+file:///repo/legacy#days-legacy@0.1.0","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml","dependencies":[{{"name":"days","kind":null,"path":"/repo"}}]}}]}}"#
        )
    }

    #[test]
    fn boundary_allows_legacy_to_depend_on_shared() {
        let json = r#"{"workspace_members":["legacy","days"],"packages":[{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml","dependencies":[{"name":"days","kind":null,"path":"/repo"}]},{"id":"days","name":"days","manifest_path":"/repo/Cargo.toml","dependencies":[]}]}"#;
        assert!(audit_boundary_metadata(json, &[]).is_ok());
    }

    #[test]
    fn boundary_rejects_normal_and_build_edges_to_legacy() {
        for kind in [None, Some("build")] {
            let error = audit_boundary_metadata(&metadata("days-executor", kind), &[])
                .expect_err("production dependency must fail");
            assert!(error.contains("days-executor"));
        }
    }

    #[test]
    fn boundary_rejects_non_path_edges_to_legacy() {
        let json = r#"{"workspace_members":["legacy","days-executor"],"packages":[{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml","dependencies":[]},{"id":"days-executor","name":"days-executor","manifest_path":"/repo/executor/Cargo.toml","dependencies":[{"name":"days-legacy","kind":null,"path":null}]}]}"#;
        assert!(audit_boundary_metadata(json, &[]).is_err());
    }

    #[test]
    fn boundary_allows_only_the_enumerated_dev_edge() {
        assert!(
            audit_boundary_metadata(&metadata("days-validation", Some("dev")), VALIDATION_ALLOW)
                .is_ok()
        );
        assert!(audit_boundary_metadata(&metadata("days-executor", Some("dev")), &[]).is_err());
    }

    #[test]
    fn boundary_rejects_a_stale_allow_list_entry() {
        let json = r#"{"workspace_members":["legacy","validation"],"packages":[{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml","dependencies":[]},{"id":"validation","name":"days-validation","manifest_path":"/repo/validation/Cargo.toml","dependencies":[]}]}"#;
        assert!(audit_boundary_metadata(json, VALIDATION_ALLOW).is_err());
    }

    #[test]
    fn boundary_requires_exactly_one_legacy_package() {
        let json = r#"{"workspace_members":["days"],"packages":[{"id":"days","name":"days","manifest_path":"/repo/Cargo.toml","dependencies":[]}]}"#;
        assert!(audit_boundary_metadata(json, &[]).is_err());
    }

    #[test]
    fn semantic_gate_audit_rejects_protocol_features_and_wrong_paths() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            "#[cfg(feature = \"dcqcn\")]\npub fn semantic_branch() {}\n",
        )
        .unwrap();
        assert!(audit_semantic_feature_gates(temp.path(), &[]).is_err());

        let allow = [AllowedFeatureGate {
            path: "lib.rs",
            predicate: r#"feature = "cuda""#,
            count: 1,
            purpose: "CUDA toolchain availability",
        }];
        assert!(audit_semantic_feature_gates(temp.path(), &allow).is_err());
    }

    #[test]
    fn semantic_gate_audit_requires_exact_predicate_and_count() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("lib.rs"),
            "#[cfg(feature = \"cuda\")]\npub mod cuda;\n",
        )
        .unwrap();
        let allow = [AllowedFeatureGate {
            path: "lib.rs",
            predicate: r#"feature = "cuda""#,
            count: 1,
            purpose: "CUDA toolchain availability",
        }];
        assert!(audit_semantic_feature_gates(temp.path(), &allow).is_ok());

        let wrong_count = [AllowedFeatureGate {
            count: 2,
            ..allow[0]
        }];
        assert!(audit_semantic_feature_gates(temp.path(), &wrong_count).is_err());
    }

    #[test]
    fn semantic_gate_audit_parses_nested_multiline_cfg() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("lib.rs"),
            "#[cfg(all(\n    feature = \"metal-spike\",\n    target_vendor = \"apple\"\n))]\npub mod metal;\n",
        )
        .unwrap();
        let allow = [AllowedFeatureGate {
            path: "lib.rs",
            predicate: r#"all(feature = "metal-spike", target_vendor = "apple")"#,
            count: 1,
            purpose: "Metal toolchain availability on Apple targets",
        }];
        assert!(audit_semantic_feature_gates(temp.path(), &allow).is_ok());
    }

    #[test]
    fn semantic_gate_audit_rejects_feature_cfg_nested_in_cfg_attr() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            "#[cfg_attr(unix, cfg(feature = \"protocol\"))]\npub fn semantic_branch() {}\n",
        )
        .unwrap();
        assert!(audit_semantic_feature_gates(temp.path(), &[]).is_err());
    }

    #[test]
    fn semantic_gate_audit_rejects_cfg_macro_feature_checks() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            "pub fn semantic_branch() -> bool { cfg!(feature = \"protocol\") }\n",
        )
        .unwrap();
        assert!(audit_semantic_feature_gates(temp.path(), &[]).is_err());
    }

    #[test]
    fn semantic_gate_audit_requires_the_source_directory() {
        assert!(
            audit_semantic_feature_gates(Path::new("definitely-missing-executor-src"), &[])
                .is_err()
        );
    }
}
