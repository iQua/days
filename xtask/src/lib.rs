use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
    workspace_root: String,
    workspace_members: Vec<String>,
    packages: Vec<Package>,
    resolve: Option<Resolve>,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    manifest_path: String,
}

#[derive(Deserialize)]
struct Resolve {
    nodes: Vec<ResolveNode>,
}

#[derive(Deserialize)]
struct ResolveNode {
    id: String,
    deps: Vec<ResolvedDependency>,
}

#[derive(Deserialize)]
struct ResolvedDependency {
    pkg: String,
    dep_kinds: Vec<ResolvedDependencyKind>,
}

#[derive(Deserialize)]
struct ResolvedDependencyKind {
    kind: Option<String>,
}

pub fn audit_boundary_metadata(
    metadata_json: &str,
    allow_list: &[AllowedDevDependency],
) -> Result<(), String> {
    let metadata: Metadata = serde_json::from_str(metadata_json)
        .map_err(|error| format!("failed to parse cargo metadata: {error}"))?;
    let resolve = metadata.resolve.as_ref().ok_or_else(|| {
        "cargo metadata did not include a resolved dependency graph; do not use `--no-deps`"
            .to_owned()
    })?;
    let workspace_members = metadata
        .workspace_members
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let packages_by_id = metadata
        .packages
        .iter()
        .map(|package| (package.id.as_str(), package))
        .collect::<BTreeMap<_, _>>();
    let nodes_by_id = resolve
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<BTreeMap<_, _>>();

    for member_id in &workspace_members {
        if !packages_by_id.contains_key(member_id.as_str()) {
            return Err(format!(
                "workspace member {member_id} is missing from cargo metadata packages"
            ));
        }
        if !nodes_by_id.contains_key(member_id.as_str()) {
            return Err(format!(
                "workspace member {member_id} is missing from the resolved dependency graph"
            ));
        }
    }

    let legacy_manifest = Path::new(&metadata.workspace_root).join("legacy/Cargo.toml");
    let legacy_packages = metadata
        .packages
        .iter()
        .filter(|package| {
            workspace_members.contains(&package.id)
                && Path::new(&package.manifest_path) == legacy_manifest
        })
        .collect::<Vec<_>>();
    if legacy_packages.len() != 1 {
        return Err(format!(
            "boundary audit requires exactly one workspace package at {}; found {}",
            legacy_manifest.display(),
            legacy_packages.len()
        ));
    }
    let legacy = legacy_packages[0];
    if legacy.name != "days-legacy" {
        return Err(format!(
            "boundary package at {} must be named `days-legacy`; found `{}`",
            legacy_manifest.display(),
            legacy.name
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
        if entry.dependency != legacy.name {
            return Err(format!(
                "boundary allow-list entry {} -> {} does not name the real legacy package at {}",
                entry.package,
                entry.dependency,
                legacy_manifest.display()
            ));
        }
        let source_packages = metadata
            .packages
            .iter()
            .filter(|package| {
                workspace_members.contains(&package.id) && package.name == entry.package
            })
            .collect::<Vec<_>>();
        if source_packages.len() != 1 {
            return Err(format!(
                "boundary allow-list source `{}` must resolve to exactly one workspace package; found {}",
                entry.package,
                source_packages.len()
            ));
        }
        let key = (source_packages[0].id.clone(), legacy.id.clone());
        if allowed.insert(key, entry.purpose).is_some() {
            return Err(format!(
                "duplicate boundary allow-list entry {} -> {}",
                entry.package, entry.dependency
            ));
        }
    }

    let mut errors = Vec::new();
    for node in &resolve.nodes {
        for dependency in &node.deps {
            if dependency.dep_kinds.is_empty() {
                errors.push(format!(
                    "resolved dependency {} -> {} has no dependency kind",
                    package_label(&node.id, &packages_by_id),
                    package_label(&dependency.pkg, &packages_by_id)
                ));
            }
        }
    }

    let mut seen_allowed = BTreeSet::new();
    for member_id in &workspace_members {
        let node = nodes_by_id[member_id.as_str()];
        for dependency in &node.deps {
            if dependency.pkg != legacy.id
                || !dependency
                    .dep_kinds
                    .iter()
                    .any(|kind| kind.kind.as_deref() == Some("dev"))
            {
                continue;
            }

            let key = (member_id.clone(), legacy.id.clone());
            if allowed.contains_key(&key) {
                seen_allowed.insert(key);
            } else {
                errors.push(format!(
                    "{} -> {} has a forbidden resolved dev dependency",
                    package_label(member_id, &packages_by_id),
                    package_label(&legacy.id, &packages_by_id)
                ));
            }
        }
    }

    for (source_id, dependency_id) in allowed.keys() {
        if !seen_allowed.contains(&(source_id.clone(), dependency_id.clone())) {
            errors.push(format!(
                "stale boundary allow-list entry {} -> {}: expected an exact resolved dev-dependency",
                package_label(source_id, &packages_by_id),
                package_label(dependency_id, &packages_by_id)
            ));
        }
    }

    let production_graph = resolve
        .nodes
        .iter()
        .map(|node| {
            let mut dependencies = node
                .deps
                .iter()
                .filter(|dependency| {
                    dependency
                        .dep_kinds
                        .iter()
                        .any(|kind| kind.kind.as_deref() != Some("dev"))
                })
                .map(|dependency| dependency.pkg.clone())
                .collect::<Vec<_>>();
            dependencies.sort();
            dependencies.dedup();
            (node.id.clone(), dependencies)
        })
        .collect::<BTreeMap<_, _>>();
    for member_id in workspace_members
        .iter()
        .filter(|member_id| member_id.as_str() != legacy.id)
    {
        if let Some(path) = resolved_path(member_id, &legacy.id, &production_graph) {
            errors.push(format!(
                "resolved production/build path reaches the legacy boundary: {}",
                path.iter()
                    .map(|package_id| package_label(package_id, &packages_by_id))
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn resolved_path(
    source: &str,
    target: &str,
    graph: &BTreeMap<String, Vec<String>>,
) -> Option<Vec<String>> {
    let mut queue = VecDeque::from([vec![source.to_owned()]]);
    let mut visited = BTreeSet::from([source.to_owned()]);
    while let Some(path) = queue.pop_front() {
        let package_id = path.last().expect("resolved path is never empty");
        for dependency_id in graph.get(package_id).into_iter().flatten() {
            let mut dependency_path = path.clone();
            dependency_path.push(dependency_id.clone());
            if dependency_id == target {
                return Some(dependency_path);
            }
            if visited.insert(dependency_id.clone()) {
                queue.push_back(dependency_path);
            }
        }
    }
    None
}

fn package_label(package_id: &str, packages: &BTreeMap<&str, &Package>) -> String {
    packages.get(package_id).map_or_else(
        || package_id.to_owned(),
        |package| format!("{} ({})", package.name, package.manifest_path),
    )
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
        if macro_
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "cfg")
        {
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
    use std::process::Command;

    const VALIDATION_ALLOW: &[AllowedDevDependency] = &[AllowedDevDependency {
        package: "days-validation",
        dependency: "days-legacy",
        purpose: "cross-engine trajectory validation",
    }];

    fn metadata(source_name: &str, kind: Option<&str>) -> String {
        let kind = kind
            .map(|kind| format!(r#""{kind}""#))
            .unwrap_or_else(|| "null".to_owned());
        format!(
            r#"{{"workspace_root":"/repo","workspace_members":["source","legacy"],"packages":[{{"id":"source","name":"{source_name}","manifest_path":"/repo/{source_name}/Cargo.toml"}},{{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml"}}],"resolve":{{"nodes":[{{"id":"source","deps":[{{"pkg":"legacy","dep_kinds":[{{"kind":{kind}}}]}}]}},{{"id":"legacy","deps":[]}}]}}}}"#
        )
    }

    fn write_package(directory: &Path, manifest: &str) {
        fs::create_dir_all(directory.join("src")).unwrap();
        fs::write(directory.join("Cargo.toml"), manifest).unwrap();
        fs::write(directory.join("src/lib.rs"), "").unwrap();
    }

    fn scratch_metadata(workspace: &Path) -> String {
        let output = Command::new(env!("CARGO"))
            .args(["metadata", "--format-version", "1", "--all-features"])
            .current_dir(workspace)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cargo metadata failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    #[test]
    fn boundary_allows_legacy_to_depend_on_shared() {
        let json = r#"{"workspace_root":"/repo","workspace_members":["legacy","days"],"packages":[{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml"},{"id":"days","name":"days","manifest_path":"/repo/Cargo.toml"}],"resolve":{"nodes":[{"id":"legacy","deps":[{"pkg":"days","dep_kinds":[{"kind":null}]}]},{"id":"days","deps":[]}]}}"#;
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
    fn boundary_rejects_resolved_edge_regardless_of_dependency_alias() {
        let json = r#"{"workspace_root":"/repo","workspace_members":["legacy","days-executor"],"packages":[{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml"},{"id":"days-executor","name":"days-executor","manifest_path":"/repo/executor/Cargo.toml"}],"resolve":{"nodes":[{"id":"legacy","deps":[]},{"id":"days-executor","deps":[{"name":"not_the_package_name","pkg":"legacy","dep_kinds":[{"kind":null}]}]}]}}"#;
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
        let json = r#"{"workspace_root":"/repo","workspace_members":["legacy","validation"],"packages":[{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml"},{"id":"validation","name":"days-validation","manifest_path":"/repo/validation/Cargo.toml"}],"resolve":{"nodes":[{"id":"legacy","deps":[]},{"id":"validation","deps":[]}]}}"#;
        assert!(audit_boundary_metadata(json, VALIDATION_ALLOW).is_err());
    }

    #[test]
    fn boundary_rejects_transitive_normal_path_to_real_legacy() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("Cargo.toml"),
            "[workspace]\nmembers = [\"app\", \"legacy\"]\nexclude = [\"bridge\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        write_package(
            &temp.path().join("app"),
            "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nbridge = { path = \"../bridge\" }\n",
        );
        write_package(
            &temp.path().join("bridge"),
            "[package]\nname = \"bridge\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\ndays-legacy = { path = \"../legacy\" }\n",
        );
        write_package(
            &temp.path().join("legacy"),
            "[package]\nname = \"days-legacy\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );

        let json = scratch_metadata(temp.path());
        let error = audit_boundary_metadata(&json, &[]).expect_err("transitive path must fail");
        assert!(error.contains("resolved production/build path"));
        assert!(error.contains("app") && error.contains("bridge") && error.contains("days-legacy"));
    }

    #[test]
    fn boundary_allow_list_rejects_same_named_impostor() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("Cargo.toml"),
            "[workspace]\nmembers = [\"validation\", \"legacy\"]\nexclude = [\"impostor\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        write_package(
            &temp.path().join("validation"),
            "[package]\nname = \"days-validation\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dev-dependencies]\ndays-legacy = { path = \"../impostor\" }\n",
        );
        write_package(
            &temp.path().join("legacy"),
            "[package]\nname = \"days-legacy\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );
        write_package(
            &temp.path().join("impostor"),
            "[package]\nname = \"days-legacy\"\nversion = \"0.2.0\"\nedition = \"2021\"\n",
        );

        let json = scratch_metadata(temp.path());
        let error = audit_boundary_metadata(&json, VALIDATION_ALLOW)
            .expect_err("same-named impostor must not satisfy the resolved allow-list");
        assert!(error.contains("stale boundary allow-list entry"));
    }

    #[test]
    fn boundary_requires_exactly_one_legacy_package() {
        let json = r#"{"workspace_root":"/repo","workspace_members":["days"],"packages":[{"id":"days","name":"days","manifest_path":"/repo/Cargo.toml"}],"resolve":{"nodes":[{"id":"days","deps":[]}]}}"#;
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
        let error = audit_semantic_feature_gates(temp.path(), &[])
            .expect_err("bare cfg macro must be inventoried");
        assert!(error.contains(r#"feature = "protocol""#));
    }

    #[test]
    fn semantic_gate_audit_rejects_qualified_cfg_macro_feature_checks() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            "pub fn semantic_branch() -> bool { std::cfg!(feature = \"protocol\") }\n",
        )
        .unwrap();
        let error = audit_semantic_feature_gates(temp.path(), &[])
            .expect_err("qualified cfg macro must be inventoried");
        assert!(error.contains(r#"feature = "protocol""#));
    }

    #[test]
    fn semantic_gate_audit_requires_the_source_directory() {
        assert!(
            audit_semantic_feature_gates(Path::new("definitely-missing-executor-src"), &[])
                .is_err()
        );
    }
}
