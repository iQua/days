use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::{Span, TokenStream, TokenTree};
use serde::Deserialize;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
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

    let legacy_directory = Path::new(&metadata.workspace_root).join("legacy");
    let legacy_manifest = legacy_directory.join("Cargo.toml");
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

    let legacy_owned_packages = metadata
        .packages
        .iter()
        .filter(|package| Path::new(&package.manifest_path).starts_with(&legacy_directory))
        .collect::<Vec<_>>();
    let legacy_package_ids = legacy_owned_packages
        .iter()
        .map(|package| package.id.clone())
        .collect::<BTreeSet<_>>();
    let workspace_legacy_packages = legacy_owned_packages
        .iter()
        .copied()
        .filter(|package| workspace_members.contains(&package.id))
        .collect::<Vec<_>>();
    if workspace_legacy_packages.len() != 1 || workspace_legacy_packages[0].id != legacy.id {
        let found = workspace_legacy_packages
            .iter()
            .map(|package| format!("{} ({})", package.name, package.manifest_path))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "legacy directory ownership requires workspace members under {} to be exactly days-legacy ({}); found [{}]",
            legacy_directory.display(),
            legacy_manifest.display(),
            found
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
        let dependency_packages = if entry.dependency == legacy.name {
            vec![legacy]
        } else {
            metadata
                .packages
                .iter()
                .filter(|package| package.name == entry.dependency)
                .collect::<Vec<_>>()
        };
        if dependency_packages.len() != 1 {
            return Err(format!(
                "boundary allow-list target `{}` must resolve to exactly one package; found {}",
                entry.dependency,
                dependency_packages.len()
            ));
        }
        let key = (
            source_packages[0].id.clone(),
            dependency_packages[0].id.clone(),
            Some("dev".to_owned()),
        );
        if allowed.insert(key, entry.purpose).is_some() {
            return Err(format!(
                "duplicate boundary allow-list entry {} -[dev]-> {}",
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

    let full_graph = resolve
        .nodes
        .iter()
        .map(|node| {
            let mut dependencies = node
                .deps
                .iter()
                .map(|dependency| dependency.pkg.clone())
                .collect::<Vec<_>>();
            dependencies.sort();
            dependencies.dedup();
            (node.id.clone(), dependencies)
        })
        .collect::<BTreeMap<_, _>>();

    let mut seen_allowed = BTreeSet::new();
    for member_id in workspace_members
        .iter()
        .filter(|member_id| !legacy_package_ids.contains(member_id.as_str()))
    {
        let node = nodes_by_id[member_id.as_str()];
        for dependency in &node.deps {
            let suffix = resolved_path_to_any(&dependency.pkg, &legacy_package_ids, &full_graph);
            let Some(suffix) = suffix else {
                continue;
            };
            let path = std::iter::once(member_id.clone())
                .chain(suffix)
                .map(|package_id| package_label(&package_id, &packages_by_id))
                .collect::<Vec<_>>()
                .join(" -> ");

            for kind in &dependency.dep_kinds {
                let key = (member_id.clone(), dependency.pkg.clone(), kind.kind.clone());
                let entry = format!(
                    "{} -[{}]-> {}",
                    package_label(member_id, &packages_by_id),
                    dependency_kind_label(kind),
                    package_label(&dependency.pkg, &packages_by_id)
                );
                if kind.kind.as_deref() == Some("dev") {
                    if allowed.contains_key(&key) {
                        seen_allowed.insert(key);
                    } else {
                        errors.push(format!(
                            "forbidden resolved dev entry edge reaches the legacy boundary: {entry}; path: {path}"
                        ));
                    }
                } else {
                    errors.push(format!(
                        "resolved production/build path reaches the legacy boundary via entry edge {entry}: {path}"
                    ));
                }
            }
        }
    }

    for (source_id, dependency_id, kind) in allowed.keys() {
        let key = (source_id.clone(), dependency_id.clone(), kind.clone());
        if !seen_allowed.contains(&key) {
            errors.push(format!(
                "stale boundary allow-list entry {} -[{}]-> {}: expected an exact resolved entry edge whose full path reaches a legacy-owned package",
                package_label(source_id, &packages_by_id),
                kind.as_deref().unwrap_or("normal"),
                package_label(dependency_id, &packages_by_id)
            ));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn dependency_kind_label(kind: &ResolvedDependencyKind) -> &str {
    kind.kind.as_deref().unwrap_or("normal")
}

fn resolved_path_to_any(
    source: &str,
    targets: &BTreeSet<String>,
    graph: &BTreeMap<String, Vec<String>>,
) -> Option<Vec<String>> {
    if targets.contains(source) {
        return Some(vec![source.to_owned()]);
    }

    let mut queue = VecDeque::from([vec![source.to_owned()]]);
    let mut visited = BTreeSet::from([source.to_owned()]);
    while let Some(path) = queue.pop_front() {
        let package_id = path.last().expect("resolved path is never empty");
        for dependency_id in graph.get(package_id).into_iter().flatten() {
            let mut dependency_path = path.clone();
            dependency_path.push(dependency_id.clone());
            if targets.contains(dependency_id) {
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
    let mut parse_errors = Vec::new();
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
        for error in collector.errors {
            parse_errors.push(format!(
                "{}:{}: failed to parse recognized {}: {}",
                relative.display(),
                error.line,
                error.form,
                error.message
            ));
        }
    }
    if !parse_errors.is_empty() {
        return Err(parse_errors.join("\n"));
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
    errors: Vec<FeatureGateParseError>,
}

struct FeatureGateParseError {
    line: usize,
    form: &'static str,
    message: String,
}

impl<'ast> Visit<'ast> for FeatureGateCollector {
    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        if path_is_ident(attribute.path(), "cfg") {
            match attribute
                .meta
                .require_list()
                .and_then(|list| parse_single_meta(list.tokens.clone(), attribute.span()))
            {
                Ok(predicate) => self.collect_meta(&predicate, "cfg attribute", attribute.span()),
                Err(error) => self.record_error("cfg attribute", attribute.span(), error),
            }
        } else if path_is_ident(attribute.path(), "cfg_attr") {
            match attribute
                .meta
                .require_list()
                .and_then(|list| parse_meta_list(list.tokens.clone()))
            {
                Ok(arguments) => self.collect_cfg_attr_arguments(
                    &arguments,
                    "cfg_attr attribute",
                    attribute.span(),
                ),
                Err(error) => self.record_error("cfg_attr attribute", attribute.span(), error),
            }
        }
        syn::visit::visit_attribute(self, attribute);
    }

    fn visit_macro(&mut self, macro_: &'ast syn::Macro) {
        if is_standard_cfg_macro(&macro_.path) {
            match parse_single_meta(macro_.tokens.clone(), macro_.span()) {
                Ok(predicate) => self.collect_meta(&predicate, "cfg macro", macro_.span()),
                Err(error) => self.record_error("cfg macro", macro_.span(), error),
            }
        } else {
            self.collect_nested_cfg_macros(macro_.tokens.clone());
        }
        syn::visit::visit_macro(self, macro_);
    }
}

impl FeatureGateCollector {
    fn collect_meta(&mut self, meta: &Meta, form: &'static str, span: Span) {
        match contains_feature(meta) {
            Ok(true) => match canonical_meta(meta) {
                Ok(predicate) => self.predicates.push(predicate),
                Err(error) => self.record_error(form, span, error),
            },
            Ok(false) => {}
            Err(error) => self.record_error(form, span, error),
        }
    }

    fn record_error(&mut self, form: &'static str, span: Span, error: syn::Error) {
        self.errors.push(FeatureGateParseError {
            line: span.start().line,
            form,
            message: error.to_string(),
        });
    }

    fn collect_cfg_attr_arguments(
        &mut self,
        arguments: &Punctuated<Meta, Token![,]>,
        form: &'static str,
        span: Span,
    ) {
        let mut arguments = arguments.iter();
        let Some(predicate) = arguments.next() else {
            self.record_error(
                form,
                span,
                syn::Error::new(span, "expected a cfg_attr predicate"),
            );
            return;
        };
        self.collect_meta(predicate, form, span);

        for attribute in arguments {
            if path_is_ident(meta_path(attribute), "cfg") {
                if matches!(attribute, Meta::List(_)) {
                    self.collect_meta(attribute, "cfg attribute nested in cfg_attr", span);
                } else {
                    self.record_error(
                        "cfg attribute nested in cfg_attr",
                        span,
                        syn::Error::new_spanned(attribute, "expected cfg predicate arguments"),
                    );
                }
            } else if path_is_ident(meta_path(attribute), "cfg_attr") {
                match attribute {
                    Meta::List(list) => match parse_meta_list(list.tokens.clone()) {
                        Ok(nested) => self.collect_cfg_attr_arguments(
                            &nested,
                            "cfg_attr attribute nested in cfg_attr",
                            span,
                        ),
                        Err(error) => {
                            self.record_error("cfg_attr attribute nested in cfg_attr", span, error)
                        }
                    },
                    _ => self.record_error(
                        "cfg_attr attribute nested in cfg_attr",
                        span,
                        syn::Error::new_spanned(attribute, "expected cfg_attr arguments"),
                    ),
                }
            }
        }
    }

    fn collect_nested_cfg_macros(&mut self, tokens: TokenStream) {
        let tokens = tokens.into_iter().collect::<Vec<_>>();
        let mut index = 0;
        while index < tokens.len() {
            if let Some((consumed, body, span)) = cfg_macro_at(&tokens, index) {
                match parse_single_meta(body, span) {
                    Ok(predicate) => {
                        self.collect_meta(&predicate, "cfg macro nested in macro tokens", span)
                    }
                    Err(error) => {
                        self.record_error("cfg macro nested in macro tokens", span, error)
                    }
                }
                index += consumed;
                continue;
            }

            if let TokenTree::Group(group) = &tokens[index] {
                self.collect_nested_cfg_macros(group.stream());
            }
            index += 1;
        }
    }
}

fn meta_path(meta: &Meta) -> &syn::Path {
    match meta {
        Meta::Path(path) => path,
        Meta::List(list) => &list.path,
        Meta::NameValue(value) => &value.path,
    }
}

fn parse_meta_list(tokens: TokenStream) -> syn::Result<Punctuated<Meta, Token![,]>> {
    Punctuated::<Meta, Token![,]>::parse_terminated.parse2(tokens)
}

fn parse_single_meta(tokens: TokenStream, span: Span) -> syn::Result<Meta> {
    let items = parse_meta_list(tokens)?;
    if items.len() != 1 {
        return Err(syn::Error::new(
            span,
            format!("expected exactly one cfg predicate, found {}", items.len()),
        ));
    }
    Ok(items.into_iter().next().expect("one cfg predicate exists"))
}

fn cfg_macro_at(tokens: &[TokenTree], index: usize) -> Option<(usize, TokenStream, Span)> {
    let mut cursor = index;
    let has_leading_colon = punct_at(tokens, cursor, ':') && punct_at(tokens, cursor + 1, ':');
    if has_leading_colon {
        if index > 0 && matches!(&tokens[index - 1], TokenTree::Ident(_)) {
            return None;
        }
        cursor += 2;
    } else if index >= 2 && punct_at(tokens, index - 1, ':') && punct_at(tokens, index - 2, ':') {
        return None;
    }

    let first = ident_at(tokens, cursor)?;
    let span = first.span();
    cursor += 1;

    if ident_is(first, "cfg") {
        if has_leading_colon {
            return None;
        }
    } else if ident_is(first, "std") || ident_is(first, "core") {
        if !punct_at(tokens, cursor, ':') || !punct_at(tokens, cursor + 1, ':') {
            return None;
        }
        cursor += 2;
        if !ident_at(tokens, cursor).is_some_and(|ident| ident_is(ident, "cfg")) {
            return None;
        }
        cursor += 1;
    } else {
        return None;
    }

    if !punct_at(tokens, cursor, '!') {
        return None;
    }
    let TokenTree::Group(body) = tokens.get(cursor + 1)? else {
        return None;
    };
    Some((cursor + 2 - index, body.stream(), span))
}

fn ident_at(tokens: &[TokenTree], index: usize) -> Option<&syn::Ident> {
    match tokens.get(index)? {
        TokenTree::Ident(ident) => Some(ident),
        _ => None,
    }
}

fn punct_at(tokens: &[TokenTree], index: usize, expected: char) -> bool {
    matches!(tokens.get(index), Some(TokenTree::Punct(punct)) if punct.as_char() == expected)
}

fn is_standard_cfg_macro(path: &syn::Path) -> bool {
    if path.leading_colon.is_none() && path_is_ident(path, "cfg") {
        return true;
    }
    if path.segments.len() != 2 {
        return false;
    }

    let mut segments = path.segments.iter();
    let root = segments.next().expect("two-segment path has a root");
    let macro_name = segments.next().expect("two-segment path has a macro name");
    (ident_is(&root.ident, "std") || ident_is(&root.ident, "core"))
        && ident_is(&macro_name.ident, "cfg")
}

fn path_is_ident(path: &syn::Path, expected: &str) -> bool {
    path.leading_colon.is_none()
        && path.segments.len() == 1
        && ident_is(&path.segments[0].ident, expected)
}

fn ident_is(ident: &syn::Ident, expected: &str) -> bool {
    normalized_ident(ident) == expected
}

fn normalized_ident(ident: &syn::Ident) -> String {
    let ident = ident.to_string();
    ident.strip_prefix("r#").unwrap_or(&ident).to_owned()
}

fn contains_feature(meta: &Meta) -> syn::Result<bool> {
    match meta {
        Meta::Path(_) => Ok(false),
        Meta::NameValue(value) => {
            canonical_expr(&value.value)?;
            Ok(path_is_ident(&value.path, "feature"))
        }
        Meta::List(list) => {
            let items = parse_meta_list(list.tokens.clone())?;
            for item in &items {
                if contains_feature(item)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn canonical_meta(meta: &Meta) -> syn::Result<String> {
    Ok(match meta {
        Meta::Path(path) => canonical_path(path),
        Meta::NameValue(value) => format!(
            "{} = {}",
            canonical_path(&value.path),
            canonical_expr(&value.value)?
        ),
        Meta::List(list) => {
            let arguments = parse_meta_list(list.tokens.clone())?;
            let arguments = arguments
                .iter()
                .map(canonical_meta)
                .collect::<syn::Result<Vec<_>>>()?
                .join(", ");
            format!("{}({arguments})", canonical_path(&list.path))
        }
    })
}

fn canonical_path(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|segment| normalized_ident(&segment.ident))
        .collect::<Vec<_>>()
        .join("::")
}

fn canonical_expr(expression: &Expr) -> syn::Result<String> {
    match expression {
        Expr::Lit(literal) => match &literal.lit {
            Lit::Str(value) => Ok(format!("\"{}\"", value.value())),
            Lit::Bool(value) => Ok(value.value.to_string()),
            Lit::Int(value) => Ok(value.base10_digits().to_owned()),
            _ => Err(syn::Error::new_spanned(
                expression,
                "unsupported cfg predicate literal",
            )),
        },
        _ => Err(syn::Error::new_spanned(
            expression,
            "unsupported cfg predicate expression",
        )),
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
    fn boundary_rejects_dev_entry_to_normal_legacy_bridge() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("Cargo.toml"),
            "[workspace]\nmembers = [\"probe\", \"legacy\"]\nexclude = [\"bridge\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        write_package(
            &temp.path().join("probe"),
            "[package]\nname = \"days-executor-probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dev-dependencies]\nbridge = { path = \"../bridge\" }\n",
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
        let error = audit_boundary_metadata(&json, &[])
            .expect_err("dev entry to a normal legacy bridge must fail");
        assert!(error.contains("days-executor-probe"));
        assert!(error.contains("bridge"));
        assert!(error.contains("days-legacy"));

        let allow = [AllowedDevDependency {
            package: "days-executor-probe",
            dependency: "bridge",
            purpose: "test-only bridge entry",
        }];
        assert!(audit_boundary_metadata(&json, &allow).is_ok());
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
    fn boundary_rejects_shadow_workspace_package_under_legacy_directory() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("Cargo.toml"),
            "[workspace]\nmembers = [\"legacy\", \"legacy/shadow\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        write_package(
            &temp.path().join("legacy"),
            "[package]\nname = \"days-legacy\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );
        write_package(
            &temp.path().join("legacy/shadow"),
            "[package]\nname = \"shadow-legacy-engine\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );

        let json = scratch_metadata(temp.path());
        let error = audit_boundary_metadata(&json, &[])
            .expect_err("a second workspace package under legacy/ must fail");
        assert!(error.contains("shadow-legacy-engine"));
        assert!(error.contains("legacy/shadow/Cargo.toml"));
    }

    #[test]
    fn boundary_rejects_path_to_non_workspace_package_under_legacy_directory() {
        let json = r#"{"workspace_root":"/repo","workspace_members":["app","legacy"],"packages":[{"id":"app","name":"app","manifest_path":"/repo/app/Cargo.toml"},{"id":"legacy","name":"days-legacy","manifest_path":"/repo/legacy/Cargo.toml"},{"id":"shadow","name":"shadow-legacy-engine","manifest_path":"/repo/legacy/shadow/Cargo.toml"}],"resolve":{"nodes":[{"id":"app","deps":[{"pkg":"shadow","dep_kinds":[{"kind":null}]}]},{"id":"legacy","deps":[]},{"id":"shadow","deps":[]}]}}"#;
        let error = audit_boundary_metadata(json, &[])
            .expect_err("every package under legacy/ must be a reachability target");
        assert!(error.contains("resolved production/build path"), "{error}");
        assert!(error.contains("shadow-legacy-engine"), "{error}");
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
    fn semantic_gate_audit_allows_unrelated_cfg_attr_attribute_syntax() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            "#![cfg_attr(unix, doc = concat!(\"hello\", \" world\"))]\n",
        )
        .unwrap();

        assert!(audit_semantic_feature_gates(temp.path(), &[]).is_ok());
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
    fn semantic_gate_audit_matches_only_standard_cfg_macro_paths() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            concat!(
                "provider::cfg!(feature = \"protocol\");\n",
                "cfg!(feature = \"protocol\");\n",
                "std::cfg!(feature = \"protocol\");\n",
                "core::cfg!(feature = \"protocol\");\n",
                "::std::cfg!(feature = \"protocol\");\n",
                "::core::cfg!(feature = \"protocol\");\n",
            ),
        )
        .unwrap();
        let allow = [AllowedFeatureGate {
            path: "model.rs",
            predicate: r#"feature = "protocol""#,
            count: 5,
            purpose: "the five exact standard cfg macro paths",
        }];

        assert!(audit_semantic_feature_gates(temp.path(), &allow).is_ok());
    }

    #[test]
    fn semantic_gate_audit_finds_standard_cfg_macros_nested_in_macro_tokens() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            concat!(
                "pub fn probe() {\n",
                "    assert!(provider::cfg!(feature = \"protocol\"));\n",
                "    assert!(provider::std::cfg!(feature = \"protocol\"));\n",
                "    assert!(provider::core::cfg!(feature = \"protocol\"));\n",
                "    assert!(cfg!(feature = \"protocol\"));\n",
                "    assert!(std::cfg!(feature = \"protocol\"));\n",
                "    assert!(core::cfg!(feature = \"protocol\"));\n",
                "    assert!(::std::cfg!(feature = \"protocol\"));\n",
                "    assert!(::core::cfg!(feature = \"protocol\"));\n",
                "    assert!(r#cfg!(r#feature = \"protocol\"));\n",
                "}\n",
            ),
        )
        .unwrap();
        let allow = [AllowedFeatureGate {
            path: "model.rs",
            predicate: r#"feature = "protocol""#,
            count: 6,
            purpose: "standard cfg paths nested in macro token streams",
        }];

        assert!(audit_semantic_feature_gates(temp.path(), &allow).is_ok());
    }

    #[test]
    fn semantic_gate_audit_finds_nested_cfg_macros_after_field_colons() {
        let mut bypasses = Vec::new();
        for invocation in [
            r#"cfg!(feature = "protocol")"#,
            r#"std::cfg!(feature = "protocol")"#,
            r#"core::cfg!(feature = "protocol")"#,
            r#"::std::cfg!(feature = "protocol")"#,
            r#"::core::cfg!(feature = "protocol")"#,
            r#"r#cfg!(r#feature = "protocol")"#,
        ] {
            let temp = tempfile::tempdir().unwrap();
            fs::write(
                temp.path().join("model.rs"),
                format!(
                    "struct Probe {{ gate: bool }}\npub fn probe() {{ let _ = vec![Probe {{ gate: {invocation} }}]; }}\n"
                ),
            )
            .unwrap();
            let allow = [AllowedFeatureGate {
                path: "model.rs",
                predicate: r#"feature = "protocol""#,
                count: 1,
                purpose: "nested cfg after a struct field colon",
            }];

            if audit_semantic_feature_gates(temp.path(), &allow).is_err() {
                bypasses.push(invocation);
            }
        }
        assert!(
            bypasses.is_empty(),
            "nested cfg macro spellings after a field colon bypassed inventory: {bypasses:?}"
        );
    }

    #[test]
    fn semantic_gate_audit_rejects_trailing_comma_in_every_standard_cfg_macro_path() {
        let mut bypasses = Vec::new();
        for invocation in [
            r#"cfg!(feature = "probe",)"#,
            r#"std::cfg!(feature = "probe",)"#,
            r#"core::cfg!(feature = "probe",)"#,
            r#"::std::cfg!(feature = "probe",)"#,
            r#"::core::cfg!(feature = "probe",)"#,
        ] {
            let temp = tempfile::tempdir().unwrap();
            fs::write(
                temp.path().join("model.rs"),
                format!("pub fn probe() -> bool {{ {invocation} }}\n"),
            )
            .unwrap();

            match audit_semantic_feature_gates(temp.path(), &[]) {
                Ok(()) => bypasses.push(invocation),
                Err(error) => {
                    assert!(error.contains("unapproved semantic feature gate"));
                    assert!(error.contains(r#"feature = "probe""#));
                }
            }
        }
        assert!(
            bypasses.is_empty(),
            "standard cfg macro spellings bypassed the inventory: {bypasses:?}"
        );
    }

    #[test]
    fn semantic_gate_audit_rejects_trailing_comma_in_cfg_attribute() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            "#[cfg(feature = \"probe\",)]\npub fn probe() {}\n",
        )
        .unwrap();

        let error = audit_semantic_feature_gates(temp.path(), &[])
            .expect_err("a trailing comma in cfg must not bypass the inventory");
        assert!(error.contains("unapproved semantic feature gate"));
        assert!(error.contains(r#"feature = "probe""#));
    }

    #[test]
    fn semantic_gate_audit_rejects_raw_identifiers_in_standard_cfg_macro_paths() {
        let mut bypasses = Vec::new();
        for invocation in [
            r#"r#cfg!(feature = "probe")"#,
            r#"std::r#cfg!(feature = "probe")"#,
            r#"r#std::cfg!(feature = "probe")"#,
            r#"core::r#cfg!(feature = "probe")"#,
            r#"r#core::cfg!(feature = "probe")"#,
            r#"::std::r#cfg!(feature = "probe")"#,
            r#"::r#std::cfg!(feature = "probe")"#,
            r#"::core::r#cfg!(feature = "probe")"#,
            r#"::r#core::cfg!(feature = "probe")"#,
            r#"cfg!(r#feature = "probe")"#,
        ] {
            let temp = tempfile::tempdir().unwrap();
            fs::write(
                temp.path().join("model.rs"),
                format!("pub fn probe() -> bool {{ {invocation} }}\n"),
            )
            .unwrap();

            match audit_semantic_feature_gates(temp.path(), &[]) {
                Ok(()) => bypasses.push(invocation),
                Err(error) => {
                    assert!(error.contains("unapproved semantic feature gate"));
                    assert!(error.contains(r#"feature = "probe""#));
                    assert!(!error.contains("r#feature"));
                }
            }
        }
        assert!(
            bypasses.is_empty(),
            "raw cfg macro spellings bypassed the inventory: {bypasses:?}"
        );
    }

    #[test]
    fn semantic_gate_audit_rejects_raw_identifiers_in_cfg_attributes() {
        let mut bypasses = Vec::new();
        for attribute in [
            r#"#[r#cfg(feature = "probe")]"#,
            r#"#[cfg(r#feature = "probe")]"#,
            r#"#[r#cfg(r#feature = "probe")]"#,
            r#"#[r#cfg_attr(unix, cfg(feature = "probe"))]"#,
            r#"#[cfg_attr(unix, r#cfg(r#feature = "probe"))]"#,
        ] {
            let temp = tempfile::tempdir().unwrap();
            fs::write(
                temp.path().join("model.rs"),
                format!("{attribute}\npub fn probe() {{}}\n"),
            )
            .unwrap();

            match audit_semantic_feature_gates(temp.path(), &[]) {
                Ok(()) => bypasses.push(attribute),
                Err(error) => {
                    assert!(error.contains("unapproved semantic feature gate"));
                    assert!(error.contains(r#"feature = "probe""#));
                    assert!(!error.contains("r#feature"));
                }
            }
        }
        assert!(
            bypasses.is_empty(),
            "raw cfg attribute spellings bypassed the inventory: {bypasses:?}"
        );
    }

    #[test]
    fn semantic_gate_audit_canonicalizes_raw_feature_predicate_identifiers() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            "#[cfg(r#feature = \"probe\")]\npub fn probe() {}\n",
        )
        .unwrap();
        let allow = [AllowedFeatureGate {
            path: "model.rs",
            predicate: r#"feature = "probe""#,
            count: 1,
            purpose: "raw identifiers have the same cfg identity",
        }];

        assert!(audit_semantic_feature_gates(temp.path(), &allow).is_ok());
    }

    #[test]
    fn semantic_gate_audit_reports_recognized_cfg_parse_failures_with_location() {
        let mut bypasses = Vec::new();
        for (form, source) in [
            (
                "cfg!",
                "pub fn probe() -> bool { cfg!(feature = \"probe\";) }\n",
            ),
            (
                "std::cfg!",
                "pub fn probe() -> bool { std::cfg!(feature = \"probe\";) }\n",
            ),
            (
                "core::cfg!",
                "pub fn probe() -> bool { core::cfg!(feature = \"probe\";) }\n",
            ),
            (
                "::std::cfg!",
                "pub fn probe() -> bool { ::std::cfg!(feature = \"probe\";) }\n",
            ),
            (
                "::core::cfg!",
                "pub fn probe() -> bool { ::core::cfg!(feature = \"probe\";) }\n",
            ),
            (
                "#[cfg]",
                "#[cfg(feature = \"probe\";)]\npub fn probe() {}\n",
            ),
            (
                "#[cfg_attr]",
                "#[cfg_attr(unix, cfg(feature = \"probe\";))]\npub fn probe() {}\n",
            ),
        ] {
            let temp = tempfile::tempdir().unwrap();
            fs::write(temp.path().join("model.rs"), source).unwrap();

            match audit_semantic_feature_gates(temp.path(), &[]) {
                Ok(()) => bypasses.push(form),
                Err(error) => {
                    assert!(error.contains("failed to parse recognized"));
                    assert!(error.contains("model.rs:1"));
                }
            }
        }
        assert!(
            bypasses.is_empty(),
            "recognized cfg parse failures were ignored: {bypasses:?}"
        );
    }

    #[test]
    fn semantic_gate_audit_requires_the_source_directory() {
        assert!(
            audit_semantic_feature_gates(Path::new("definitely-missing-executor-src"), &[])
                .is_err()
        );
    }
}
