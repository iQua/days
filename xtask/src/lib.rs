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
                "{}:{}: {}",
                relative.display(),
                error.line,
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
    errors: Vec<FeatureGateAuditError>,
}

struct FeatureGateAuditError {
    line: usize,
    message: String,
}

#[derive(Clone, Copy)]
enum CfgAttributeKind {
    Cfg,
    CfgAttr,
}

impl<'ast> Visit<'ast> for FeatureGateCollector {
    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        self.collect_cfg_attribute_meta(
            &attribute.meta,
            "cfg attribute",
            "cfg_attr attribute",
            attribute.span(),
        );
        syn::visit::visit_attribute(self, attribute);
    }

    fn visit_macro(&mut self, macro_: &'ast syn::Macro) {
        if is_standard_cfg_macro(&macro_.path) {
            match parse_single_meta(macro_.tokens.clone(), macro_.span()) {
                Ok(predicate) => self.collect_meta(&predicate, "cfg macro", macro_.span()),
                Err(error) => self.record_error("cfg macro", macro_.span(), error),
            }
        } else {
            self.collect_nested_cfg_forms(macro_.tokens.clone());
        }
        syn::visit::visit_macro(self, macro_);
    }
}

impl FeatureGateCollector {
    fn collect_cfg_attribute_meta(
        &mut self,
        meta: &Meta,
        cfg_form: &'static str,
        cfg_attr_form: &'static str,
        span: Span,
    ) {
        if path_is_ident(meta_path(meta), "cfg") {
            match meta
                .require_list()
                .and_then(|list| parse_single_meta(list.tokens.clone(), span))
            {
                Ok(predicate) => self.collect_meta(&predicate, cfg_form, span),
                Err(error) => self.record_error(cfg_form, span, error),
            }
        } else if path_is_ident(meta_path(meta), "cfg_attr") {
            match meta
                .require_list()
                .and_then(|list| parse_meta_list(list.tokens.clone()))
            {
                Ok(arguments) => self.collect_cfg_attr_arguments(&arguments, cfg_attr_form, span),
                Err(error) => self.record_error(cfg_attr_form, span, error),
            }
        }
    }

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
        self.errors.push(FeatureGateAuditError {
            line: span.start().line,
            message: format!("failed to parse recognized {form}: {error}"),
        });
    }

    fn record_uncaptured_feature_key(&mut self, span: Span) {
        self.errors.push(FeatureGateAuditError {
            line: span.start().line,
            message: "uncaptured feature predicate in cfg-shaped macro tokens".to_owned(),
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

    fn collect_nested_cfg_forms(&mut self, tokens: TokenStream) {
        self.collect_nested_cfg_forms_in_context(tokens, false);
    }

    fn collect_nested_cfg_forms_in_context(&mut self, tokens: TokenStream, inside_cfg_shape: bool) {
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

            if let Some((consumed, body, kind, span)) = cfg_attribute_at(&tokens, index) {
                let cfg_form = "cfg attribute nested in macro tokens";
                let cfg_attr_form = "cfg_attr attribute nested in macro tokens";
                match syn::parse2::<Meta>(body) {
                    Ok(meta) => {
                        self.collect_cfg_attribute_meta(&meta, cfg_form, cfg_attr_form, span);
                        self.collect_cfg_attribute_residuals(&meta);
                    }
                    Err(error) => {
                        let form = match kind {
                            CfgAttributeKind::Cfg => cfg_form,
                            CfgAttributeKind::CfgAttr => cfg_attr_form,
                        };
                        self.record_error(form, span, error);
                    }
                }
                index += consumed;
                continue;
            }

            if inside_cfg_shape {
                if let Some(span) = feature_predicate_key_at(&tokens, index) {
                    self.record_uncaptured_feature_key(span);
                    index += 1;
                    continue;
                }
            }

            if let TokenTree::Group(group) = &tokens[index] {
                self.collect_nested_cfg_forms_in_context(
                    group.stream(),
                    inside_cfg_shape || cfg_shaped_group_at(&tokens, index),
                );
            }
            index += 1;
        }
    }

    fn collect_cfg_attribute_residuals(&mut self, meta: &Meta) {
        let Meta::List(list) = meta else {
            return;
        };
        if !path_is_ident(&list.path, "cfg_attr") {
            return;
        }
        self.collect_cfg_attr_residual_tokens(list.tokens.clone());
    }

    fn collect_cfg_attr_residual_tokens(&mut self, tokens: TokenStream) {
        for argument in split_top_level_arguments(tokens).into_iter().skip(1) {
            match syn::parse2::<Meta>(argument.clone()) {
                Ok(nested) if path_is_ident(meta_path(&nested), "cfg") => {}
                Ok(Meta::List(nested)) if path_is_ident(&nested.path, "cfg_attr") => {
                    self.collect_cfg_attr_residual_tokens(nested.tokens);
                }
                Ok(nested) if path_is_ident(meta_path(&nested), "cfg_attr") => {}
                _ => self.collect_nested_cfg_forms_in_context(argument, true),
            }
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

fn split_top_level_arguments(tokens: TokenStream) -> Vec<TokenStream> {
    let mut arguments = Vec::new();
    let mut current = TokenStream::new();
    for token in tokens {
        if matches!(&token, TokenTree::Punct(punct) if punct.as_char() == ',') {
            if !current.is_empty() {
                arguments.push(current);
                current = TokenStream::new();
            }
        } else {
            current.extend([token]);
        }
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    arguments
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

fn cfg_attribute_at(
    tokens: &[TokenTree],
    index: usize,
) -> Option<(usize, TokenStream, CfgAttributeKind, Span)> {
    if !punct_at(tokens, index, '#') {
        return None;
    }

    let mut cursor = index + 1;
    if punct_at(tokens, cursor, '!') {
        cursor += 1;
    }
    let TokenTree::Group(attribute) = tokens.get(cursor)? else {
        return None;
    };
    if attribute.delimiter() != proc_macro2::Delimiter::Bracket {
        return None;
    }

    let body = attribute.stream();
    let body_tokens = body.clone().into_iter().collect::<Vec<_>>();
    let path = ident_at(&body_tokens, 0)?;
    if punct_at(&body_tokens, 1, ':') {
        return None;
    }
    let kind = if ident_is(path, "cfg") {
        CfgAttributeKind::Cfg
    } else if ident_is(path, "cfg_attr") {
        CfgAttributeKind::CfgAttr
    } else {
        return None;
    };

    Some((cursor + 1 - index, body, kind, path.span()))
}

fn cfg_shaped_group_at(tokens: &[TokenTree], index: usize) -> bool {
    let Some(TokenTree::Group(group)) = tokens.get(index) else {
        return false;
    };

    if index >= 2
        && punct_at(tokens, index - 1, '!')
        && ident_at(tokens, index - 2)
            .is_some_and(|ident| ident_is(ident, "cfg") || ident_is(ident, "cfg_attr"))
    {
        return true;
    }
    if index >= 1
        && ident_at(tokens, index - 1)
            .is_some_and(|ident| ident_is(ident, "cfg") || ident_is(ident, "cfg_attr"))
    {
        return true;
    }

    if group.delimiter() != proc_macro2::Delimiter::Bracket {
        return false;
    }
    group
        .stream()
        .into_iter()
        .take_while(|token| match token {
            TokenTree::Group(_) => false,
            TokenTree::Punct(punct) => punct.as_char() != '=',
            _ => true,
        })
        .any(|token| {
            matches!(token, TokenTree::Ident(ident) if ident_is(&ident, "cfg") || ident_is(&ident, "cfg_attr"))
        })
}

fn feature_predicate_key_at(tokens: &[TokenTree], index: usize) -> Option<Span> {
    ident_at(tokens, index)
        .filter(|ident| ident_is(ident, "feature") && punct_at(tokens, index + 1, '='))
        .map(syn::Ident::span)
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

/// Host tables a stage-path function may read only through the stage view's counted accessors.
///
/// `generators`, `stages` and `tcp_receivers` are the view's `ProbedTable`s, and
/// `causes`/`stage_causes` the event's `PendingCauses`. The names are the bindings the executor
/// uses for them.
pub const STAGE_PATH_TABLES: &[&str] = &[
    "generators",
    "stages",
    "tcp_receivers",
    "causes",
    "stage_causes",
];

/// Methods that begin a scan of a collection.
const SCAN_METHODS: &[&str] = &["iter", "iter_mut", "into_iter"];

/// Accessors that return a host's raw `HostState`, whose tables no probe counts.
const RAW_HOST_ACCESSORS: &[&str] = &["host_state", "host_state_mut"];
/// Fields holding raw host states: the image's `host_states`, the executor's `hosts` store, and
/// the Scalar store's `states` and `indices` tables.
const RAW_HOST_FIELDS: &[&str] = &["host_states", "hosts", "states", "indices"];
/// Types that hold a raw host state: the state itself, the executor's host store, and the store's
/// per-host entry and whole-image tables.
const RAW_HOST_TYPES: &[&str] = &["HostState", "HostStore", "HostEntry", "HostTables"];

/// Element types of the raw host tables and of the raw pending-cause list.
const RAW_TABLE_ELEMENTS: &[&str] = &["FlowGeneratorState", "TcpReceiverState"];
/// The record type of the raw host stage table, whose elements are `Option<CollectiveStage>`.
const RAW_STAGE_RECORD: &str = "CollectiveStage";
const RAW_CAUSE_ELEMENT: &str = "PendingCollectiveProgress";

/// P14 scan audit: the named stage-path functions of `scalar.rs` reach host tables only through
/// the counted stage view, and never scan a table directly.
///
/// The rule rejects, inside each listed function (signature and body):
/// - a scan of a stage-path table: `<table>.iter()`, `.iter_mut()`, `.into_iter()`, or a `for`
///   loop over `<table>`, `&<table>` or `&mut <table>`, where `<table>` is a field or binding
///   named in [`STAGE_PATH_TABLES`]. Keyed lookups through the stage index are the only per-event
///   way into these tables;
/// - raw host access that would bypass the view's counters: a call of `host_state` or
///   `host_state_mut`; the `host_states`, `hosts`, `states` or `indices` field (the image's host
///   states, the executor's host store, and the Scalar store's tables); or the `HostState`,
///   `HostStore`, `HostEntry` or `HostTables` type;
/// - raw table types that would let a table escape the view: a slice of `FlowGeneratorState` or
///   `TcpReceiverState`, or a `Vec` of `PendingCollectiveProgress`.
///
/// Every listed function must exist, so a rename cannot silently drop one from the audit. The
/// rule is syntactic; a scan written through an alias it cannot see is still counted by the
/// view's probe, which the scaling budget in `tests/scalar_stage_scaling.rs` gates.
///
/// Returns the violations in `source`, one line each.
pub fn stage_path_table_violations(
    source: &str,
    functions: &[&str],
) -> Result<Vec<String>, String> {
    let syntax = syn::parse_file(source).map_err(|error| format!("failed to parse: {error}"))?;
    let mut finder = StageFunctionFinder {
        functions,
        found: BTreeSet::new(),
        violations: Vec::new(),
    };
    finder.visit_file(&syntax);
    let mut violations = finder.violations;
    for function in functions {
        if !finder.found.contains(*function) {
            violations.push(format!(
                "stage-path function `{function}` is listed for the table-access audit but not found"
            ));
        }
    }
    Ok(violations)
}

struct StageFunctionFinder<'a> {
    functions: &'a [&'a str],
    found: BTreeSet<String>,
    violations: Vec<String>,
}

impl StageFunctionFinder<'_> {
    fn check(&mut self, name: &syn::Ident, signature: &syn::Signature, block: &syn::Block) {
        let name = name.to_string();
        if !self.functions.contains(&name.as_str()) {
            return;
        }
        self.found.insert(name.clone());
        let mut checker = StageBodyChecker {
            function: name,
            violations: &mut self.violations,
        };
        checker.visit_signature(signature);
        checker.visit_block(block);
    }
}

impl<'ast> Visit<'ast> for StageFunctionFinder<'_> {
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.check(&item.sig.ident, &item.sig, &item.block);
        syn::visit::visit_item_fn(self, item);
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.check(&item.sig.ident, &item.sig, &item.block);
        syn::visit::visit_impl_item_fn(self, item);
    }
}

struct StageBodyChecker<'a> {
    function: String,
    violations: &'a mut Vec<String>,
}

impl StageBodyChecker<'_> {
    fn violation(&mut self, span: Span, message: String) {
        self.violations.push(format!(
            "scalar.rs:{}: in stage-path function `{}`: {message}",
            span.start().line,
            self.function
        ));
    }
}

/// The stage-path table an expression names, looking through references and parentheses.
fn stage_path_table(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Reference(reference) => stage_path_table(&reference.expr),
        Expr::Paren(paren) => stage_path_table(&paren.expr),
        Expr::Field(field) => match &field.member {
            syn::Member::Named(ident) => Some(ident.to_string()),
            syn::Member::Unnamed(_) => None,
        },
        Expr::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string()),
        _ => None,
    }
    .filter(|name| STAGE_PATH_TABLES.contains(&name.as_str()))
}

/// Whether a type is, or is a path ending in, `name`.
/// Whether `ty` is `Option<T>` (through references and parentheses) with `T` named `name`.
fn type_is_option_of(ty: &syn::Type, name: &str) -> bool {
    match ty {
        syn::Type::Path(path) => path.path.segments.last().is_some_and(|segment| {
            segment.ident == "Option"
                && matches!(&segment.arguments, syn::PathArguments::AngleBracketed(arguments)
                if arguments.args.iter().any(|argument| {
                    matches!(argument, syn::GenericArgument::Type(inner) if type_names(inner, name))
                }))
        }),
        syn::Type::Reference(reference) => type_is_option_of(&reference.elem, name),
        syn::Type::Paren(paren) => type_is_option_of(&paren.elem, name),
        _ => false,
    }
}

fn type_names(ty: &syn::Type, name: &str) -> bool {
    match ty {
        syn::Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == name),
        syn::Type::Reference(reference) => type_names(&reference.elem, name),
        syn::Type::Paren(paren) => type_names(&paren.elem, name),
        _ => false,
    }
}

impl<'ast> Visit<'ast> for StageBodyChecker<'_> {
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let method = call.method.to_string();
        if SCAN_METHODS.contains(&method.as_str()) {
            if let Some(table) = stage_path_table(&call.receiver) {
                self.violation(
                    call.method.span(),
                    format!("scans `{table}` with `.{method}()`; use a keyed stage-index lookup"),
                );
            }
        }
        if RAW_HOST_ACCESSORS.contains(&method.as_str()) {
            self.violation(
                call.method.span(),
                format!("reaches the raw host state through `{method}`; use the stage view"),
            );
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_for_loop(&mut self, for_loop: &'ast syn::ExprForLoop) {
        if let Some(table) = stage_path_table(&for_loop.expr) {
            self.violation(
                for_loop.for_token.span,
                format!("scans `{table}` with a `for` loop; use a keyed stage-index lookup"),
            );
        }
        syn::visit::visit_expr_for_loop(self, for_loop);
    }

    fn visit_expr_field(&mut self, field: &'ast syn::ExprField) {
        if let syn::Member::Named(ident) = &field.member {
            if RAW_HOST_FIELDS.contains(&ident.to_string().as_str()) {
                self.violation(
                    ident.span(),
                    "reaches the raw host states; use the stage view".to_owned(),
                );
            }
        }
        syn::visit::visit_expr_field(self, field);
    }

    fn visit_type_slice(&mut self, slice: &'ast syn::TypeSlice) {
        if let Some(element) = RAW_TABLE_ELEMENTS
            .iter()
            .find(|element| type_names(&slice.elem, element))
        {
            self.violation(
                slice.bracket_token.span.join(),
                format!("takes a raw `[{element}]` table, which bypasses the view's counters"),
            );
        }
        if type_is_option_of(&slice.elem, RAW_STAGE_RECORD) {
            self.violation(
                slice.bracket_token.span.join(),
                format!(
                    "takes a raw `[Option<{RAW_STAGE_RECORD}>]` table, which bypasses the view's \
                     counters"
                ),
            );
        }
        syn::visit::visit_type_slice(self, slice);
    }

    fn visit_type_path(&mut self, path: &'ast syn::TypePath) {
        if let Some(last) = path.path.segments.last() {
            if let Some(raw) = RAW_HOST_TYPES.iter().find(|raw| last.ident == **raw) {
                self.violation(
                    last.ident.span(),
                    format!("names the raw `{raw}`; use the stage view"),
                );
            }
            if last.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(arguments) = &last.arguments {
                    let raw_causes = arguments.args.iter().any(|argument| {
                        matches!(argument, syn::GenericArgument::Type(ty) if type_names(ty, RAW_CAUSE_ELEMENT))
                    });
                    if raw_causes {
                        self.violation(
                            last.ident.span(),
                            format!("holds causes in a raw `Vec<{RAW_CAUSE_ELEMENT}>`; use `PendingCauses`"),
                        );
                    }
                }
            }
        }
        syn::visit::visit_type_path(self, path);
    }
}

/// Both halves of the `scalar.rs` table audit, as `cargo xtask audit` runs them: the stage-path
/// view rule over `stage_functions` ([`stage_path_table_violations`]) and the default-deny
/// host-table scan rule over every function ([`table_scan_violations`]).
pub fn scalar_table_violations(
    source: &str,
    stage_functions: &[&str],
    scanners: &[AllowedTableScanner],
) -> Result<Vec<String>, String> {
    let mut violations = stage_path_table_violations(source, stage_functions)?;
    let mut access_scopes = stage_functions.to_vec();
    access_scopes.push(VIEW_CONSTRUCTOR);
    violations.extend(table_access_violations(
        source,
        "scalar.rs",
        scanners,
        &access_scopes,
    )?);
    Ok(violations)
}

/// [`scalar_table_violations`] over the file at `scalar_rs`.
pub fn audit_scalar_table_access(
    scalar_rs: &Path,
    stage_functions: &[&str],
    scanners: &[AllowedTableScanner],
) -> Result<(), String> {
    let source = fs::read_to_string(scalar_rs)
        .map_err(|error| format!("failed to read {}: {error}", scalar_rs.display()))?;
    let violations = scalar_table_violations(&source, stage_functions, scanners)?;
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations.join("\n"))
    }
}

/// Host tables no function may scan unless it is allowlisted: a host's generator table, its stage
/// table and its TCP-receiver table, whether raw `Vec`s or the stage view's `ProbedTable`s.
pub const HOST_TABLES: &[&str] = &["generators", "stages", "tcp_receivers"];

/// The function of `scalar.rs` that builds the stage view by destructuring a `HostState`.
const VIEW_CONSTRUCTOR: &str = "host_parts_mut";

/// Slice and `Vec` methods that walk a table: iteration, and the read-only linear searches a raw
/// `Vec` offers without one (`contains`, `windows`, `chunks`, `to_vec`).
const TABLE_WALK_METHODS: &[&str] = &[
    "iter",
    "iter_mut",
    "into_iter",
    "contains",
    "windows",
    "chunks",
    "to_vec",
];

/// One function (or inline module) of a file that may scan a host table, and why.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AllowedTableScanner {
    /// The function's name, or the name of an inline module all of whose functions may scan.
    pub scope: &'static str,
    pub reason: &'static str,
}

/// P14 scan audit, default-deny half: no function of the file scans a host table
/// ([`HOST_TABLES`]), or reaches one at all, unless its scope is allowlisted.
///
/// - A *scan* is a call of a `TABLE_WALK_METHODS` method on, or a `for` loop over, a field or
///   binding named in [`HOST_TABLES`] (through `&`, `&mut` and parentheses). Only the allow-listed
///   scanners may scan.
/// - An *access* is a field expression `<expr>.generators` / `<expr>.tcp_receivers`, or a struct
///   pattern that binds one of those fields. Only the scanners and the access scopes may access a
///   host table (for `scalar.rs`, the stage-path functions, which reach it through the counted
///   view, and the view's constructor). This closes a helper that aliases a raw table under
///   another name before scanning it, which the scan rule alone cannot see.
///
/// Both are attributed to the innermost enclosing function and to every enclosing inline module;
/// closures belong to their function. Every allowlist entry must have a reason and must still
/// reach a host table, so the list cannot go stale.
pub fn audit_table_scans(file: &Path, allow_list: &[AllowedTableScanner]) -> Result<(), String> {
    let source = fs::read_to_string(file)
        .map_err(|error| format!("failed to read {}: {error}", file.display()))?;
    let label = file.file_name().map_or_else(
        || file.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    );
    let violations = table_access_violations(&source, &label, allow_list, &[])?;
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations.join("\n"))
    }
}

/// The violations of [`audit_table_scans`] in `source`, with no access scopes beyond the scanners.
pub fn table_scan_violations(
    source: &str,
    label: &str,
    allow_list: &[AllowedTableScanner],
) -> Result<Vec<String>, String> {
    table_access_violations(source, label, allow_list, &[])
}

/// The violations of the default-deny rule in `source`, one line each; `access_scopes` may reach
/// host tables but not scan them.
pub fn table_access_violations(
    source: &str,
    label: &str,
    allow_list: &[AllowedTableScanner],
    access_scopes: &[&str],
) -> Result<Vec<String>, String> {
    let syntax =
        syn::parse_file(source).map_err(|error| format!("failed to parse {label}: {error}"))?;
    let mut violations = Vec::new();
    for entry in allow_list {
        if entry.reason.trim().is_empty() {
            violations.push(format!(
                "{label}: table-scan allow-list entry `{}` has no documented reason",
                entry.scope
            ));
        }
    }
    let mut finder = TableScanFinder {
        label,
        allow_list,
        access_scopes,
        reported_fields: BTreeSet::new(),
        functions: Vec::new(),
        modules: Vec::new(),
        used: BTreeSet::new(),
        violations: Vec::new(),
    };
    finder.visit_file(&syntax);
    violations.extend(finder.violations);
    for entry in allow_list {
        if !finder.used.contains(entry.scope) {
            violations.push(format!(
                "{label}: `{}` is allow-listed to reach a host table but reaches none; remove the entry",
                entry.scope
            ));
        }
    }
    Ok(violations)
}

struct TableScanFinder<'a> {
    label: &'a str,
    allow_list: &'a [AllowedTableScanner],
    access_scopes: &'a [&'a str],
    /// Source positions of table fields already reported as scanned, so a scan is one violation.
    reported_fields: BTreeSet<(usize, usize)>,
    functions: Vec<String>,
    modules: Vec<String>,
    used: BTreeSet<String>,
    violations: Vec<String>,
}

impl TableScanFinder<'_> {
    fn function_label(&self) -> String {
        self.functions.last().map_or_else(
            || "(outside any function)".to_owned(),
            |name| format!("`{name}`"),
        )
    }

    fn access(&mut self, span: Span, table: &str, form: &str) {
        let position = (span.start().line, span.start().column);
        if self.reported_fields.contains(&position) {
            return;
        }
        let scopes = self
            .functions
            .last()
            .into_iter()
            .chain(self.modules.iter())
            .cloned()
            .collect::<Vec<_>>();
        let listed = scopes
            .iter()
            .filter(|scope| {
                self.allow_list
                    .iter()
                    .any(|entry| entry.scope == scope.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let allowed = !listed.is_empty()
            || scopes
                .iter()
                .any(|scope| self.access_scopes.contains(&scope.as_str()));
        self.used.extend(listed);
        if !allowed {
            let function = self.function_label();
            self.violations.push(format!(
                "{}:{}: {function} reaches host table `{table}` through {form}; outside the \
                 stage-path functions and the table-scan allow-list, host tables are reached \
                 only through the stage index",
                self.label,
                span.start().line
            ));
        }
    }

    /// Records the table field a reported scan read, so it is not reported again as an access.
    fn mark_reported(&mut self, expression: &Expr) {
        match expression {
            Expr::Reference(reference) => self.mark_reported(&reference.expr),
            Expr::Paren(paren) => self.mark_reported(&paren.expr),
            Expr::Field(field) => {
                if let syn::Member::Named(ident) = &field.member {
                    let start = ident.span().start();
                    self.reported_fields.insert((start.line, start.column));
                }
            }
            _ => {}
        }
    }

    fn scan(&mut self, span: Span, table: &str, form: String) {
        let scopes = self.functions.last().into_iter().chain(self.modules.iter());
        let allowed = scopes
            .filter(|scope| {
                self.allow_list
                    .iter()
                    .any(|entry| entry.scope == scope.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        if allowed.is_empty() {
            let function = self.function_label();
            self.violations.push(format!(
                "{}:{}: {function} scans host table `{table}` with {form}; host tables are \
                 read by key (the stage index) outside the table-scan allow-list",
                self.label,
                span.start().line
            ));
        } else {
            self.used.extend(allowed);
        }
    }
}

/// The host table an expression names, looking through references and parentheses.
fn host_table(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Reference(reference) => host_table(&reference.expr),
        Expr::Paren(paren) => host_table(&paren.expr),
        Expr::Field(field) => match &field.member {
            syn::Member::Named(ident) => Some(ident.to_string()),
            syn::Member::Unnamed(_) => None,
        },
        Expr::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string()),
        _ => None,
    }
    .filter(|name| HOST_TABLES.contains(&name.as_str()))
}

impl<'ast> Visit<'ast> for TableScanFinder<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        self.modules.push(item.ident.to_string());
        syn::visit::visit_item_mod(self, item);
        self.modules.pop();
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.functions.push(item.sig.ident.to_string());
        syn::visit::visit_item_fn(self, item);
        self.functions.pop();
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.functions.push(item.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, item);
        self.functions.pop();
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let method = call.method.to_string();
        if TABLE_WALK_METHODS.contains(&method.as_str()) {
            if let Some(table) = host_table(&call.receiver) {
                self.scan(call.method.span(), &table, format!("`.{method}()`"));
                self.mark_reported(&call.receiver);
            }
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_for_loop(&mut self, for_loop: &'ast syn::ExprForLoop) {
        if let Some(table) = host_table(&for_loop.expr) {
            self.scan(for_loop.for_token.span, &table, "a `for` loop".to_owned());
            self.mark_reported(&for_loop.expr);
        }
        syn::visit::visit_expr_for_loop(self, for_loop);
    }

    fn visit_expr_field(&mut self, field: &'ast syn::ExprField) {
        if let syn::Member::Named(ident) = &field.member {
            let name = ident.to_string();
            if HOST_TABLES.contains(&name.as_str()) {
                self.access(ident.span(), &name, "a field expression");
            }
        }
        syn::visit::visit_expr_field(self, field);
    }

    fn visit_pat_struct(&mut self, pattern: &'ast syn::PatStruct) {
        for field in &pattern.fields {
            if let syn::Member::Named(ident) = &field.member {
                let name = ident.to_string();
                if HOST_TABLES.contains(&name.as_str()) {
                    self.access(ident.span(), &name, "a struct pattern");
                }
            }
        }
        syn::visit::visit_pat_struct(self, pattern);
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

    fn assert_package_compiles_offline(package: &Path, features: &str) {
        let output = Command::new(env!("CARGO"))
            .args(["check", "--offline", "--features", features])
            .current_dir(package)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cargo check failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
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
            "#[cfg(all(\n    feature = \"metal\",\n    target_vendor = \"apple\"\n))]\npub mod metal;\n",
        )
        .unwrap();
        let allow = [AllowedFeatureGate {
            path: "lib.rs",
            predicate: r#"all(feature = "metal", target_vendor = "apple")"#,
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
    fn semantic_gate_audit_rejects_compiling_cfg_if_attribute_in_opaque_macro_body() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("src")).unwrap();
        fs::write(
            temp.path().join("Cargo.toml"),
            concat!(
                "[package]\n",
                "name = \"cfg-if-audit-probe\"\n",
                "version = \"0.1.0\"\n",
                "edition = \"2024\"\n",
                "\n",
                "[features]\n",
                "protocol = []\n",
                "\n",
                "[dependencies]\n",
                "cfg-if = \"1.0\"\n",
                "\n",
                "[workspace]\n",
            ),
        )
        .unwrap();
        fs::write(
            temp.path().join("src/lib.rs"),
            concat!(
                "cfg_if::cfg_if! {\n",
                "    if #[cfg(feature = \"protocol\")] {\n",
                "        pub fn semantic_branch() -> bool { true }\n",
                "    } else {\n",
                "        pub fn semantic_branch() -> bool { false }\n",
                "    }\n",
                "}\n",
            ),
        )
        .unwrap();

        assert_package_compiles_offline(temp.path(), "protocol");
        let error = audit_semantic_feature_gates(&temp.path().join("src"), &[])
            .expect_err("a compiling cfg_if feature attribute must be inventoried");
        assert!(error.contains(r#"feature = "protocol""#), "{error}");

        let allow = [AllowedFeatureGate {
            path: "lib.rs",
            predicate: r#"feature = "protocol""#,
            count: 1,
            purpose: "opaque cfg_if probe",
        }];
        assert!(audit_semantic_feature_gates(&temp.path().join("src"), &allow).is_ok());
    }

    #[test]
    fn semantic_gate_audit_rejects_cfg_attr_in_opaque_macro_body() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            concat!(
                "macro_rules! sink { ($($tokens:tt)*) => {}; }\n",
                "sink! {\n",
                "    #[r#cfg_attr(unix, r#cfg(r#feature = \"protocol\"))]\n",
                "    pub fn semantic_branch() {}\n",
                "}\n",
            ),
        )
        .unwrap();

        let error = audit_semantic_feature_gates(temp.path(), &[])
            .expect_err("a cfg_attr in opaque macro tokens must be inventoried");
        assert!(error.contains(r#"cfg(feature = "protocol")"#), "{error}");
        assert!(!error.contains("r#feature"), "{error}");

        fs::write(
            temp.path().join("model.rs"),
            concat!(
                "macro_rules! sink { ($($tokens:tt)*) => {}; }\n",
                "sink! { #[cfg_attr(unix, cfg(feature = \"protocol\";))] }\n",
            ),
        )
        .unwrap();
        let error = audit_semantic_feature_gates(temp.path(), &[])
            .expect_err("a malformed cfg_attr in opaque macro tokens must be fatal");
        assert!(error.contains("failed to parse recognized"), "{error}");
        assert!(error.contains("model.rs:2"), "{error}");
    }

    #[test]
    fn semantic_gate_audit_rejects_cfg_attribute_in_doubly_nested_macro_groups() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            concat!(
                "macro_rules! sink { ($($tokens:tt)*) => {}; }\n",
                "sink! {\n",
                "    ({ #[cfg(feature = \"protocol\")] pub fn semantic_branch() {} })\n",
                "}\n",
            ),
        )
        .unwrap();

        let error = audit_semantic_feature_gates(temp.path(), &[])
            .expect_err("a cfg attribute two token groups deep must be inventoried");
        assert!(error.contains(r#"feature = "protocol""#), "{error}");
    }

    #[test]
    fn semantic_gate_audit_fails_closed_on_uncaptured_cfg_shaped_feature_keys() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("model.rs"),
            concat!(
                "macro_rules! sink { ($($tokens:tt)*) => {}; }\n",
                "sink! { provider::cfg!(feature = \"protocol\") }\n",
            ),
        )
        .unwrap();

        let error = audit_semantic_feature_gates(temp.path(), &[])
            .expect_err("an uncaptured feature key in a cfg-shaped group must be fatal");
        assert!(error.contains("uncaptured feature predicate"), "{error}");
        assert!(error.contains("model.rs:2"), "{error}");
    }

    #[test]
    fn semantic_gate_audit_scans_residual_tokens_in_nested_cfg_attr() {
        let temp = tempfile::tempdir().unwrap();
        let mut bypasses = Vec::new();
        for (source, expected) in [
            (
                concat!(
                    "macro_rules! sink { ($($tokens:tt)*) => {}; }\n",
                    "sink! { #[cfg_attr(unix, marker(cfg!(feature = \"protocol\")))] }\n",
                ),
                r#"feature = "protocol""#,
            ),
            (
                concat!(
                    "macro_rules! sink { ($($tokens:tt)*) => {}; }\n",
                    "sink! { #[cfg_attr(unix, marker(feature = \"protocol\"))] }\n",
                ),
                "uncaptured feature predicate",
            ),
            (
                concat!(
                    "macro_rules! sink { ($($tokens:tt)*) => {}; }\n",
                    "sink! { #[cfg_attr(unix, cfg_attr(windows, marker(feature = \"protocol\")))] }\n",
                ),
                "uncaptured feature predicate",
            ),
        ] {
            fs::write(temp.path().join("model.rs"), source).unwrap();
            match audit_semantic_feature_gates(temp.path(), &[]) {
                Ok(()) => bypasses.push(expected),
                Err(error) => assert!(error.contains(expected), "{error}"),
            }
        }
        assert!(
            bypasses.is_empty(),
            "cfg_attr residual-token probes bypassed the audit: {bypasses:?}"
        );
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

    const STAGE_FUNCTIONS: &[&str] = &["record_inbound_progress", "host_tcp_data_arrival"];

    fn stage_violations(body: &str) -> Vec<String> {
        stage_path_table_violations(body, STAGE_FUNCTIONS).unwrap()
    }

    /// The indexed shape of the two functions probe A rewrote.
    const INDEXED: &str = r#"
fn record_inbound_progress(generators: &mut ProbedTable<'_, crate::FlowGeneratorState>, index: &mut HostStageIndex, inbound: FlowId, causes: &mut PendingCauses) {
    let (successors, releasable) = index.inbound_successors_of(inbound);
    for &position in successors {
        let generator = &mut generators[position];
        releasable.refresh(position, generator);
    }
}
impl TransitionState<'_> {
    fn host_tcp_data_arrival(&mut self, node: NodeDescriptor) {
        let (mut state, index) = self.host_parts_mut(node).unwrap();
        let receiver = &mut state.tcp_receivers[index.first_receiver(flow).unwrap()];
        let mut stage_causes = PendingCauses::default();
        record_inbound_progress(&mut state.generators, index, flow, &mut stage_causes);
    }
}
"#;

    #[test]
    fn stage_table_audit_accepts_keyed_access_through_the_view() {
        assert_eq!(stage_violations(INDEXED), Vec::<String>::new());
    }

    #[test]
    fn stage_table_audit_rejects_probe_a_scans() {
        // Probe A of the P14 scan review: the receiver found by a linear scan, and the inbound
        // walk over every generator, in each form such a scan can take.
        for (scan, expected) in [
            (
                "let position = state.tcp_receivers.iter().position(|receiver| receiver.flow == flow).unwrap();",
                "scans `tcp_receivers` with `.iter()`",
            ),
            (
                "let receiver = state.tcp_receivers.iter_mut().find(|receiver| receiver.flow == flow).unwrap();",
                "scans `tcp_receivers` with `.iter_mut()`",
            ),
            (
                "for (position, generator) in generators.iter_mut().enumerate() {}",
                "scans `generators` with `.iter_mut()`",
            ),
            (
                "for generator in generators {}",
                "scans `generators` with a `for` loop",
            ),
            (
                "for generator in &mut state.generators {}",
                "scans `generators` with a `for` loop",
            ),
            (
                "let first = stage_causes.iter().position(|cause| cause.flow == flow);",
                "scans `stage_causes` with `.iter()`",
            ),
            // P14 slim round 2: the host's stage table is a stage-path table too.
            (
                "let first = state.stages.iter().position(Option::is_some);",
                "scans `stages` with `.iter()`",
            ),
            (
                "for stage in &mut state.stages {}",
                "scans `stages` with a `for` loop",
            ),
        ] {
            let source = INDEXED.replace(
                "let mut stage_causes = PendingCauses::default();",
                &format!("let mut stage_causes = PendingCauses::default(); {scan}"),
            );
            let violations = stage_violations(&source);
            assert_eq!(violations.len(), 1, "{scan}: {violations:?}");
            assert!(violations[0].contains(expected), "{scan}: {violations:?}");
            assert!(
                violations[0].contains("`host_tcp_data_arrival`"),
                "{violations:?}"
            );
        }
    }

    #[test]
    fn stage_table_audit_rejects_raw_host_access_and_raw_tables() {
        for (raw, expected) in [
            (
                "let state = self.host_state_mut(node).unwrap();",
                "through `host_state_mut`",
            ),
            (
                "let flows = self.host_state(node).unwrap().generators.len();",
                "through `host_state`",
            ),
            (
                "let states = &self.host_states;",
                "reaches the raw host states",
            ),
            ("let hosts = &self.hosts;", "reaches the raw host states"),
            (
                "let flows = self.hosts[0].state.generators.len();",
                "reaches the raw host states",
            ),
            (
                "let host: &mut HostEntry = todo!();",
                "names the raw `HostEntry`",
            ),
            (
                "let store: &HostStore = todo!();",
                "names the raw `HostStore`",
            ),
            (
                "let tables: &HostTables = todo!();",
                "names the raw `HostTables`",
            ),
            (
                "for state in &tables.states {}",
                "reaches the raw host states",
            ),
            (
                "let slot = &tables.indices[0];",
                "reaches the raw host states",
            ),
            (
                "let state: &HostState = todo!();",
                "names the raw `HostState`",
            ),
            (
                "let table: &mut [crate::FlowGeneratorState] = todo!();",
                "raw `[FlowGeneratorState]`",
            ),
            (
                "let table: &[TcpReceiverState] = todo!();",
                "raw `[TcpReceiverState]`",
            ),
            (
                "let table: &mut [Option<crate::CollectiveStage>] = todo!();",
                "raw `[Option<CollectiveStage>]`",
            ),
            (
                "let causes: Vec<PendingCollectiveProgress> = Vec::new();",
                "raw `Vec<PendingCollectiveProgress>`",
            ),
        ] {
            let source = INDEXED.replace(
                "let mut stage_causes = PendingCauses::default();",
                &format!("let mut stage_causes = PendingCauses::default(); {raw}"),
            );
            let violations = stage_violations(&source);
            assert_eq!(violations.len(), 1, "{raw}: {violations:?}");
            assert!(violations[0].contains(expected), "{raw}: {violations:?}");
        }
        // Probe A's original signature: the inbound walk over a raw generator slice.
        let raw_signature = INDEXED.replace(
            "generators: &mut ProbedTable<'_, crate::FlowGeneratorState>",
            "generators: &mut [crate::FlowGeneratorState]",
        );
        let violations = stage_violations(&raw_signature);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(
            violations[0].contains("`record_inbound_progress`: takes a raw `[FlowGeneratorState]`")
        );
    }

    #[test]
    fn stage_table_audit_ignores_unlisted_functions_and_requires_listed_ones() {
        let source = format!(
            "{INDEXED}\nfn host_retransmission_timeout(state: &mut HostState) {{ for generator in &mut state.generators {{}} }}"
        );
        assert_eq!(stage_violations(&source), Vec::<String>::new());
        let violations =
            stage_path_table_violations(INDEXED, &["record_inbound_progress", "renamed"]).unwrap();
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("`renamed` is listed"));
    }

    const SCANNERS: &[AllowedTableScanner] = &[AllowedTableScanner {
        scope: "host_retransmission_timeout",
        reason: "per timeout, keyed by timer identity",
    }];

    /// The indexed data-arrival shape, plus an allow-listed non-stage scanner.
    const CLEAN_SCALAR: &str = r#"
fn record_inbound_progress(generators: &mut ProbedTable<'_, crate::FlowGeneratorState>, index: &mut HostStageIndex, inbound: FlowId, causes: &mut PendingCauses) {
    let (successors, releasable) = index.inbound_successors_of(inbound);
    for &position in successors {
        let generator = &mut generators[position];
        releasable.refresh(position, generator);
    }
}
impl TransitionState<'_> {
    fn host_tcp_data_arrival(&mut self, node: NodeDescriptor) {
        let (mut state, index) = self.host_parts_mut(node).unwrap();
        let receiver = &mut state.tcp_receivers[index.first_receiver(flow).unwrap()];
    }
    fn host_retransmission_timeout(&mut self, node: NodeDescriptor) {
        let state = self.host_state_mut(node).unwrap();
        for generator in &mut state.generators {}
    }
}
"#;

    fn scalar_violations(source: &str) -> Vec<String> {
        scalar_table_violations(source, STAGE_FUNCTIONS, SCANNERS).unwrap()
    }

    #[test]
    fn scalar_table_audit_accepts_the_indexed_path_and_allow_listed_scanners() {
        assert_eq!(scalar_violations(CLEAN_SCALAR), Vec::<String>::new());
    }

    #[test]
    fn scalar_table_audit_rejects_probe_r2_helper() {
        // Review probe R2 (fix round 1): the receiver scan moved into a helper outside the
        // stage-path functions, over the raw host state, called from the data-arrival handler.
        let source = CLEAN_SCALAR
            .replace(
                "let receiver = &mut state.tcp_receivers[index.first_receiver(flow).unwrap()];",
                "let receiver = &mut state.tcp_receivers[scanned.unwrap()];",
            )
            .replace(
                "        let (mut state, index) = self.host_parts_mut(node).unwrap();",
                "        let scanned = self.receiver_position(node, flow).unwrap();\n        let (mut state, index) = self.host_parts_mut(node).unwrap();",
            )
            .replace(
                "    fn host_retransmission_timeout(",
                "    fn receiver_position(&self, node: NodeDescriptor, flow: FlowId) -> Result<Option<usize>, ExecutionError> {\n        Ok(self.host_state(node)?.tcp_receivers.iter().position(|receiver| receiver.flow == flow))\n    }\n    fn host_retransmission_timeout(",
            );
        let violations = scalar_violations(&source);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(
            violations[0]
                .contains("`receiver_position` scans host table `tcp_receivers` with `.iter()`"),
            "{violations:?}"
        );
    }

    #[test]
    fn scalar_table_audit_rejects_an_unlisted_handler_that_scans() {
        // A new stage-path handler, not added to the stage-function list, scanning through the
        // stage view and through the raw state, and walking a table without iterating.
        for (body, form) in [
            (
                "let (mut state, index) = self.host_parts_mut(node).unwrap(); for generator in &mut state.generators {}",
                "a `for` loop",
            ),
            (
                "let (mut state, index) = self.host_parts_mut(node).unwrap(); let found = state.generators.iter_mut().find(|generator| generator.flow == flow);",
                "`.iter_mut()`",
            ),
            (
                "let state = self.host_state(node).unwrap(); let known = state.tcp_receivers.contains(&receiver);",
                "`.contains()`",
            ),
            (
                "let table = &self.host_state(node).unwrap().generators; let found = table.iter().position(|generator| generator.flow == flow);",
                "a field expression",
            ),
            (
                "let (mut state, index) = self.host_parts_mut(node).unwrap(); let staged = state.stages.iter().flatten().count();",
                "`.iter()`",
            ),
        ] {
            let source = CLEAN_SCALAR.replace(
                "    fn host_retransmission_timeout(",
                &format!("    fn host_tcp_resend_timer(&mut self, node: NodeDescriptor) {{ {body} }}\n    fn host_retransmission_timeout("),
            );
            let violations = scalar_violations(&source);
            assert_eq!(violations.len(), 1, "{body}: {violations:?}");
            assert!(
                violations[0].contains("`host_tcp_resend_timer` ")
                    && violations[0].contains("host table")
                    && violations[0].contains(form),
                "{body}: {violations:?}"
            );
        }
    }

    #[test]
    fn table_scan_audit_attributes_closures_and_allows_whole_modules() {
        let source = r#"
fn outer(state: &HostState) { let find = |flow| state.generators.iter().position(|g| g.flow == flow); }
mod legacy_scans { fn first(state: &HostState) -> Option<usize> { state.generators.iter().position(|g| g.flow == flow) } }
"#;
        let modules = [AllowedTableScanner {
            scope: "legacy_scans",
            reason: "test-only oracle",
        }];
        let violations = table_scan_violations(source, "x.rs", &modules).unwrap();
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(
            violations[0].contains("x.rs:2: `outer` scans host table `generators`"),
            "{violations:?}"
        );
    }

    #[test]
    fn table_scan_audit_rejects_stale_and_undocumented_entries() {
        let entries = [
            AllowedTableScanner {
                scope: "host_retransmission_timeout",
                reason: " ",
            },
            AllowedTableScanner {
                scope: "no_longer_scans",
                reason: "was a scanner",
            },
        ];
        let violations = scalar_table_violations(CLEAN_SCALAR, STAGE_FUNCTIONS, &entries).unwrap();
        assert_eq!(violations.len(), 2, "{violations:?}");
        assert!(violations[0].contains("`host_retransmission_timeout` has no documented reason"));
        assert!(
            violations[1].contains(
                "`no_longer_scans` is allow-listed to reach a host table but reaches none"
            )
        );
    }

    #[test]
    fn scalar_table_audit_rejects_a_table_destructured_out_of_the_raw_state() {
        // Only the view's constructor may take a host table out of a `HostState` by pattern.
        let helper = "    fn receiver_position(&self, node: NodeDescriptor, flow: FlowId) -> Option<usize> {\n        let HostState { tcp_receivers: receivers, .. } = self.host_state(node).unwrap();\n        receivers.iter().position(|receiver| receiver.flow == flow)\n    }\n    fn host_retransmission_timeout(";
        let source = CLEAN_SCALAR.replace("    fn host_retransmission_timeout(", helper);
        let violations = scalar_violations(&source);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(
            violations[0].contains(
                "`receiver_position` reaches host table `tcp_receivers` through a struct pattern"
            ),
            "{violations:?}"
        );
        let constructor = "    fn host_parts_mut(&mut self, node: NodeDescriptor) {\n        let HostState { generators, tcp_receivers, .. } = self.raw(node);\n    }\n    fn host_retransmission_timeout(";
        let source = CLEAN_SCALAR.replace("    fn host_retransmission_timeout(", constructor);
        assert_eq!(scalar_violations(&source), Vec::<String>::new());
    }
}
