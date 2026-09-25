use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use days_executor::{LinkId, NodeId};

/// Role-tagged physical topology identity.
///
/// A host attachment and its adjacent switch may share a topology number, but they are distinct
/// physical entities.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum PhysicalNodeKey {
    Host(u64),
    Switch(u64),
}

/// Semantic identity of one directed physical link.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct LinkKey {
    pub source: PhysicalNodeKey,
    pub target: PhysicalNodeKey,
}

/// Semantic identity of one logical process.
///
/// Variant order is part of the canonical dense-ID order. A switch port is keyed by both the
/// physical switch and its directed egress link, never by construction or enumeration order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum LpKey {
    Host(u64),
    SwitchPort { switch: u64, egress: LinkKey },
}

impl LpKey {
    pub(crate) const fn for_link_source(link: LinkKey) -> Self {
        match link.source {
            PhysicalNodeKey::Host(host) => Self::Host(host),
            PhysicalNodeKey::Switch(switch) => Self::SwitchPort {
                switch,
                egress: link,
            },
        }
    }

    pub(crate) const fn for_link_target(link: LinkKey) -> Self {
        match link.target {
            PhysicalNodeKey::Host(host) => Self::Host(host),
            PhysicalNodeKey::Switch(switch) => Self::SwitchPort {
                switch,
                egress: LinkKey {
                    source: link.target,
                    target: link.source,
                },
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IdError;

impl fmt::Display for IdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("scenario contains more than u64::MAX semantic identities")
    }
}

impl Error for IdError {}

/// Assigns dense IDs after canonical sorting and duplicate removal.
///
/// Callers provide semantic keys rather than construction artifacts. `BTreeSet` makes assignment
/// independent of input order, allocation order, pointer identity, and hash iteration order.
pub(crate) fn dense_ids<K>(keys: impl IntoIterator<Item = K>) -> Result<BTreeMap<K, u64>, IdError>
where
    K: Ord,
{
    BTreeSet::from_iter(keys)
        .into_iter()
        .enumerate()
        .map(|(index, key)| {
            let id = u64::try_from(index).map_err(|_| IdError)?;
            Ok((key, id))
        })
        .collect()
}

pub(crate) struct StableIds {
    nodes: BTreeMap<LpKey, u64>,
    links: BTreeMap<LinkKey, u64>,
}

impl StableIds {
    pub(crate) fn new(
        nodes: impl IntoIterator<Item = LpKey>,
        links: impl IntoIterator<Item = LinkKey>,
    ) -> Result<Self, IdError> {
        Ok(Self {
            nodes: dense_ids(nodes)?,
            links: dense_ids(links)?,
        })
    }

    pub(crate) fn node(&self, key: LpKey) -> NodeId {
        NodeId(self.nodes[&key])
    }

    pub(crate) fn link(&self, key: LinkKey) -> LinkId {
        LinkId(self.links[&key])
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = (LpKey, NodeId)> + '_ {
        self.nodes.iter().map(|(&key, &id)| (key, NodeId(id)))
    }

    pub(crate) fn links(&self) -> impl Iterator<Item = (LinkKey, LinkId)> + '_ {
        self.links.iter().map(|(&key, &id)| (key, LinkId(id)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{LinkKey, LpKey, PhysicalNodeKey, StableIds};

    #[test]
    fn map_insertion_order_does_not_change_dense_ids() {
        let first_switch = PhysicalNodeKey::Switch(3);
        let second_switch = PhysicalNodeKey::Switch(8);
        let forward = LinkKey {
            source: first_switch,
            target: second_switch,
        };
        let reverse = LinkKey {
            source: second_switch,
            target: first_switch,
        };
        let host_lp = LpKey::Host(3);
        let first_port = LpKey::for_link_source(forward);
        let second_port = LpKey::for_link_source(reverse);

        let first_nodes = HashMap::from([(second_port, ()), (host_lp, ()), (first_port, ())]);
        let second_nodes = HashMap::from([(first_port, ()), (host_lp, ()), (second_port, ())]);
        let first_links = HashMap::from([(reverse, ()), (forward, ())]);
        let second_links = HashMap::from([(forward, ()), (reverse, ())]);

        let first = StableIds::new(first_nodes.into_keys(), first_links.into_keys()).unwrap();
        let second = StableIds::new(second_nodes.into_keys(), second_links.into_keys()).unwrap();

        assert_eq!(
            first.nodes().collect::<Vec<_>>(),
            second.nodes().collect::<Vec<_>>()
        );
        assert_eq!(
            first.links().collect::<Vec<_>>(),
            second.links().collect::<Vec<_>>()
        );
    }
}
