//! A planned AICB schedule as H1's typed workload IR (design note §3.2; ruling R12).
//!
//! Every rank of a pipeline stage runs the same chain, so the IR is built per group instance:
//!
//! - each fused segment (ruling A1) becomes one compute operation per instance of the group family
//!   of the cross-host point that ends it, with the segment's delays plus the per-rank delay of
//!   each single-server collective inside it (H2's `single_server_collective_delay_ns`);
//! - each cross-host model-stream collective becomes one collective operation per group instance,
//!   after that instance's compute;
//! - each weight-gradient collective becomes one data-stream (stream 1) collective per instance,
//!   after the compute that ends at its fork and after the previous data collective in R9's order;
//! - every collective carries its record column's position in the realized start order as its
//!   `issue_ordinal` (ruling A7/C2), so SimAI's ECMP ports follow issue order.
//!
//! H1's `after` relation pairs stages of groups with equal host lists, so a chain may pass only
//! between operations on the same group family. A family change (all-to-all to a DP ring, or a
//! pipeline transfer) needs host-matched `after` (ruling C1) and is refused until it lands.

use std::collections::BTreeMap;

use crate::scenario::collective_shapes::{RoutingSkew, SeededAllToAll, simai_ring_channels};
use crate::scenario::workload::{
    Algorithm as IrAlgorithm, Collective, Operation, OperationKind, Transport, Workload,
};
use crate::topos::rail::ServerLocality;

use super::{
    AicbError, Algorithm, ChainItem, CollectiveOp, ExpertRouting, GroupKind, Groups, Plan,
    SegmentEnd, StartItem,
};

/// The group family a chain point belongs to (H1's `after` needs equal host lists).
type Family = GroupKind;

/// Builds the workload IR of a planned schedule.
pub fn lower_plan(
    plan: &Plan,
    groups: &Groups,
    routing: ExpertRouting,
    locality: &ServerLocality,
    mtu_bytes: u64,
    transport: Transport,
) -> Result<Workload, AicbError> {
    if plan.stages.len() != 1 {
        return Err(AicbError::new(
            "pipeline transfers between stages need host-matched `after` (ruling C1, pending)",
        ));
    }
    let stage = &plan.stages[0];
    let issue_ordinal: BTreeMap<usize, u64> = plan
        .start_order
        .iter()
        .enumerate()
        .filter_map(|(position, item)| match item {
            StartItem::Collective(op) => Some((*op, position as u64)),
            StartItem::Pipeline(_) => None,
        })
        .collect();
    let mut builder = Builder {
        workload: Workload {
            groups: Vec::new(),
            transports: vec![transport],
            operations: Vec::new(),
        },
        group_index: BTreeMap::new(),
        groups,
    };
    // The stream-0 point the next segment follows: its family and its operations by instance.
    let mut previous: Option<(Family, Vec<usize>)> = None;
    // Forks in chain order: the data op and the compute operations that end at it.
    let mut forks: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut segments = stage.segments.iter();
    for item in &stage.items {
        let (op, fork) = match *item {
            ChainItem::Collective(op) if !plan.ops[op].single_server() => (op, false),
            ChainItem::Fork(op) => (op, true),
            ChainItem::Receive(_) | ChainItem::Send(_) => {
                return Err(AicbError::new(
                    "pipeline transfers need host-matched `after` (ruling C1, pending)",
                ));
            }
            _ => continue,
        };
        let segment = segments
            .next()
            .ok_or_else(|| AicbError::new("a cross-host point has no preceding delay"))?;
        let expected = if fork {
            SegmentEnd::Fork(op)
        } else {
            SegmentEnd::Collective(op)
        };
        if segment.end != expected {
            return Err(AicbError::new(format!(
                "the chain's segment ends at {:?}, its cross-host point is {expected:?}",
                segment.end
            )));
        }
        let family = plan.ops[op].group;
        let mut duration = segment.delay_ns;
        for &single in &segment.single_server_ops {
            let collective = &plan.ops[single];
            let delay = locality
                .single_server_collective_delay_ns(
                    u64::from(collective.steps),
                    collective.message_bytes,
                    u64::from(collective.channels),
                    mtu_bytes,
                )
                .ok_or_else(|| AicbError::new("a single-server collective's delay overflows"))?;
            duration = duration
                .checked_add(delay)
                .ok_or_else(|| AicbError::new("a fused segment's delay overflows"))?;
        }
        let instances = groups.family(family).len();
        let predecessors = match &previous {
            None => None,
            Some((previous_family, operations)) if *previous_family == family => {
                Some(operations.clone())
            }
            Some((previous_family, _)) => {
                return Err(AicbError::new(format!(
                    "the chain passes from {previous_family:?} groups to {family:?} groups, which \
                     needs host-matched `after` (ruling C1, pending)"
                )));
            }
        };
        let computes = (0..instances)
            .map(|instance| {
                let after = predecessors
                    .as_ref()
                    .map_or_else(Vec::new, |operations| vec![operations[instance]]);
                builder.push(
                    family,
                    instance,
                    after,
                    0,
                    OperationKind::Compute {
                        duration_ns: duration,
                    },
                )
            })
            .collect::<Vec<_>>();
        if fork {
            forks.insert(op, computes.clone());
            previous = Some((family, computes));
        } else {
            let collective = &plan.ops[op];
            let ordinal = issue_ordinal[&op];
            let operations = (0..instances)
                .map(|instance| {
                    let kind = builder.collective(collective, instance, ordinal, routing)?;
                    Ok(builder.push(family, instance, vec![computes[instance]], 0, kind))
                })
                .collect::<Result<Vec<_>, AicbError>>()?;
            previous = Some((family, operations));
        }
    }
    // The data queue, in R9's start order: each data collective after its fork's compute and the
    // previous data collective.
    let mut previous_data: Option<(Family, Vec<usize>)> = None;
    for &op in &plan.data_queue.order {
        let collective = &plan.ops[op];
        let family = collective.group;
        let computes = forks
            .get(&op)
            .ok_or_else(|| AicbError::new("a data collective has no fork in the chain"))?;
        let chained = match &previous_data {
            None => None,
            Some((previous_family, operations)) if *previous_family == family => {
                Some(operations.clone())
            }
            Some((previous_family, _)) => {
                return Err(AicbError::new(format!(
                    "the data queue passes from {previous_family:?} groups to {family:?} groups, \
                     which needs host-matched `after` (ruling C1, pending)"
                )));
            }
        };
        let ordinal = issue_ordinal[&op];
        let operations = (0..groups.family(family).len())
            .map(|instance| {
                let mut after = vec![computes[instance]];
                if let Some(chained) = &chained {
                    after.push(chained[instance]);
                }
                let kind = builder.collective(collective, instance, ordinal, routing)?;
                Ok(builder.push(family, instance, after, 1, kind))
            })
            .collect::<Result<Vec<_>, AicbError>>()?;
        previous_data = Some((family, operations));
    }
    Ok(builder.workload)
}

struct Builder<'a> {
    workload: Workload,
    group_index: BTreeMap<(Family, usize), usize>,
    groups: &'a Groups,
}

impl Builder<'_> {
    fn push(
        &mut self,
        family: Family,
        instance: usize,
        after: Vec<usize>,
        stream: u32,
        kind: OperationKind,
    ) -> usize {
        let groups = self.groups;
        let workload = &mut self.workload;
        let group = *self
            .group_index
            .entry((family, instance))
            .or_insert_with(|| {
                workload.groups.push(
                    groups
                        .family(family)
                        .group(instance)
                        .iter()
                        .map(|&rank| u64::from(rank))
                        .collect(),
                );
                workload.groups.len() - 1
            });
        self.workload.operations.push(Operation {
            group,
            after,
            stream,
            kind,
        });
        self.workload.operations.len() - 1
    }

    fn collective(
        &self,
        collective: &CollectiveOp,
        instance: usize,
        issue_ordinal: u64,
        routing: ExpertRouting,
    ) -> Result<OperationKind, AicbError> {
        let ranks = self.groups.family(collective.group).group(instance);
        let hosts = ranks
            .iter()
            .map(|&rank| u64::from(rank))
            .collect::<Vec<_>>();
        let (algorithm, channels, seeded) = match collective.algorithm {
            Algorithm::AllToAll => {
                let seeded = match (routing, collective.matrix) {
                    (ExpertRouting::Imbalanced(params), Some(matrix)) => Some(SeededAllToAll {
                        seed: params.seed,
                        matrix: u64::from(matrix.matrix),
                        group: instance as u64,
                        transpose: matrix.transpose,
                        experts: u64::from(params.experts),
                        topk: u64::from(params.topk),
                        tokens: params.tokens_per_rank,
                        bytes_per_copy: matrix.bytes_per_copy,
                        skew: if params.zipf == 0 {
                            RoutingSkew::Uniform
                        } else {
                            RoutingSkew::Zipf1
                        },
                    }),
                    _ => None,
                };
                (IrAlgorithm::AllToAll, None, seeded)
            }
            ring => {
                let gpus_per_server = u64::from(self.groups.gpus_per_server);
                let channels = simai_ring_channels(&hosts, |rank| rank / gpus_per_server)
                    .ok_or_else(|| {
                        AicbError::new(format!(
                            "{:?} group {instance} is not SimAI-regular",
                            collective.group
                        ))
                    })?;
                let algorithm = match ring {
                    Algorithm::AllGather => IrAlgorithm::AllGather,
                    Algorithm::ReduceScatter => IrAlgorithm::ReduceScatter,
                    _ => IrAlgorithm::AllReduce,
                };
                (algorithm, Some(channels), None)
            }
        };
        Ok(OperationKind::Collective(Collective {
            algorithm,
            bytes: collective.total_bytes,
            transport: 0,
            channels,
            uniform_floor: seeded.is_none(),
            seeded,
            issue_ordinal: Some(issue_ordinal),
        }))
    }
}
