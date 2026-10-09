//! A planned AICB schedule as H1's typed workload IR (design note §3.2; ruling R12).
//!
//! Every rank of a pipeline stage runs the same chain, and `after` is host-matched (ruling C1:
//! rank `r` of an operation waits, at its host, for each listed operation that runs there), so:
//!
//! - each fused segment (ruling A1) of a stage becomes one compute operation over the stage's
//!   ranks, with the segment's delays plus the per-rank delay of each single-server collective
//!   inside it (H2's `single_server_collective_delay_ns`), after whatever ended the previous
//!   segment at those ranks;
//! - each cross-host model-stream collective becomes one collective operation per group instance
//!   of the stage, after the segment that ends at it; the next segment follows every instance;
//! - each pipeline transfer becomes one Send/Recv operation per stage pair `(r, r ± W/PP)`, after
//!   the sender's segment; the sender's next segment follows the transfers (Megatron's send
//!   waits for completion) and the receiver's next segment follows its own previous segment and
//!   the transfers;
//! - each weight-gradient collective becomes one data-stream (stream 1) collective per instance,
//!   after the segment that ends at its fork in the instance's stage and after the instances of
//!   the previous data collective in R9's order that share a host with it;
//! - every collective carries its record column's (or transfer's) position in the realized start
//!   order as its `issue_ordinal` (ruling A7/C2), so SimAI's ECMP ports follow issue order.

use std::collections::BTreeMap;

use crate::scenario::collective_shapes::{RoutingSkew, SeededAllToAll, simai_ring_channels};
use crate::scenario::workload::{
    Algorithm as IrAlgorithm, Collective, Operation, OperationKind, Transport, Workload,
};
use crate::topos::rail::ServerLocality;

use super::{
    AicbError, Algorithm, ChainItem, CollectiveOp, ExpertRouting, GroupKind, Groups,
    PipelineTransfer, Plan, SegmentEnd, StartItem,
};

/// Builds the workload IR of a planned schedule.
pub fn lower_plan(
    plan: &Plan,
    groups: &Groups,
    routing: ExpertRouting,
    locality: &ServerLocality,
    mtu_bytes: u64,
    transport: Transport,
) -> Result<Workload, AicbError> {
    let mut ordinals = BTreeMap::new();
    let mut transfer_ordinals = BTreeMap::new();
    for (position, item) in plan.start_order.iter().enumerate() {
        match *item {
            StartItem::Collective(op) => {
                ordinals.insert(op, position as u64);
            }
            StartItem::Pipeline(transfer) => {
                transfer_ordinals.insert(transfer, position as u64);
            }
        }
    }
    let stage_ranks = groups.stage_ranks();
    let mut builder = Builder {
        workload: Workload {
            groups: Vec::new(),
            transports: vec![transport],
            operations: Vec::new(),
        },
        group_index: BTreeMap::new(),
        groups,
        routing,
    };
    // The Send/Recv operations of each transfer, created when first met (by its sender or its
    // receiver); their `after` is the sender's segment, set when the sender's chain reaches it.
    let mut transfers: BTreeMap<PipelineTransfer, Vec<usize>> = BTreeMap::new();
    // The compute operation that ends at each data collective's fork, by stage.
    let mut forks: BTreeMap<(usize, u32), usize> = BTreeMap::new();
    for (stage, chain) in plan.stages.iter().enumerate() {
        let stage = stage as u32;
        let ranks = (stage * stage_ranks..(stage + 1) * stage_ranks)
            .map(u64::from)
            .collect::<Vec<_>>();
        // What the next segment follows at this stage's ranks.
        let mut previous: Vec<usize> = Vec::new();
        let mut segments = chain.segments.iter();
        for item in &chain.items {
            let end = match *item {
                ChainItem::Collective(op) if !plan.ops[op].single_server() => {
                    SegmentEnd::Collective(op)
                }
                ChainItem::Fork(op) => SegmentEnd::Fork(op),
                ChainItem::Send(transfer) => SegmentEnd::Send(transfer),
                ChainItem::Receive(transfer) => SegmentEnd::Receive(transfer),
                _ => continue,
            };
            let segment = segments
                .next()
                .ok_or_else(|| AicbError::new("a cross-host point has no preceding delay"))?;
            if segment.end != end {
                return Err(AicbError::new(format!(
                    "the chain's segment ends at {:?}, its cross-host point is {end:?}",
                    segment.end
                )));
            }
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
                    .ok_or_else(|| {
                        AicbError::new("a single-server collective's delay overflows")
                    })?;
                duration = duration
                    .checked_add(delay)
                    .ok_or_else(|| AicbError::new("a fused segment's delay overflows"))?;
            }
            let compute = builder.push_hosts(
                &ranks,
                std::mem::take(&mut previous),
                0,
                OperationKind::Compute {
                    duration_ns: duration,
                },
            );
            match end {
                SegmentEnd::Collective(op) => {
                    let collective = &plan.ops[op];
                    let ordinal = ordinals[&op];
                    for instance in builder.stage_instances(collective.group, stage) {
                        let kind = builder.collective(collective, instance, ordinal)?;
                        previous.push(builder.push_instance(
                            collective.group,
                            instance,
                            vec![compute],
                            0,
                            kind,
                        ));
                    }
                }
                SegmentEnd::Fork(op) => {
                    forks.insert((op, stage), compute);
                    previous.push(compute);
                }
                SegmentEnd::Send(transfer) => {
                    let operations =
                        builder.transfer(&mut transfers, transfer, &transfer_ordinals, plan)?;
                    for &operation in &operations {
                        builder.workload.operations[operation].after = vec![compute];
                    }
                    previous.extend(operations);
                }
                SegmentEnd::Receive(transfer) => {
                    let operations =
                        builder.transfer(&mut transfers, transfer, &transfer_ordinals, plan)?;
                    previous.push(compute);
                    previous.extend(operations);
                }
            }
        }
    }
    // The data queue, in R9's start order.
    let mut previous_data: Option<(GroupKind, Vec<usize>)> = None;
    for &op in &plan.data_queue.order {
        let collective = &plan.ops[op];
        let ordinal = ordinals[&op];
        let family = groups.family(collective.group);
        let mut operations = Vec::with_capacity(family.len());
        for instance in 0..family.len() {
            let stage = family.group(instance)[0] / stage_ranks;
            let fork = *forks
                .get(&(op, stage))
                .ok_or_else(|| AicbError::new("a data collective has no fork in its stage"))?;
            let mut after = vec![fork];
            if let Some((previous_kind, previous_operations)) = &previous_data {
                // The previous data collective's instances that share a host with this one.
                let previous_family = groups.family(*previous_kind);
                let mut shared = family
                    .group(instance)
                    .iter()
                    .filter_map(|&rank| previous_family.group_index_of(rank))
                    .collect::<Vec<_>>();
                shared.sort_unstable();
                shared.dedup();
                after.extend(shared.into_iter().map(|index| previous_operations[index]));
            }
            let kind = builder.collective(collective, instance, ordinal)?;
            operations.push(builder.push_instance(collective.group, instance, after, 1, kind));
        }
        previous_data = Some((collective.group, operations));
    }
    if transfers
        .values()
        .flatten()
        .any(|&operation| builder.workload.operations[operation].after.is_empty())
    {
        return Err(AicbError::new("a pipeline transfer has no sender segment"));
    }
    Ok(builder.workload)
}

struct Builder<'a> {
    workload: Workload,
    group_index: BTreeMap<Vec<u64>, usize>,
    groups: &'a Groups,
    routing: ExpertRouting,
}

impl Builder<'_> {
    fn group(&mut self, hosts: Vec<u64>) -> usize {
        let workload = &mut self.workload;
        *self.group_index.entry(hosts).or_insert_with_key(|hosts| {
            workload.groups.push(hosts.clone());
            workload.groups.len() - 1
        })
    }

    fn push_hosts(
        &mut self,
        hosts: &[u64],
        after: Vec<usize>,
        stream: u32,
        kind: OperationKind,
    ) -> usize {
        let group = self.group(hosts.to_vec());
        self.workload.operations.push(Operation {
            group,
            after,
            stream,
            kind,
        });
        self.workload.operations.len() - 1
    }

    fn push_instance(
        &mut self,
        family: GroupKind,
        instance: usize,
        after: Vec<usize>,
        stream: u32,
        kind: OperationKind,
    ) -> usize {
        let hosts = self
            .groups
            .family(family)
            .group(instance)
            .iter()
            .map(|&rank| u64::from(rank))
            .collect::<Vec<_>>();
        self.push_hosts(&hosts, after, stream, kind)
    }

    /// The instances of a family that lie in pipeline stage `stage`.
    fn stage_instances(&self, family: GroupKind, stage: u32) -> Vec<usize> {
        let stage_ranks = self.groups.stage_ranks();
        let family = self.groups.family(family);
        (0..family.len())
            .filter(|&instance| family.group(instance)[0] / stage_ranks == stage)
            .collect()
    }

    /// The Send/Recv operations of a transfer: one per pair, sender first.
    fn transfer(
        &mut self,
        transfers: &mut BTreeMap<PipelineTransfer, Vec<usize>>,
        transfer: PipelineTransfer,
        ordinals: &BTreeMap<PipelineTransfer, u64>,
        plan: &Plan,
    ) -> Result<Vec<usize>, AicbError> {
        if let Some(operations) = transfers.get(&transfer) {
            return Ok(operations.clone());
        }
        let ordinal = *ordinals
            .get(&transfer)
            .ok_or_else(|| AicbError::new("a pipeline transfer is not in the start order"))?;
        let bytes = plan.pipeline_bytes;
        let stage_ranks = self.groups.stage_ranks();
        let low = transfer.boundary * stage_ranks;
        let mut operations = Vec::with_capacity(stage_ranks as usize);
        for rank in low..low + stage_ranks {
            let (sender, receiver) = if transfer.backward {
                (rank + stage_ranks, rank)
            } else {
                (rank, rank + stage_ranks)
            };
            operations.push(self.push_hosts(
                &[u64::from(sender), u64::from(receiver)],
                Vec::new(),
                0,
                OperationKind::Collective(Collective {
                    algorithm: IrAlgorithm::SendRecv,
                    bytes,
                    transport: 0,
                    channels: None,
                    uniform_floor: false,
                    seeded: None,
                    issue_ordinal: Some(ordinal),
                }),
            ));
        }
        transfers.insert(transfer, operations.clone());
        Ok(operations)
    }

    fn collective(
        &self,
        collective: &CollectiveOp,
        instance: usize,
        issue_ordinal: u64,
    ) -> Result<OperationKind, AicbError> {
        let ranks = self.groups.family(collective.group).group(instance);
        let hosts = ranks
            .iter()
            .map(|&rank| u64::from(rank))
            .collect::<Vec<_>>();
        let (algorithm, channels, seeded) = match collective.algorithm {
            Algorithm::AllToAll => {
                let seeded = match (self.routing, collective.matrix) {
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
