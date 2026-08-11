//! Legacy topology runtime over shared topology graphs and configuration.

pub mod build {
    use petgraph::graph::UnGraph;

    pub use days::topos::build::{
        HostAttachment, HostAttachments, PairingPolicy, Result, TopologyError, TopologyProfile,
    };

    pub fn build_graph(file_path: &str) -> Result<(UnGraph<usize, ()>, HostAttachments)> {
        crate::validate_config(file_path).map_err(TopologyError::InvalidConfig)?;
        days::topos::build::build_graph(file_path)
    }

    pub fn build_graph_with_profile(
        file_path: &str,
    ) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)> {
        crate::validate_config(file_path).map_err(TopologyError::InvalidConfig)?;
        days::topos::build::build_graph_with_profile(file_path)
    }
}
pub mod topo;
