//! Legacy topology runtime over shared topology graphs and configuration.

pub mod build {
    use petgraph::graph::UnGraph;

    pub use days::topos::build::{
        HostAttachment, HostAttachments, PairingPolicy, Result, TopologyError, TopologyProfile,
    };

    pub fn build_graph(file_path: &str) -> Result<(UnGraph<usize, ()>, HostAttachments)> {
        let (graph, hosts, _) = build_graph_with_profile(file_path)?;
        Ok((graph, hosts))
    }

    /// The legacy runtime has one link rate and no rail model, so it refuses the Spectrum-X
    /// fabric (Days AGO lowers it).
    pub fn build_graph_with_profile(
        file_path: &str,
    ) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)> {
        crate::validate_config(file_path).map_err(TopologyError::InvalidConfig)?;
        let built = days::topos::build::build_graph_with_profile(file_path)?;
        if let TopologyProfile::Rail(_) = built.2 {
            return Err(TopologyError::InvalidConfig(
                "the legacy runtime does not support the SpectrumX topology".into(),
            ));
        }
        Ok(built)
    }
}
pub mod topo;
