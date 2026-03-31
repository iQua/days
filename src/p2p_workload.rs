use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2pFlow {
    pub flow_id: usize,
    pub src: usize,
    pub dst: usize,
    pub bytes: usize,
    pub starts_after: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub seed: usize,
    pub duration: f64,
    pub log_path: String,
    pub port_rate: f64,
    pub capacity: usize,
    pub packet_size: i64,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            seed: 1000,
            duration: 1500.0,
            log_path: "logs/p2p_import".to_string(),
            port_rate: 8000.0,
            capacity: 100,
            packet_size: 4096,
        }
    }
}

fn parse_required_usize(field: &str, row: &[&str], headers: &[String]) -> Result<usize, String> {
    let idx = headers
        .iter()
        .position(|h| h == field)
        .ok_or_else(|| format!("missing required column `{field}`"))?;
    let token = row
        .get(idx)
        .ok_or_else(|| format!("missing value for column `{field}`"))?;
    token
        .trim()
        .parse::<usize>()
        .map_err(|e| format!("invalid usize in `{field}`: {e}"))
}

fn parse_optional_dependency_list(
    field: &str,
    row: &[&str],
    headers: &[String],
) -> Result<Vec<usize>, String> {
    let Some(idx) = headers.iter().position(|h| h == field) else {
        return Ok(Vec::new());
    };
    let token = row.get(idx).map(|s| s.trim()).unwrap_or_default();
    if token.is_empty() {
        return Ok(Vec::new());
    }
    let mut deps = Vec::new();
    for item in token.split('|') {
        let trimmed = item.trim();
        if trimmed.is_empty() || trimmed == "-1" {
            continue;
        }
        let value = trimmed
            .parse::<isize>()
            .map_err(|e| format!("invalid dependency id `{trimmed}`: {e}"))?;
        if value >= 0 {
            deps.push(value as usize);
        }
    }
    deps.sort_unstable();
    deps.dedup();
    Ok(deps)
}

pub fn parse_flow_dump_tsv(input: &str) -> Result<Vec<P2pFlow>, String> {
    let mut lines = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'));

    let header_line = lines
        .next()
        .ok_or_else(|| "empty input: missing header line".to_string())?;
    let headers = header_line
        .split('\t')
        .map(|h| h.trim().to_string())
        .collect::<Vec<_>>();

    let mut flows = Vec::new();
    for line in lines {
        let row = line.split('\t').collect::<Vec<_>>();
        let flow_id = parse_required_usize("flow_id", &row, &headers)?;
        let src = parse_required_usize("src", &row, &headers)?;
        let dst = parse_required_usize("dst", &row, &headers)?;
        let bytes = parse_required_usize("flow_size", &row, &headers)?;
        if bytes == 0 {
            continue;
        }
        let starts_after = parse_optional_dependency_list("prev", &row, &headers)?;
        flows.push(P2pFlow {
            flow_id,
            src,
            dst,
            bytes,
            starts_after,
        });
    }

    flows.sort_by_key(|f| f.flow_id);
    Ok(flows)
}

pub fn render_days_config_toml(flows: &[P2pFlow], opts: &ExportOptions) -> Result<String, String> {
    if flows.is_empty() {
        return Err("no valid flow records found".to_string());
    }
    if opts.packet_size <= 0 {
        return Err("packet_size must be positive".to_string());
    }

    let mut hosts = BTreeSet::<usize>::new();
    let mut edges = BTreeSet::<(usize, usize)>::new();
    for flow in flows {
        hosts.insert(flow.src);
        hosts.insert(flow.dst);
        let edge = if flow.src <= flow.dst {
            (flow.src, flow.dst)
        } else {
            (flow.dst, flow.src)
        };
        edges.insert(edge);
    }

    let hosts_vec = hosts.into_iter().collect::<Vec<_>>();
    let edges_vec = edges.into_iter().collect::<Vec<_>>();

    let mut out = String::new();
    out.push_str("# Generated from a p2p flow dump.\n");
    out.push_str("seed = ");
    out.push_str(&opts.seed.to_string());
    out.push('\n');

    out.push_str("edges = [");
    for (i, (a, b)) in edges_vec.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('[');
        out.push_str(&a.to_string());
        out.push_str(", ");
        out.push_str(&b.to_string());
        out.push(']');
    }
    out.push_str("]\n");

    out.push_str("hosts = [");
    for (i, host) in hosts_vec.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&host.to_string());
    }
    out.push_str("]\n");

    out.push_str("duration = ");
    out.push_str(&format!("{:.3}", opts.duration));
    out.push('\n');
    out.push_str("log_path = ");
    out.push('"');
    out.push_str(&opts.log_path);
    out.push_str("\"\n\n");

    out.push_str("[switch]\n");
    out.push_str("port_rate = ");
    out.push_str(&format!("{:.3}", opts.port_rate));
    out.push('\n');
    out.push_str("capacity = ");
    out.push_str(&opts.capacity.to_string());
    out.push('\n');
    out.push_str("weights = [1]\n");
    out.push_str("discipline = \"FIFO\"\n");
    out.push_str("drop = \"RED\"\n\n");

    for flow in flows {
        out.push_str("[[flow]]\n");
        out.push_str("flow_id = ");
        out.push_str(&flow.flow_id.to_string());
        out.push('\n');
        if !flow.starts_after.is_empty() {
            out.push_str("starts_after = [");
            for (i, dep) in flow.starts_after.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&dep.to_string());
            }
            out.push_str("]\n");
        }
        out.push_str("flow_type = \"PacketDistribution\"\n");
        out.push_str("graph = [[");
        out.push_str(&flow.src.to_string());
        out.push_str(", ");
        out.push_str(&flow.dst.to_string());
        out.push_str("]]\n");
        out.push_str("routing = \"PathFromConfig\"\n\n");
        out.push_str("[flow.traffic]\n");
        out.push_str("initial_delay = 0.0\n");
        out.push_str("size = ");
        out.push_str(&flow.bytes.to_string());
        out.push('\n');
        out.push_str("arr_dist = { type = \"Uniform\", low = 1.0, high = 1.0 }\n");
        out.push_str("pkt_size_dist = { type = \"DiscreteUniform\", low = ");
        out.push_str(&opts.packet_size.to_string());
        out.push_str(", high = ");
        out.push_str(&opts.packet_size.to_string());
        out.push_str(" }\n\n");
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{ExportOptions, parse_flow_dump_tsv, render_days_config_toml};

    #[test]
    fn parse_flow_dump_works() {
        let input = r#"
# rank	0
flow_key_channel	flow_key_idx	flow_id	src	dst	flow_size	channel_id	chunk_id	chunk_count	conn_type	prev	parent	child
0	0	3	0	1	4096	0	0	1	x		-1	4
0	1	4	1	2	8192	0	1	1	x	3	3	
"#;
        let parsed = parse_flow_dump_tsv(input).expect("must parse");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].flow_id, 3);
        assert_eq!(parsed[0].starts_after.len(), 0);
        assert_eq!(parsed[1].flow_id, 4);
        assert_eq!(parsed[1].starts_after, vec![3]);
    }

    #[test]
    fn render_config_contains_expected_sections() {
        let input = r#"
flow_key_channel	flow_key_idx	flow_id	src	dst	flow_size	channel_id	chunk_id	chunk_count	conn_type	prev	parent	child
0	0	1	2	5	4096	0	0	1	x		-1	2
"#;
        let parsed = parse_flow_dump_tsv(input).expect("must parse");
        let rendered =
            render_days_config_toml(&parsed, &ExportOptions::default()).expect("must render");
        assert!(rendered.contains("edges = [[2, 5]]"));
        assert!(rendered.contains("hosts = [2, 5]"));
        assert!(rendered.contains("[[flow]]"));
        assert!(rendered.contains("flow_id = 1"));
        assert!(rendered.contains("graph = [[2, 5]]"));
        assert!(rendered.contains("size = 4096"));
    }
}
