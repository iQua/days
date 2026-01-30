use serde_json::{Map, Number, Value};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct NdjsonRow {
    time_ns: u64,
    event_id: u64,
    obj: Map<String, Value>,
}

/// Default output path for an NDJSON export: replace the input extension with `.ndjson`.
pub fn default_ndjson_output_path(input: &Path) -> PathBuf {
    let mut out = input.to_path_buf();
    out.set_extension("ndjson");
    out
}

/// Export a Days `*_events.csv` file to an NDJSON file (one JSON object per row).
///
/// If `sort` is true, rows are canonicalized by sorting on `(time_ns, event_id)` and duplicates
/// are rejected.
pub fn export_csv_to_ndjson(input: &Path, output: &Path, sort: bool) -> Result<(), String> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(input)
        .map_err(|e| format!("Failed to open CSV {}: {e}", input.display()))?;

    let headers = rdr
        .headers()
        .map_err(|e| format!("Failed to read CSV header {}: {e}", input.display()))?
        .clone();

    let time_idx = headers
        .iter()
        .position(|h| h == "time_ns")
        .ok_or_else(|| "CSV header is missing required column: time_ns".to_string())?;
    let event_idx = headers
        .iter()
        .position(|h| h == "event_id")
        .ok_or_else(|| "CSV header is missing required column: event_id".to_string())?;

    let mut rows = Vec::new();
    for (row_idx, result) in rdr.records().enumerate() {
        let line_no = row_idx + 2;
        let record = result.map_err(|e| {
            format!(
                "{}:{}: Failed to read CSV record: {e}",
                input.display(),
                line_no
            )
        })?;

        let time_ns_str = record
            .get(time_idx)
            .ok_or_else(|| format!("{}:{}: missing time_ns field", input.display(), line_no))?
            .trim();
        let event_id_str = record
            .get(event_idx)
            .ok_or_else(|| format!("{}:{}: missing event_id field", input.display(), line_no))?
            .trim();

        let time_ns = time_ns_str.parse::<u64>().map_err(|e| {
            format!(
                "{}:{}: invalid time_ns value {:?}: {e}",
                input.display(),
                line_no,
                time_ns_str
            )
        })?;
        let event_id = event_id_str.parse::<u64>().map_err(|e| {
            format!(
                "{}:{}: invalid event_id value {:?}: {e}",
                input.display(),
                line_no,
                event_id_str
            )
        })?;

        let mut obj = Map::new();
        for (i, key) in headers.iter().enumerate() {
            let raw = record.get(i).unwrap_or("").trim();
            if raw.is_empty() {
                continue;
            }
            obj.insert(key.to_string(), parse_scalar_value(raw));
        }

        rows.push(NdjsonRow {
            time_ns,
            event_id,
            obj,
        });
    }

    if sort {
        rows.sort_by_key(|r| (r.time_ns, r.event_id));

        for pair in rows.windows(2) {
            let a = &pair[0];
            let b = &pair[1];
            if a.time_ns == b.time_ns && a.event_id == b.event_id {
                return Err(format!(
                    "Duplicate key (time_ns={}, event_id={}) in {}",
                    a.time_ns,
                    a.event_id,
                    input.display()
                ));
            }
        }
    }

    let file = File::create(output)
        .map_err(|e| format!("Failed to create NDJSON {}: {e}", output.display()))?;
    let mut writer = BufWriter::new(file);

    for row in rows {
        serde_json::to_writer(&mut writer, &Value::Object(row.obj))
            .map_err(|e| format!("Failed to serialize NDJSON row: {e}"))?;
        writer
            .write_all(b"\n")
            .map_err(|e| format!("Failed to write NDJSON row: {e}"))?;
    }

    writer
        .flush()
        .map_err(|e| format!("Failed to flush NDJSON output: {e}"))?;
    Ok(())
}

/// Export a Days `*_events.csv` file to a `.tla` module that defines `Trace` as a sequence of records.
///
/// This is intended for TLC versions that do not support directly deserializing NDJSON.
///
/// If `sort` is true, rows are canonicalized by sorting on `(time_ns, event_id)` and duplicates
/// are rejected.
pub fn export_csv_to_tla_trace_module(
    input: &Path,
    output: &Path,
    sort: bool,
    module_name: &str,
) -> Result<(), String> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(input)
        .map_err(|e| format!("Failed to open CSV {}: {e}", input.display()))?;

    let headers = rdr
        .headers()
        .map_err(|e| format!("Failed to read CSV header {}: {e}", input.display()))?
        .clone();

    let time_idx = headers
        .iter()
        .position(|h| h == "time_ns")
        .ok_or_else(|| "CSV header is missing required column: time_ns".to_string())?;
    let event_idx = headers
        .iter()
        .position(|h| h == "event_id")
        .ok_or_else(|| "CSV header is missing required column: event_id".to_string())?;

    let mut rows = Vec::new();
    for (row_idx, result) in rdr.records().enumerate() {
        let line_no = row_idx + 2;
        let record = result.map_err(|e| {
            format!(
                "{}:{}: Failed to read CSV record: {e}",
                input.display(),
                line_no
            )
        })?;

        let time_ns_str = record
            .get(time_idx)
            .ok_or_else(|| format!("{}:{}: missing time_ns field", input.display(), line_no))?
            .trim();
        let event_id_str = record
            .get(event_idx)
            .ok_or_else(|| format!("{}:{}: missing event_id field", input.display(), line_no))?
            .trim();

        let time_ns = time_ns_str.parse::<u64>().map_err(|e| {
            format!(
                "{}:{}: invalid time_ns value {:?}: {e}",
                input.display(),
                line_no,
                time_ns_str
            )
        })?;
        let event_id = event_id_str.parse::<u64>().map_err(|e| {
            format!(
                "{}:{}: invalid event_id value {:?}: {e}",
                input.display(),
                line_no,
                event_id_str
            )
        })?;

        let mut obj = Map::new();
        for (i, key) in headers.iter().enumerate() {
            let raw = record.get(i).unwrap_or("").trim();
            if raw.is_empty() {
                continue;
            }
            obj.insert(key.to_string(), parse_scalar_value(raw));
        }

        rows.push(NdjsonRow {
            time_ns,
            event_id,
            obj,
        });
    }

    if sort {
        rows.sort_by_key(|r| (r.time_ns, r.event_id));

        for pair in rows.windows(2) {
            let a = &pair[0];
            let b = &pair[1];
            if a.time_ns == b.time_ns && a.event_id == b.event_id {
                return Err(format!(
                    "Duplicate key (time_ns={}, event_id={}) in {}",
                    a.time_ns,
                    a.event_id,
                    input.display()
                ));
            }
        }
    }

    let file = File::create(output)
        .map_err(|e| format!("Failed to create TLA module {}: {e}", output.display()))?;
    let mut writer = BufWriter::new(file);

    writeln!(
        writer,
        "---------------------------- MODULE {module_name} ----------------------------"
    )
    .map_err(|e| format!("Failed to write TLA header: {e}"))?;
    writeln!(writer, "\\* AUTOGENERATED from {}", input.display())
        .map_err(|e| format!("Failed to write TLA header: {e}"))?;
    writeln!(
        writer,
        "\\* NOTE: This trace is rescaled to fit TLC's 32-bit integers:"
    )
    .map_err(|e| format!("Failed to write TLA header: {e}"))?;
    writeln!(
        writer,
        "\\* - `*_ns`   are stored in microseconds (rounded)"
    )
    .map_err(|e| format!("Failed to write TLA header: {e}"))?;
    writeln!(
        writer,
        "\\* - `*_bps`  are stored in units of 10 Mbps (rounded)"
    )
    .map_err(|e| format!("Failed to write TLA header: {e}"))?;
    writeln!(
        writer,
        "\\* - `*_ppb`  are stored in permille (1/1000) units (rounded)"
    )
    .map_err(|e| format!("Failed to write TLA header: {e}"))?;
    writeln!(writer, "").map_err(|e| format!("Failed to write TLA header: {e}"))?;

    writeln!(writer, "Trace == <<").map_err(|e| format!("Failed to write TLA Trace: {e}"))?;
    for (idx, row) in rows.iter().enumerate() {
        write!(writer, "  [").map_err(|e| format!("Failed to write TLA Trace row: {e}"))?;

        let mut first = true;
        for key in headers.iter() {
            let Some(value) = row.obj.get(key) else {
                continue;
            };
            let value = scale_for_tlc_trace(key, value.clone()).map_err(|e| {
                format!(
                    "Failed to rescale field {key} for TLC trace export (input {}): {e}",
                    input.display()
                )
            })?;
            if !first {
                write!(writer, ", ").map_err(|e| format!("Failed to write TLA Trace row: {e}"))?;
            }
            first = false;

            write!(writer, "{key} |-> {}", to_tla_value(&value))
                .map_err(|e| format!("Failed to write TLA Trace row: {e}"))?;
        }

        if idx + 1 == rows.len() {
            writeln!(writer, "]").map_err(|e| format!("Failed to write TLA Trace row: {e}"))?;
        } else {
            writeln!(writer, "],").map_err(|e| format!("Failed to write TLA Trace row: {e}"))?;
        }
    }
    writeln!(writer, ">>").map_err(|e| format!("Failed to write TLA Trace: {e}"))?;

    writeln!(
        writer,
        "\n============================================================================="
    )
    .map_err(|e| format!("Failed to write TLA footer: {e}"))?;

    writer
        .flush()
        .map_err(|e| format!("Failed to flush TLA output: {e}"))?;
    Ok(())
}

fn parse_scalar_value(s: &str) -> Value {
    match s {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }

    if let Some(num) = parse_json_number(s) {
        return Value::Number(num);
    }

    Value::String(s.to_string())
}

fn parse_json_number(s: &str) -> Option<Number> {
    if s.is_empty() {
        return None;
    }

    let (neg, rest) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
    if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    if neg {
        let n = s.parse::<i64>().ok()?;
        Some(Number::from(n))
    } else {
        let n = s.parse::<u64>().ok()?;
        Some(Number::from(n))
    }
}

fn to_tla_value(v: &Value) -> String {
    match v {
        Value::Bool(true) => "TRUE".to_string(),
        Value::Bool(false) => "FALSE".to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", escape_tla_string(s)),
        _ => "FALSE".to_string(),
    }
}

fn escape_tla_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

fn scale_for_tlc_trace(field: &str, value: Value) -> Result<Value, String> {
    let Value::Number(n) = value else {
        return Ok(value);
    };

    let raw = if let Some(v) = n.as_u64() {
        v
    } else if let Some(v) = n.as_i64() {
        if v < 0 {
            return Err("negative integers are not supported in TLC trace export".to_string());
        }
        v as u64
    } else {
        return Ok(Value::Number(n));
    };

    let div = if field.ends_with("_ns") {
        1_000u64
    } else if field.ends_with("_bps") {
        10_000_000u64
    } else if field.ends_with("_ppb") {
        1_000_000u64
    } else {
        return Ok(Value::Number(n));
    };

    let scaled = (raw + (div / 2)) / div;
    if scaled > i32::MAX as u64 {
        return Err(format!(
            "scaled value {scaled} exceeds TLC's 32-bit integer range"
        ));
    }
    Ok(Value::Number(Number::from(scaled)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndjson_is_lossless_but_tla_trace_is_scaled_for_tlc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("dcqcn_events.csv");
        let ndjson_path = dir.path().join("dcqcn_events.ndjson");
        let tla_path = dir.path().join("TraceData.tla");

        {
            let mut f = File::create(&csv_path).expect("create csv");
            writeln!(f, "time_ns,event_id,kind,rate_bps,g_ppb,cnp_interval_ns").unwrap();
            writeln!(f, "100000,7,cnp_recv,10000000000,500000000,10000").unwrap();
        }

        export_csv_to_ndjson(&csv_path, &ndjson_path, true).expect("ndjson export");
        let ndjson = std::fs::read_to_string(&ndjson_path).expect("read ndjson");
        assert!(
            ndjson.contains("\"rate_bps\":10000000000"),
            "expected lossless bps in ndjson, got: {ndjson}"
        );
        assert!(
            ndjson.contains("\"g_ppb\":500000000"),
            "expected lossless ppb in ndjson, got: {ndjson}"
        );
        assert!(
            ndjson.contains("\"time_ns\":100000"),
            "expected lossless ns in ndjson, got: {ndjson}"
        );

        export_csv_to_tla_trace_module(&csv_path, &tla_path, true, "TraceData")
            .expect("tla trace export");
        let tla = std::fs::read_to_string(&tla_path).expect("read tla");
        assert!(
            tla.contains("rate_bps |-> 1000"),
            "expected scaled bps in tla module, got:\n{tla}"
        );
        assert!(
            tla.contains("g_ppb |-> 500"),
            "expected scaled ppb in tla module, got:\n{tla}"
        );
        assert!(
            tla.contains("cnp_interval_ns |-> 10"),
            "expected scaled ns in tla module, got:\n{tla}"
        );
        assert!(
            tla.contains("time_ns |-> 100"),
            "expected scaled time in tla module, got:\n{tla}"
        );
    }
}
