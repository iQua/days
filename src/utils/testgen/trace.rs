#![allow(dead_code)]

use csv::StringRecord;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct CsvIndex {
    map: HashMap<String, usize>,
}

impl CsvIndex {
    pub fn new(headers: &StringRecord) -> Self {
        let mut map = HashMap::new();
        for (idx, header) in headers.iter().enumerate() {
            map.insert(header.trim().to_string(), idx);
        }
        Self { map }
    }

    pub fn get<'a>(&self, record: &'a StringRecord, name: &str) -> Option<&'a str> {
        let idx = self.map.get(name)?;
        record.get(*idx).map(|v| v.trim())
    }

    pub fn get_u64(&self, record: &StringRecord, name: &str) -> Option<u64> {
        parse_u64(self.get(record, name))
    }

    pub fn get_i64(&self, record: &StringRecord, name: &str) -> Option<i64> {
        parse_i64(self.get(record, name))
    }

    pub fn get_f64(&self, record: &StringRecord, name: &str) -> Option<f64> {
        parse_f64(self.get(record, name))
    }

    pub fn get_bool(&self, record: &StringRecord, name: &str) -> Option<bool> {
        parse_bool(self.get(record, name))
    }
}

pub fn open_csv(path: &Path) -> Result<(csv::Reader<File>, CsvIndex), String> {
    let file =
        File::open(path).map_err(|e| format!("Failed to open CSV {}: {e}", path.display()))?;
    let mut reader = csv::Reader::from_reader(file);
    let headers = reader
        .headers()
        .map_err(|e| format!("Failed to read CSV header {}: {e}", path.display()))?
        .clone();
    Ok((reader, CsvIndex::new(&headers)))
}

fn parse_u64(value: Option<&str>) -> Option<u64> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    value.parse::<u64>().ok()
}

fn parse_i64(value: Option<&str>) -> Option<i64> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    value.parse::<i64>().ok()
}

fn parse_f64(value: Option<&str>) -> Option<f64> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    value.parse::<f64>().ok()
}

fn parse_bool(value: Option<&str>) -> Option<bool> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    match value.to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}
