//! Configuration-schema helpers (P16 a2aset part 2: unknown keys are refused, user ruling Oct 8):
//! the field names a derived struct accepts, and the table an unknown key sits in.

use serde::Deserialize;
use serde::de::{self, Visitor};

/// The `deserialize_struct` probe's outcome: the struct's field names.
#[derive(Debug)]
struct Fields(&'static [&'static str]);

impl std::fmt::Display for Fields {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "fields {:?}", self.0)
    }
}

impl std::error::Error for Fields {}

impl de::Error for Fields {
    fn custom<T: std::fmt::Display>(_: T) -> Self {
        Self(&[])
    }
}

/// A deserializer that only answers `deserialize_struct`, with the fields a derive declares.
struct Probe;

impl<'de> de::Deserializer<'de> for Probe {
    type Error = Fields;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Fields> {
        Err(Fields(&[]))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Fields> {
        Err(Fields(fields))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map enum identifier
        ignored_any
    }
}

/// The field names a `#[derive(Deserialize)]` struct accepts (empty for anything else).
pub fn struct_fields<T: for<'de> Deserialize<'de>>() -> &'static [&'static str] {
    match T::deserialize(Probe) {
        Ok(_) => &[],
        Err(Fields(fields)) => fields,
    }
}

/// For a TOML error refusing an unknown key, or an unknown value of an enumerated key (P17: the
/// error's span is then the value, on the key's line), the table it sits in: `` `[header]` ``
/// (the nearest table header above it, or the header that names the unknown key as its last
/// segment), or `the root table`. `None` for any other error.
pub fn unknown_key_table(content: &str, error: &toml::de::Error) -> Option<String> {
    if !error.message().contains("unknown field") && !error.message().contains("unknown variant") {
        return None;
    }
    Some(table_at(content, error.span()?.start))
}

/// The table a key at byte `offset` of `content` sits in: `` `[header]` `` (the nearest table
/// header above it, or the header on the key's own line, which names it as a sub-table), or
/// `the root table`.
pub fn table_at(content: &str, offset: usize) -> String {
    let end = content
        .get(offset..)
        .and_then(|rest| rest.find('\n'))
        .map_or(content.len(), |line| offset + line);
    table_up_to(content, end)
}

fn table_up_to(content: &str, end: usize) -> String {
    let header = content
        .get(..end)
        .unwrap_or(content)
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| is_header(line));
    header.map_or_else(|| "the root table".to_owned(), |line| format!("`{line}`"))
}

/// Whether a trimmed line is a table header (`[a.b]` or `[[a]]`, a trailing comment allowed), not
/// an array value such as `[0, 4],`.
fn is_header(line: &str) -> bool {
    let line = line.split('#').next().unwrap_or_default().trim_end();
    let inner = if let Some(rest) = line.strip_prefix("[[") {
        rest.strip_suffix("]]")
    } else if let Some(rest) = line.strip_prefix('[') {
        rest.strip_suffix(']')
    } else {
        None
    };
    inner.is_some_and(|name| {
        let name = name.trim();
        name.chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_' || first == '"')
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '"' | ' '))
    })
}
