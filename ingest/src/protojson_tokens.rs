//! Fallback normalization without retaining a JSON Value tree.
use std::collections::{BTreeMap, HashSet};

use serde::Deserialize;
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use super::{SPECIALS, StandIns};

pub(super) fn normalize(body: &[u8]) -> Option<(Vec<u8>, StandIns)> {
    // Match the old Value parser's number/depth validation, including values
    // subsequently overwritten by duplicate keys, without allocating nodes.
    serde_json::from_slice::<Checked>(body).ok()?;
    let raw: &RawValue = serde_json::from_slice(body).ok()?;
    let mut out = Output {
        bytes: Vec::with_capacity(body.len()),
        doubles: HashSet::new(),
        specials: Vec::new(),
    };
    out.emit(raw, false, 0)?;
    let stand_ins = StandIns::choose(&out.doubles);
    for (offset, i) in out.specials {
        let number = serde_json::to_string(&stand_ins.0[i]).ok()?;
        out.bytes[offset..offset + number.len()].copy_from_slice(number.as_bytes());
    }
    Some((out.bytes, stand_ins))
}

struct Output {
    bytes: Vec<u8>,
    doubles: HashSet<u64>,
    specials: Vec<(usize, usize)>,
}

impl Output {
    fn emit(&mut self, raw: &RawValue, double: bool, depth: usize) -> Option<()> {
        if depth >= 128 {
            return None;
        }
        let text = raw.get();
        if double {
            if text.starts_with('"') {
                let value: String = serde_json::from_str(text).ok()?;
                if let Some(i) = SPECIALS.iter().position(|(s, _)| *s == value) {
                    self.specials.push((self.bytes.len(), i));
                    self.bytes
                        .extend_from_slice(b"0                               ");
                } else {
                    let number = value.parse::<f64>().ok().filter(|d| d.is_finite())?;
                    self.doubles.insert(number.to_bits());
                    serde_json::to_writer(&mut self.bytes, &number).ok()?;
                }
                return Some(());
            }
            if let Ok(number) = serde_json::from_str::<serde_json::Number>(text) {
                self.doubles.insert(number.as_f64()?.to_bits());
            }
        }
        match text.as_bytes()[0] {
            b'{' => {
                // The former Value map sorted keys and kept the last duplicate.
                // Borrow entire values; only this object's keys need allocation.
                let fields: BTreeMap<String, &RawValue> = serde_json::from_str(text).ok()?;
                self.bytes.push(b'{');
                let mut comma = false;
                for (key, value) in fields {
                    if value.get() == "null" {
                        continue;
                    }
                    if comma {
                        self.bytes.push(b',');
                    }
                    comma = true;
                    serde_json::to_writer(&mut self.bytes, &key).ok()?;
                    self.bytes.push(b':');
                    self.emit(value, key == "doubleValue", depth + 1)?;
                }
                self.bytes.push(b'}');
            }
            b'[' => {
                self.bytes.push(b'[');
                let mut parser = serde_json::Deserializer::from_str(text);
                de::Deserializer::deserialize_seq(&mut parser, Array { out: self, depth }).ok()?;
                self.bytes.push(b']');
            }
            _ => self.bytes.extend_from_slice(text.as_bytes()),
        }
        Some(())
    }
}

struct Array<'a> {
    out: &'a mut Output,
    depth: usize,
}

impl<'de> Visitor<'de> for Array<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let mut comma = false;
        while let Some(raw) = seq.next_element::<&RawValue>()? {
            if comma {
                self.out.bytes.push(b',');
            }
            comma = true;
            self.out
                .emit(raw, false, self.depth + 1)
                .ok_or_else(|| de::Error::custom("invalid protojson"))?;
        }
        Ok(())
    }
}

struct Checked;

impl<'de> Deserialize<'de> for Checked {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(Self)
    }
}

impl<'de> Visitor<'de> for Checked {
    type Value = Self;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON")
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self, A::Error> {
        while seq.next_element::<Checked>()?.is_some() {}
        Ok(self)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self, A::Error> {
        if let Some(first) = map.next_key::<String>()? {
            // Value's private escape interprets nested JSON strings with fresh
            // depth budgets and live copies of outer strings. Refuse it only
            // on the fallback path, rather than retaining that unbounded work.
            if first == "$serde_json::private::RawValue" {
                return Err(de::Error::custom("embedded raw JSON is not supported"));
            }
            map.next_value::<Checked>()?;
        }
        while map.next_entry::<IgnoredAny, Checked>()?.is_some() {}
        Ok(self)
    }
}
