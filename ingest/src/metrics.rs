//! Process counters, kept in the service so they stay readable while
//! ClickHouse is down (RFD 0001 PoC gate: independent health counters).
//!
//! A series is a metric name plus label pairs, keyed by its Prometheus text
//! form. Label values are fixed strings chosen by the service (signals,
//! destinations, rejection reasons), never producer input, so they need no
//! escaping and cannot grow the series set without bound.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// Every series name starts with this prefix.
pub const PREFIX: &str = "toudai_ingest_";

#[derive(Debug, Default)]
pub struct Counters {
    series: Mutex<BTreeMap<String, u64>>,
}

fn series_key(name: &str, labels: &[(&str, &str)]) -> String {
    let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
    format!("{PREFIX}{name}{{{}}}", labels.join(","))
}

impl Counters {
    pub fn add(&self, name: &str, labels: &[(&str, &str)], n: u64) {
        if n == 0 {
            return;
        }
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        *series.entry(series_key(name, labels)).or_default() += n;
    }

    /// The current value of one series, 0 when it was never incremented.
    pub fn get(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        series.get(&series_key(name, labels)).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_are_keyed_by_name_and_labels_in_order() {
        let counters = Counters::default();
        counters.add(
            "records",
            &[("signal", "logs"), ("destination", "serving")],
            2,
        );
        counters.add(
            "records",
            &[("signal", "logs"), ("destination", "serving")],
            3,
        );
        counters.add(
            "records",
            &[("signal", "spans"), ("destination", "serving")],
            7,
        );
        counters.add("batches", &[("signal", "logs")], 0);
        let cases = [
            (
                "records",
                vec![("signal", "logs"), ("destination", "serving")],
                5,
            ),
            (
                "records",
                vec![("signal", "spans"), ("destination", "serving")],
                7,
            ),
            (
                "records",
                vec![("signal", "logs"), ("destination", "canonical")],
                0,
            ),
            ("batches", vec![("signal", "logs")], 0),
        ];
        for (name, labels, want) in cases {
            assert_eq!(counters.get(name, &labels), want, "{name} {labels:?}");
        }
        assert_eq!(
            series_key("records", &[("signal", "logs")]),
            "toudai_ingest_records{signal=\"logs\"}"
        );
    }
}
