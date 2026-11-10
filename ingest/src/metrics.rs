//! Process counters, kept in the service so they stay readable while
//! ClickHouse is down (RFD 0001 PoC gate: independent health counters), and
//! served as Prometheus text on `/metrics`.
//!
//! A series is a metric name plus label pairs. Label values are fixed strings
//! chosen by the service (signals, destinations, rejection reasons), never
//! producer input, so they need no escaping and cannot grow the series set
//! without bound.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;

/// Every series name starts with this prefix, and every counter ends with
/// `_total` as Prometheus convention asks. The Go exporter's
/// `toudai_exporter_records` (served by the Collector without a suffix, as
/// Experiment 021's `metrics-split.txt` shows) is `toudai_ingest_records_total`
/// here, with the same labels, so dashboards rename both prefix and suffix.
pub const PREFIX: &str = "toudai_ingest_";

#[derive(Debug, Default)]
pub struct Counters {
    /// (name, rendered labels) → value.
    series: Mutex<BTreeMap<(String, String), u64>>,
}

fn label_text(labels: &[(&str, &str)]) -> String {
    let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
    labels.join(",")
}

impl Counters {
    pub fn add(&self, name: &str, labels: &[(&str, &str)], n: u64) {
        if n == 0 {
            return;
        }
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        *series
            .entry((name.to_owned(), label_text(labels)))
            .or_default() += n;
    }

    /// The current value of one series, 0 when it was never incremented.
    pub fn get(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        series
            .get(&(name.to_owned(), label_text(labels)))
            .copied()
            .unwrap_or(0)
    }

    /// Every series in the Prometheus text exposition format, grouped by
    /// metric with one `# TYPE` line each.
    pub fn render(&self) -> String {
        let series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = String::new();
        let mut current = None;
        for ((name, labels), value) in series.iter() {
            if current != Some(name) {
                let _ = writeln!(out, "# TYPE {PREFIX}{name}_total counter");
                current = Some(name);
            }
            if labels.is_empty() {
                let _ = writeln!(out, "{PREFIX}{name}_total {value}");
            } else {
                let _ = writeln!(out, "{PREFIX}{name}_total{{{labels}}} {value}");
            }
        }
        out
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
    }

    #[test]
    fn renders_prometheus_text() {
        let counters = Counters::default();
        counters.add(
            "records",
            &[("signal", "spans"), ("destination", "serving")],
            7,
        );
        counters.add("batches", &[("signal", "logs")], 1);
        counters.add(
            "records",
            &[("signal", "logs"), ("destination", "serving")],
            5,
        );
        counters.add("restarts", &[], 2);
        counters.add("ignored", &[("signal", "logs")], 0);
        let want = "\
# TYPE toudai_ingest_batches_total counter
toudai_ingest_batches_total{signal=\"logs\"} 1
# TYPE toudai_ingest_records_total counter
toudai_ingest_records_total{signal=\"logs\",destination=\"serving\"} 5
toudai_ingest_records_total{signal=\"spans\",destination=\"serving\"} 7
# TYPE toudai_ingest_restarts_total counter
toudai_ingest_restarts_total 2
";
        assert_eq!(counters.render(), want);
    }
}
