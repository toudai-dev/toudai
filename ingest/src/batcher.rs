//! Group commit: concurrent requests of one signal share one canonical insert
//! and one serving insert, and each is answered when the insert holding its
//! rows commits (Decision 0003 D2).
//!
//! A batch opens with the first request to arrive and flushes at whichever
//! comes first: its row threshold, its flush interval (the Go collector's
//! 1000 rows or 1 s, so part counts stay comparable), or the moment that
//! leaves its tightest deadline [`FlushConfig::reserve`] for the inserts. A
//! request that arrives with less than the reserve left is instead written
//! on its own as soon as a flush slot is free, so one producer's short
//! `grpc-timeout` neither cuts the retries of everyone else's rows nor turns
//! every batch into a single request. At most [`FlushConfig::concurrency`]
//! flushes run at once; while they do, the next batch keeps collecting.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;

use crate::mapping::Mapped;
use crate::pipeline::Held;
use crate::writer::{MIN_ATTEMPT, ServingRow, WriteError, Writer};

#[derive(Debug, Clone, Copy)]
pub struct FlushConfig {
    /// Flush once a batch holds this many serving rows.
    pub rows: usize,
    /// Flush this long after a batch opens.
    pub interval: Duration,
    /// Flush no later than this long before the batch's tightest deadline,
    /// leaving the inserts time for their retries.
    pub reserve: Duration,
    /// Flushes in flight at once, as the Go exporter's four queue consumers.
    /// Without a bound, a slow ClickHouse would face one retrying insert per
    /// batch for every batch that formed while the others were retrying.
    pub concurrency: usize,
}

impl Default for FlushConfig {
    fn default() -> FlushConfig {
        FlushConfig {
            rows: 1000,
            interval: Duration::from_secs(1),
            reserve: Duration::from_secs(1),
            concurrency: 4,
        }
    }
}

/// How a request's rows fared. A failure is shared by every request in the
/// batch, so it is reference-counted.
pub type Outcome = Result<(), Arc<WriteError>>;

struct Job<R> {
    mapped: Mapped<R>,
    deadline: Instant,
    /// The request's share of the admission budget, held until the insert
    /// holding its rows finishes, even if the producer has gone.
    held: Held,
    reply: oneshot::Sender<Outcome>,
}

/// The handle requests submit to; the batching task ends when it is dropped.
pub struct Batcher<R> {
    jobs: mpsc::UnboundedSender<Job<R>>,
    writer: Arc<Writer>,
    flushing: Arc<Semaphore>,
    reserve: Duration,
}

impl<R: ServingRow> Batcher<R> {
    /// Start the batching task on the current runtime.
    pub fn spawn(writer: Arc<Writer>, config: FlushConfig) -> Batcher<R> {
        let (jobs, receiver) = mpsc::unbounded_channel();
        let flushing = Arc::new(Semaphore::new(config.concurrency));
        tokio::spawn(collect(receiver, writer.clone(), flushing.clone(), config));
        Batcher {
            jobs,
            writer,
            flushing,
            reserve: config.reserve,
        }
    }

    /// Queue `mapped` for the next flush and wait for it. The admission
    /// budget bounds what can be queued, so the queue needs no bound of its
    /// own.
    pub async fn submit(&self, mapped: Mapped<R>, deadline: Instant, held: Held) -> Outcome {
        let (reply, outcome) = oneshot::channel();
        let job = Job {
            mapped,
            deadline,
            held,
            reply,
        };
        if deadline < Instant::now() + self.reserve {
            // Too short to wait for a batch: written alone, on a task of its
            // own so that, as in a batch, the write outlives the producer.
            let (writer, flushing) = (self.writer.clone(), self.flushing.clone());
            tokio::spawn(async move {
                let permit = flushing.acquire_owned().await;
                flush(writer, vec![job], permit.ok()).await;
            });
        } else if self.jobs.send(job).is_err() {
            return Err(Arc::new(timed_out()));
        }
        outcome.await.unwrap_or_else(|_| Err(Arc::new(timed_out())))
    }
}

/// The failure of a request that was not written: the same answer a write
/// that ran out of time gets.
fn timed_out() -> WriteError {
    WriteError {
        destination: crate::writer::Destination::Canonical,
        source: clickhouse::error::Error::TimedOut,
    }
}

async fn collect<R: ServingRow>(
    mut jobs: mpsc::UnboundedReceiver<Job<R>>,
    writer: Arc<Writer>,
    flushing: Arc<Semaphore>,
    config: FlushConfig,
) {
    while let Some(first) = jobs.recv().await {
        let opened = Instant::now();
        let mut rows = first.mapped.serving.len();
        let mut earliest = first.deadline;
        let mut batch = vec![first];
        while rows < config.rows {
            let room = earliest.checked_sub(config.reserve).unwrap_or(opened);
            let flush_at = (opened + config.interval).min(room);
            tokio::select! {
                // Requests already queued, such as those that arrived while
                // every flush slot was busy, join before a due flush fires.
                biased;
                job = jobs.recv() => match job {
                    Some(job) => {
                        rows += job.mapped.serving.len();
                        earliest = earliest.min(job.deadline);
                        batch.push(job);
                    }
                    None => break,
                },
                () = tokio::time::sleep_until(flush_at) => break,
            }
        }
        // Waiting for a slot here lets the next batch grow meanwhile, rather
        // than adding another insert against a ClickHouse already behind.
        let permit = flushing.clone().acquire_owned().await.ok();
        tokio::spawn(flush(writer.clone(), batch, permit));
    }
}

/// `_permit` is the flush slot, given back once the write ends.
async fn flush<R: ServingRow>(
    writer: Arc<Writer>,
    batch: Vec<Job<R>>,
    _permit: Option<OwnedSemaphorePermit>,
) {
    // A request with too little time left for an insert, such as one that
    // waited behind busy flush slots, is answered as one that ran out of
    // time rather than cutting the batch's deadline to its own.
    let now = Instant::now();
    let (late, ready): (Vec<_>, Vec<_>) = batch
        .into_iter()
        .partition(|job| job.deadline.saturating_duration_since(now) < MIN_ATTEMPT);
    if !late.is_empty() {
        writer
            .counters()
            .add("late_requests", &[("signal", R::LABEL)], late.len() as u64);
        let late_error = Arc::new(timed_out());
        for job in late {
            let _ = job.reply.send(Err(late_error.clone()));
        }
    }
    let Some(deadline) = ready.iter().map(|job| job.deadline).min() else {
        return;
    };
    let mut merged = Mapped {
        serving: Vec::new(),
        canonical: Vec::new(),
        rejections: Vec::new(),
        dropped_reserved: 0,
    };
    let mut waiting = Vec::with_capacity(ready.len());
    for job in ready {
        merged.serving.extend(job.mapped.serving);
        merged.canonical.extend(job.mapped.canonical);
        merged.rejections.extend(job.mapped.rejections);
        merged.dropped_reserved += job.mapped.dropped_reserved;
        waiting.push((job.reply, job.held));
    }
    let outcome = writer.write(&merged, deadline).await.map_err(|error| {
        // Logged once per batch, not once per request in it. The error names
        // the destination and ClickHouse's reason, never a key.
        eprintln!("toudai-ingest: {} write: {error}", R::LABEL);
        Arc::new(error)
    });
    for (reply, held) in waiting {
        let _ = reply.send(outcome.clone());
        drop(held);
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use uuid::Uuid;

    use super::*;
    use crate::mapping::{LogRow, map_logs};
    use crate::writer::WriterConfig;

    /// ClickHouse is unreachable (nothing listens on port 1), so every write
    /// fails; the `batches` counter shows how many flushes there were.
    fn batcher(config: FlushConfig) -> (Batcher<LogRow>, Arc<Writer>) {
        let config_ = WriterConfig {
            url: "http://127.0.0.1:1".into(),
            user: None,
            password: None,
            database: "toudai".into(),
        };
        let writer = Arc::new(Writer::new(config_, Arc::default()).unwrap());
        (Batcher::spawn(writer.clone(), config), writer)
    }

    fn rows(n: usize) -> Mapped<LogRow> {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default(); n],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let ids: Vec<Uuid> = (0..n).map(|_| Uuid::now_v7()).collect();
        map_logs(&request, "prod", &ids, 1)
    }

    fn flushes(writer: &Writer) -> u64 {
        writer.counters().get("batches", &[("signal", "logs")])
    }

    fn attempts(writer: &Writer) -> u64 {
        let labels = [("signal", "logs"), ("destination", "canonical")];
        writer.counters().get("insert_failures", &labels)
    }

    fn after(ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    #[tokio::test]
    async fn concurrent_requests_share_one_flush() {
        let (batcher, writer) = batcher(FlushConfig {
            interval: Duration::from_millis(200),
            reserve: Duration::ZERO,
            ..FlushConfig::default()
        });
        let submit = |n| batcher.submit(rows(n), after(600), Held::default());
        let (a, b, c) = tokio::join!(submit(1), submit(2), submit(3));
        let (a, b, c) = (a.unwrap_err(), b.unwrap_err(), c.unwrap_err());
        assert!(
            Arc::ptr_eq(&a, &b) && Arc::ptr_eq(&b, &c),
            "one write, one outcome"
        );
        assert_eq!(flushes(&writer), 1);
    }

    #[tokio::test]
    async fn flushes_at_the_row_threshold_without_waiting() {
        let (batcher, writer) = batcher(FlushConfig {
            rows: 2,
            interval: Duration::from_secs(30),
            reserve: Duration::ZERO,
            ..FlushConfig::default()
        });
        let started = Instant::now();
        let _ = batcher.submit(rows(2), after(300), Held::default()).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(flushes(&writer), 1);
    }

    #[tokio::test]
    async fn a_tight_deadline_flushes_early() {
        let (batcher, writer) = batcher(FlushConfig {
            interval: Duration::from_secs(30),
            reserve: Duration::from_millis(200),
            ..FlushConfig::default()
        });
        let started = Instant::now();
        let _ = batcher.submit(rows(1), after(500), Held::default()).await;
        let elapsed = started.elapsed();
        // Flushed 200 ms before the deadline, not after the 30 s interval.
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(2),
            "{elapsed:?}"
        );
        assert_eq!(flushes(&writer), 1);
    }

    /// A request with no time left for an insert is answered without being
    /// written, and does not cut the rest of the batch's retries to its own
    /// deadline.
    #[tokio::test]
    async fn a_late_request_does_not_shorten_the_batch() {
        let (batcher, writer) = batcher(FlushConfig {
            interval: Duration::from_millis(100),
            reserve: Duration::ZERO,
            ..FlushConfig::default()
        });
        let (late, on_time) = tokio::join!(
            batcher.submit(rows(1), after(60), Held::default()),
            batcher.submit(rows(1), after(800), Held::default()),
        );
        let (late, on_time) = (late.unwrap_err(), on_time.unwrap_err());
        assert!(
            matches!(late.source, clickhouse::error::Error::TimedOut),
            "{late}"
        );
        assert!(!Arc::ptr_eq(&late, &on_time));
        assert_eq!(flushes(&writer), 1, "only the on-time request was written");
        assert!(
            attempts(&writer) >= 2,
            "the write was cut to the late deadline"
        );
    }

    /// A batch flushes in time for its tightest deadline, even when the
    /// request carrying it arrives second, and is written with that deadline
    /// so no request in it is answered late.
    #[tokio::test]
    async fn a_batch_keeps_its_earliest_deadline() {
        let (batcher, writer) = batcher(FlushConfig {
            interval: Duration::from_secs(30),
            reserve: Duration::from_secs(1),
            ..FlushConfig::default()
        });
        let started = Instant::now();
        let (long, short) = tokio::join!(
            batcher.submit(rows(1), after(3000), Held::default()),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                batcher.submit(rows(1), after(1400), Held::default()).await
            },
        );
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_millis(1450), "{elapsed:?}");
        assert!(Arc::ptr_eq(&long.unwrap_err(), &short.unwrap_err()));
        assert_eq!(flushes(&writer), 1);
    }

    /// A request with less than the reserve left is written on its own at
    /// once: it neither joins the open batch nor cuts that batch's retries to
    /// its own deadline.
    #[tokio::test]
    async fn a_short_deadline_is_written_alone() {
        let (batcher, writer) = batcher(FlushConfig {
            interval: Duration::from_millis(300),
            reserve: Duration::from_secs(1),
            ..FlushConfig::default()
        });
        let started = Instant::now();
        let (long, short) = tokio::join!(
            async {
                let outcome = batcher.submit(rows(1), after(3000), Held::default()).await;
                (outcome, started.elapsed())
            },
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let outcome = batcher.submit(rows(1), after(400), Held::default()).await;
                (outcome, started.elapsed())
            },
        );
        // The short request was answered by its deadline; the long one kept
        // its own retries (its write stops no earlier than 1.1 s before 3 s).
        assert!(short.1 < Duration::from_millis(500), "{:?}", short.1);
        assert!(long.1 > Duration::from_millis(1800), "{:?}", long.1);
        assert!(!Arc::ptr_eq(&long.0.unwrap_err(), &short.0.unwrap_err()));
        assert_eq!(flushes(&writer), 2);
    }

    /// Flushes beyond the concurrency limit wait for a slot, solo flushes of
    /// short-deadline requests included; a request whose deadline passes
    /// meanwhile is answered without being written, and counted.
    #[tokio::test]
    async fn flushes_wait_for_a_free_slot() {
        let (batcher, writer) = batcher(FlushConfig {
            interval: Duration::from_millis(50),
            reserve: Duration::from_secs(1),
            concurrency: 1,
            ..FlushConfig::default()
        });
        let started = Instant::now();
        let at = |ms| started + Duration::from_millis(ms);
        let (first, batched, short) = tokio::join!(
            // Its write retries until at least 1.4 s.
            batcher.submit(rows(1), at(2500), Held::default()),
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                // 1.1 s left: batched.
                batcher.submit(rows(1), at(1300), Held::default()).await
            },
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                // 0.8 s left, under the reserve: a solo flush.
                batcher.submit(rows(1), at(1000), Held::default()).await
            },
        );
        let (first, batched, short) =
            (first.unwrap_err(), batched.unwrap_err(), short.unwrap_err());
        assert!(!Arc::ptr_eq(&first, &batched) && !Arc::ptr_eq(&first, &short));
        assert_eq!(flushes(&writer), 1, "a later flush was written alongside");
        let late = writer
            .counters()
            .get("late_requests", &[("signal", "logs")]);
        assert_eq!(late, 2);
    }
}
