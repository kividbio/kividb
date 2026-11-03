use ahash::AHashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

pub struct LatencySample {
    pub timestamp: u64, // Unix seconds
    pub latency_ms: u32,
}

/// One event's samples, and the most it has taken since it was reset:
/// `LATENCY LATEST`'s fourth field.
#[derive(Default)]
struct Event {
    samples: VecDeque<LatencySample>,
    max_ms: u32,
}

pub struct LatencyTracker {
    events: RwLock<AHashMap<String, Event>>,
    max_samples: usize,
}

/// `latency-monitor-threshold`, in milliseconds; 0, the default, is off.
/// What a command or an expire cycle has to take to be recorded (#158).
static THRESHOLD_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn threshold_ms() -> u64 {
    THRESHOLD_MS.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_threshold_ms(ms: u64) {
    THRESHOLD_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
}

/// After a command has run: a `fast-command` or `command` event, as Redis
/// records it, if it took at least `latency-monitor-threshold` (#158). It
/// was recorded when the command was slower than `slowlog-log-slower-than`,
/// and a `fast-command` was one that took under a millisecond.
#[inline]
pub fn note_command(tracker: &LatencyTracker, name: &str, elapsed: std::time::Duration) {
    let threshold = threshold_ms();
    if threshold == 0 {
        return;
    }
    let ms = elapsed.as_millis() as u64;
    if ms < threshold {
        return;
    }
    let event = if crate::commands::server::is_fast_command(name) {
        "fast-command"
    } else {
        "command"
    };
    tracker.record(event, ms.min(u32::MAX as u64) as u32);
}

pub type SharedLatencyTracker = Arc<LatencyTracker>;

pub fn new_latency_tracker() -> SharedLatencyTracker {
    Arc::new(LatencyTracker {
        events: RwLock::new(AHashMap::new()),
        max_samples: 180,
    })
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl LatencyTracker {
    pub fn record(&self, event: &str, latency_ms: u32) {
        let mut map = self.events.write();
        let event = map.entry(event.to_string()).or_default();
        if event.samples.len() >= self.max_samples {
            event.samples.pop_front();
        }
        event.samples.push_back(LatencySample {
            timestamp: unix_secs(),
            latency_ms,
        });
        event.max_ms = event.max_ms.max(latency_ms);
    }

    /// `(event, latest time, latest ms, most ms)` of each event, as
    /// `LATENCY LATEST` gives them.
    pub fn latest(&self) -> Vec<(String, u64, u32, u32)> {
        self.events
            .read()
            .iter()
            .filter_map(|(name, event)| {
                event
                    .samples
                    .back()
                    .map(|s| (name.clone(), s.timestamp, s.latency_ms, event.max_ms))
            })
            .collect()
    }

    pub fn history(&self, event: &str) -> Vec<LatencySample> {
        self.events
            .read()
            .get(event)
            .map(|event| {
                event
                    .samples
                    .iter()
                    .map(|s| LatencySample {
                        timestamp: s.timestamp,
                        latency_ms: s.latency_ms,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Forget the samples of `events` — of every event, if none is named.
    /// Returns how many events had samples to forget.
    pub fn reset(&self, events: &[&str]) -> usize {
        let mut map = self.events.write();
        if events.is_empty() {
            let had = map.len();
            map.clear();
            had
        } else {
            events
                .iter()
                .filter(|event| map.remove(**event).is_some())
                .count()
        }
    }
}
