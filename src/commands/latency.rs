use crate::commands::ctx::Ctx;
use crate::resp::write::*;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "LATENCY" {
        return None;
    }

    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'latency' command",
        ));
    }

    let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();
    match sub.as_str() {
        "LATEST" => {
            let samples = ctx.latency.latest();
            let mut out = format!("*{}\r\n", samples.len()).into_bytes();
            // Four fields, as Redis gives them: the maximum is what
            // redis_exporter reads (#158).
            for (name, ts, ms, max) in samples {
                out.extend_from_slice(b"*4\r\n");
                out.extend_from_slice(&resp_bulk_str(&name));
                out.extend_from_slice(&resp_integer(ts as i64));
                out.extend_from_slice(&resp_integer(ms as i64));
                out.extend_from_slice(&resp_integer(max as i64));
            }
            Some(out)
        }

        "HISTORY" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'latency|history' command",
                ));
            }
            let event = String::from_utf8_lossy(cmd[2]);
            let samples = ctx.latency.history(&event);
            let mut out = format!("*{}\r\n", samples.len()).into_bytes();
            for s in samples {
                out.extend_from_slice(b"*2\r\n");
                out.extend_from_slice(&resp_integer(s.timestamp as i64));
                out.extend_from_slice(&resp_integer(s.latency_ms as i64));
            }
            Some(out)
        }

        "RESET" => {
            let events: Vec<&str> = cmd[2..]
                .iter()
                .map(|b| std::str::from_utf8(b).unwrap_or(""))
                .collect();
            // The number of event series there were to reset.
            Some(resp_integer(ctx.latency.reset(&events) as i64))
        }

        "GRAPH" => {
            // Redis's graph: a header with the high and the low, then the
            // samples as an ASCII sparkline (see `graph`).
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'latency|graph' command",
                ));
            }
            let event = String::from_utf8_lossy(cmd[2]);
            let samples = ctx.latency.history(&event);
            if samples.is_empty() {
                return Some(resp_error(&format!("ERR No samples for event '{event}'")));
            }
            Some(resp_bulk_str(&graph(&event, &samples, unix_now_secs())))
        }

        "HISTOGRAM" => {
            // LATENCY HISTOGRAM [command ...]
            //
            // For each command that has been called: how many times, and
            // the distribution of how long it took — for each time that is
            // a power of two microseconds, how many calls took less. Only
            // the commands named, if any are: a command with subcommands
            // names them all, `client|id` names one, and a name that is not
            // a command's is passed over.
            let wanted: Vec<String> = cmd[2..]
                .iter()
                .map(|raw| String::from_utf8_lossy(raw).to_ascii_lowercase())
                .collect();
            let mut reply: Vec<Vec<u8>> = Vec::new();
            for (name, totals) in crate::cmdstats::totals() {
                let named = wanted.is_empty()
                    || wanted.iter().any(|want| {
                        *want == name
                            || name
                                .split_once('|')
                                .is_some_and(|(container, _)| container == want)
                    });
                if !named {
                    continue;
                }
                let mut histogram: Vec<Vec<u8>> = Vec::new();
                let mut so_far = 0u64;
                for (i, count) in totals.buckets.iter().enumerate() {
                    if *count > 0 {
                        so_far += count;
                        histogram.push(resp_integer(1i64 << i.min(62)));
                        histogram.push(resp_integer(so_far as i64));
                    }
                }
                let resp = crate::commands::handler::effective_resp_for_reply(ctx);
                reply.push(resp_bulk_str(&name));
                reply.push(resp_map_for(
                    resp,
                    vec![
                        resp_bulk_str("calls"),
                        resp_integer(totals.calls as i64),
                        resp_bulk_str("histogram_usec"),
                        resp_map_for(resp, histogram),
                    ],
                ));
            }
            Some(resp_map_for(
                crate::commands::handler::effective_resp_for_reply(ctx),
                reply,
            ))
        }

        "DOCTOR" => {
            // LATENCY DOCTOR — human-readable diagnostic report.
            //
            // Walks the latest latency samples and flags any event whose
            // most recent latency exceeds common thresholds. If nothing
            // looks wrong, emit Redis's exact clean-bill-of-health message
            // so tooling (redis-cli --latency-doctor) parses it correctly.
            let samples = ctx.latency.latest();

            if samples.is_empty() {
                return Some(resp_bulk_str(
                    "Dave, I have observed the system for some time \
                     and I have no problem to report.",
                ));
            }

            // Thresholds that indicate something worth reporting.
            // These match Redis's own heuristics.
            const WARN_MS: u32 = 100; // warn if latest sample >= 100ms
            const CRIT_MS: u32 = 500; // critical if >= 500ms

            let mut issues: Vec<String> = Vec::new();

            for (event, _ts, ms, _max) in &samples {
                if *ms >= CRIT_MS {
                    issues.push(format!(
                        "- Event '{}': latency is {}ms — CRITICAL. \
                         This is severely impacting response times. \
                         Check for blocking operations or system resource contention.",
                        event, ms
                    ));
                } else if *ms >= WARN_MS {
                    issues.push(format!(
                        "- Event '{}': latency is {}ms — WARNING. \
                         This may indicate slow commands, memory pressure, \
                         or OS scheduling delays.",
                        event, ms
                    ));
                }
            }

            if issues.is_empty() {
                Some(resp_bulk_str(
                    "Dave, I have observed the system for some time \
                     and I have no problem to report.",
                ))
            } else {
                let mut report = String::from("I detected the following latency issues:\n\n");
                for issue in &issues {
                    report.push_str(issue);
                    report.push('\n');
                }
                report.push_str("\nRefer to https://redis.io/topics/latency for more information.");
                Some(resp_bulk_str(&report))
            }
        }

        "HELP" => Some(resp_array(
            [
                "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "DOCTOR",
                "    Return a human readable latency analysis report.",
                "GRAPH <event>",
                "    Return an ASCII latency graph for the <event> class.",
                "HISTORY <event>",
                "    Return time-latency samples for the <event> class.",
                "LATEST",
                "    Return the latest latency samples for all events.",
                "RESET [<event> ...]",
                "    Reset latency data of one or more <event> classes.",
                "    (default: reset all data for all event classes)",
                "HISTOGRAM [COMMAND ...]",
                "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
                "    If no commands are specified then all histograms are replied.",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|line| resp_simple(line))
            .collect(),
        )),

        _ => Some(resp_error("ERR unknown subcommand for LATENCY")),
    }
}

/// `LATENCY GRAPH`, as Redis's `latencyCommandGenSparkeline` draws it: a
/// header, a rule of 80 dashes, and the samples as a sparkline of four rows
/// (`sparkline.c`, filled), each labelled with how long ago it was, read
/// downwards. Past 80 samples the graph goes on below. The "all time high"
/// is the highest of the samples kept.
fn graph(event: &str, samples: &[crate::latency::LatencySample], now: u64) -> String {
    const COLUMNS: usize = 80;
    const ROWS: usize = 4;
    let values: Vec<u32> = samples.iter().map(|s| s.latency_ms).collect();
    let max = values.iter().copied().max().unwrap_or(0);
    let min = values.iter().copied().min().unwrap_or(0);
    let labels: Vec<String> = samples
        .iter()
        .map(|s| {
            let ago = now.saturating_sub(s.timestamp);
            match ago {
                0..60 => format!("{ago}s"),
                60..3600 => format!("{}m", ago / 60),
                3600..86400 => format!("{}h", ago / 3600),
                _ => format!("{}d", ago / 86400),
            }
        })
        .collect();

    let mut out = format!("{event} - high {max} ms, low {min} ms (all time high {max} ms)\n");
    out.push_str(&"-".repeat(COLUMNS));
    out.push('\n');
    let (lo, hi) = (min as f64, max as f64);
    for (n, start) in (0..values.len()).step_by(COLUMNS).enumerate() {
        if n != 0 {
            out.push('\n');
        }
        let end = (start + COLUMNS).min(values.len());
        sparkline(
            &mut out,
            &values[start..end],
            &labels[start..end],
            lo,
            hi,
            ROWS,
        );
    }
    out
}

/// Redis's `sparklineRenderRange`, with `SPARKLINE_FILL`.
fn sparkline(out: &mut String, values: &[u32], labels: &[String], lo: f64, hi: f64, rows: usize) {
    const CHARSET_FILL: &[u8] = b"_o#";
    let steps = (CHARSET_FILL.len() * rows) as i64;
    let relmax = if hi - lo == 0.0 { 1.0 } else { hi - lo };
    let label_margin_top = 1;
    let mut row = 0;
    loop {
        let mut chars = vec![b' '; values.len()];
        let mut more = false;
        for (j, &value) in values.iter().enumerate() {
            if row < rows {
                let step = (((value as f64 - lo) * steps as f64) / relmax) as i64;
                let step = step.clamp(0, steps - 1);
                let at = step - ((rows - row - 1) * CHARSET_FILL.len()) as i64;
                more = true;
                if (0..CHARSET_FILL.len() as i64).contains(&at) {
                    chars[j] = CHARSET_FILL[at as usize];
                } else if at >= CHARSET_FILL.len() as i64 {
                    chars[j] = b'|';
                }
            } else {
                if row - rows < label_margin_top {
                    more = true;
                    break;
                }
                let label = labels[j].as_bytes();
                let at = row - rows - label_margin_top;
                if at < label.len() {
                    more = true;
                    chars[j] = label[at];
                }
            }
        }
        if !more {
            break;
        }
        row += 1;
        out.push_str(std::str::from_utf8(&chars).unwrap_or_default());
        out.push('\n');
    }
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod graph_tests {
    use super::graph;
    use crate::latency::LatencySample;

    #[test]
    fn the_graph_is_redis_s() {
        let samples = [
            LatencySample {
                timestamp: 110,
                latency_ms: 10,
            },
            LatencySample {
                timestamp: 130,
                latency_ms: 300,
            },
            LatencySample {
                timestamp: 150,
                latency_ms: 150,
            },
        ];
        let g = graph("expire-cycle", &samples, 160);
        let mut lines = g.lines();
        assert_eq!(
            lines.next(),
            Some("expire-cycle - high 300 ms, low 10 ms (all time high 300 ms)")
        );
        assert_eq!(lines.next(), Some("-".repeat(80).as_str()));
        // Four rows of the sparkline, a blank line, then the labels read
        // downwards: 50s, 30s and 10s ago.
        let rest: Vec<&str> = lines.collect();
        assert_eq!(
            rest,
            [" # ", " | ", " |#", "_||", "   ", "531", "000", "sss"]
        );
    }
}
