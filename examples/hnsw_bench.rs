//! The HNSW graph on its own, without a server or a client in the way: build
//! it from a vector-db-benchmark dataset of the "tar" kind (`vectors.npy`
//! and `tests.jsonl`, such as dbpedia-openai-100K-1536-angular) with several
//! threads inserting at once, as the server's workers do, then search it.
//!
//! ```text
//! cargo run --release --example hnsw_bench -- DIR [--threads 8] [--limit N]
//!     [--m 16] [--efc 128] [--ef 64,128,256] [--k 10] [--clients 1,8]
//!     [--queries 2000] [--metric cosine|l2|ip]
//! ```
//!
//! Prints the build time, the bytes the graph holds per vector (from the
//! counting allocator), and for each ef and number of searching threads the
//! queries a second and the recall at k against the dataset's ground truth.

use std::io::{BufRead, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use kividb::vector::{DistanceMetric, HnswIndex, hnsw_insert, knn_hnsw};

struct Args {
    dir: std::path::PathBuf,
    threads: usize,
    limit: usize,
    m: usize,
    efc: usize,
    efs: Vec<usize>,
    k: usize,
    clients: Vec<usize>,
    queries: usize,
    metric: DistanceMetric,
}

fn list(s: &str) -> Vec<usize> {
    s.split(',').map(|x| x.parse().expect("a number")).collect()
}

fn args() -> Args {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        dir: it.next().expect("the dataset directory").into(),
        threads: 8,
        limit: usize::MAX,
        m: 16,
        efc: 128,
        efs: vec![64, 128, 256],
        k: 10,
        clients: vec![1, 8],
        queries: 2000,
        metric: DistanceMetric::Cosine,
    };
    while let Some(flag) = it.next() {
        let v = it.next().expect("a value after the flag");
        match flag.as_str() {
            "--threads" => a.threads = v.parse().unwrap(),
            "--limit" => a.limit = v.parse().unwrap(),
            "--m" => a.m = v.parse().unwrap(),
            "--efc" => a.efc = v.parse().unwrap(),
            "--ef" => a.efs = list(&v),
            "--k" => a.k = v.parse().unwrap(),
            "--clients" => a.clients = list(&v),
            "--queries" => a.queries = v.parse().unwrap(),
            "--metric" => a.metric = DistanceMetric::metric_from_str(&v).expect("cosine, l2 or ip"),
            other => panic!("unknown flag {other}"),
        }
    }
    a
}

/// A float32 matrix from a `.npy` file: (rows, columns, values).
fn read_npy(path: &std::path::Path) -> (usize, usize, Vec<f32>) {
    let mut f = std::io::BufReader::new(std::fs::File::open(path).expect("vectors.npy"));
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic).unwrap();
    assert_eq!(&magic[..6], b"\x93NUMPY", "not a .npy file");
    let header_len = if magic[6] == 1 {
        let mut b = [0u8; 2];
        f.read_exact(&mut b).unwrap();
        u16::from_le_bytes(b) as usize
    } else {
        let mut b = [0u8; 4];
        f.read_exact(&mut b).unwrap();
        u32::from_le_bytes(b) as usize
    };
    let mut header = vec![0u8; header_len];
    f.read_exact(&mut header).unwrap();
    let header = String::from_utf8(header).unwrap();
    assert!(header.contains("'<f4'"), "float32 only: {header}");
    assert!(header.contains("'fortran_order': False"), "C order only");
    let shape = &header[header.find("'shape': (").unwrap() + 10..];
    let shape = &shape[..shape.find(')').unwrap()];
    let dims: Vec<usize> = shape
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().parse().unwrap())
        .collect();
    let (rows, cols) = (dims[0], dims[1]);
    let mut raw = vec![0u8; rows * cols * 4];
    f.read_exact(&mut raw).unwrap();
    let values = raw
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_le_bytes(*c))
        .collect();
    (rows, cols, values)
}

/// The queries of `tests.jsonl`, with the ids of their true neighbours.
fn read_tests(path: &std::path::Path, limit: usize) -> Vec<(Vec<f32>, Vec<usize>)> {
    let f = std::io::BufReader::new(std::fs::File::open(path).expect("tests.jsonl"));
    let mut out = Vec::new();
    for line in f.lines().take(limit) {
        let v: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let q = v["query"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap() as f32)
            .collect();
        let ids = v["closest_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_u64().unwrap() as usize)
            .collect();
        out.push((q, ids));
    }
    out
}

fn main() {
    let a = args();
    let (rows, dim, values) = read_npy(&a.dir.join("vectors.npy"));
    let n = rows.min(a.limit);
    let tests = read_tests(&a.dir.join("tests.jsonl"), a.queries);
    println!(
        "dataset {} vectors x {dim}, {} queries; M {} efc {} threads {}",
        n,
        tests.len(),
        a.m,
        a.efc,
        a.threads
    );

    // ── Build ────────────────────────────────────────────────────────────
    let index = Arc::new(HnswIndex::new(a.m, a.efc));
    let before = kividb::memory_use::allocated();
    let next = AtomicUsize::new(0);
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..a.threads {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    let v = values[i * dim..(i + 1) * dim].to_vec();
                    hnsw_insert(&index, i.to_string(), v, &a.metric);
                }
            });
        }
    });
    let build = start.elapsed().as_secs_f64();
    let bytes = kividb::memory_use::allocated().saturating_sub(before);
    println!(
        "build {build:.2} s, {:.0} inserts/s, {:.0} bytes/vector ({} of them the vector)",
        n as f64 / build,
        bytes as f64 / n as f64,
        dim * 4
    );

    // ── Search ───────────────────────────────────────────────────────────
    // Ground truth past the subset built is left out.
    let truth: Vec<Vec<usize>> = tests
        .iter()
        .map(|(_, ids)| ids.iter().copied().filter(|&i| i < n).take(a.k).collect())
        .collect();
    for &ef in &a.efs {
        for &clients in &a.clients {
            let next = AtomicUsize::new(0);
            let hits = AtomicUsize::new(0);
            let wanted = AtomicUsize::new(0);
            let start = Instant::now();
            // Each thread answers the queries in turn until all are done;
            // with one client the run is the queries one after the other.
            let rounds = if clients > 1 { 4 } else { 1 };
            let total = tests.len() * rounds;
            std::thread::scope(|s| {
                for _ in 0..clients {
                    s.spawn(|| {
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= total {
                                break;
                            }
                            let q = i % tests.len();
                            let found = knn_hnsw(&index, &tests[q].0, a.k, ef, &a.metric);
                            if i < tests.len() {
                                let t = &truth[q];
                                let got = found
                                    .iter()
                                    .filter(|r| {
                                        r.key.parse::<usize>().is_ok_and(|id| t.contains(&id))
                                    })
                                    .count();
                                hits.fetch_add(got, Ordering::Relaxed);
                                wanted.fetch_add(t.len(), Ordering::Relaxed);
                            }
                        }
                    });
                }
            });
            let secs = start.elapsed().as_secs_f64();
            println!(
                "ef {ef:4} clients {clients:2}: {:8.1} q/s  {:7.3} ms/query  recall@{} {:.4}",
                total as f64 / secs,
                secs * 1000.0 * clients as f64 / total as f64,
                a.k,
                hits.load(Ordering::Relaxed) as f64 / wanted.load(Ordering::Relaxed).max(1) as f64
            );
        }
    }
}
