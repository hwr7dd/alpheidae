//! Multi-node demo: dynamic Raft membership (AddPeer) + mid-query workers.
//!
//!   A. Start 2-node catalog → elect → commit history
//!      Start a 3rd process with empty peers → leader AddPeer → snapshot catch-up
//!      Verify late node has *historical* keys (snapshot install)
//!   B. Query workers join mid-query at staggered delays

use blitz_cluster::ClusteredTable;
use blitz_core::{Block, Column, BLOCK_ROWS};
use blitz_exec::{run_ramped, worker_main, Timeline};
use blitz_meta::{MetaClient, MetaNode, PutResult};
use blitz_sql::parse;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn banner(s: &str) {
    println!("\n=== {s} {}", "=".repeat(74usize.saturating_sub(s.len())));
}

fn gen_table(rows: usize) -> Vec<Block> {
    let mut state = 0xC0FFEE_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut blocks = vec![];
    let mut done = 0;
    while done < rows {
        let n = BLOCK_ROWS.min(rows - done);
        let mut c0 = Vec::with_capacity(n);
        let mut c1 = Vec::with_capacity(n);
        let mut c2 = Vec::with_capacity(n);
        for _ in 0..n {
            c0.push((next() % 100_000_000) as i64);
            c1.push((next() % 10_000) as i64);
            c2.push((next() % 16) as i64);
        }
        blocks.push(Block {
            rows: n,
            columns: vec![Column::I64(c0), Column::I64(c1), Column::I64(c2)],
        });
        done += n;
    }
    blocks
}

fn main() {
    println!("Blitz Raft membership demo — AddPeer + snapshot catch-up\n");

    banner("A. META — grow cluster from 2 → 3 via Raft AddPeer");

    // Initial membership: only two nodes know each other.
    let n0 = MetaNode::start(
        "127.0.0.1:7601",
        vec!["127.0.0.1:7602".into()],
        false,
    );
    let n1 = MetaNode::start(
        "127.0.0.1:7602",
        vec!["127.0.0.1:7601".into()],
        false,
    );

    let early = ["127.0.0.1:7601", "127.0.0.1:7602"];
    let leader = MetaClient::wait_for_leader(&early, 15000).expect("2-node election");
    println!("2-node cluster; Raft leader = {leader}");
    println!("leader peers = {:?}", {
        if leader == "127.0.0.1:7601" {
            n0.peers()
        } else {
            n1.peers()
        }
    });

    let client = MetaClient::new(&leader);
    match client.put("history.key", "before-add") {
        PutResult::Committed(i) => println!("PUT history.key=before-add -> log[{i}]"),
        other => panic!("PUT failed: {other:?}"),
    }

    println!("\nstarting meta-2 as joiner (not yet a member, will not self-elect) ...");
    let n2 = MetaNode::start_joiner("127.0.0.1:7603");
    thread::sleep(Duration::from_millis(100));

    println!("leader AddPeer(127.0.0.1:7603) — Raft config change + snapshot ...");
    let mut add_result = client.add_peer("127.0.0.1:7603");
    if add_result == PutResult::NotLeader || add_result == PutResult::Unreachable {
        if let Some(l2) = MetaClient::wait_for_leader(&early, 3000) {
            add_result = MetaClient::new(&l2).add_peer("127.0.0.1:7603");
        }
    }
    match add_result {
        PutResult::Committed(i) => println!("AddPeer committed at log[{i}]"),
        other => panic!("AddPeer failed: {other:?}"),
    }
    // Wait until joiner has history AND leader lists it as a peer.
    for _ in 0..30 {
        thread::sleep(Duration::from_millis(50));
        let has_hist = MetaClient::new("127.0.0.1:7603")
            .get("history.key")
            .as_deref()
            == Some("before-add");
        let peers = if leader == "127.0.0.1:7601" {
            n0.peers()
        } else {
            n1.peers()
        };
        if has_hist && peers.iter().any(|p| p == "127.0.0.1:7603") {
            break;
        }
    }

    let st = MetaClient::status("127.0.0.1:7603");
    println!("meta-2 status after AddPeer: {:?}", st);

    let late = MetaClient::new("127.0.0.1:7603");
    let hist = late.get("history.key");
    println!(
        "late node GET history.key = {:?} {}",
        hist,
        if hist.as_deref() == Some("before-add") {
            "OK — snapshot catch-up restored history"
        } else {
            "FAIL — expected snapshot install"
        }
    );

    match client.put("after.add", "v2") {
        PutResult::Committed(i) => println!("PUT after.add=v2 -> log[{i}] (3-node quorum)"),
        other => {
            // Leader may have moved; retry.
            if let Some(l2) = MetaClient::wait_for_leader(&["127.0.0.1:7601", "127.0.0.1:7602", "127.0.0.1:7603"], 3000) {
                match MetaClient::new(&l2).put("after.add", "v2") {
                    PutResult::Committed(i) => println!("PUT after.add=v2 -> log[{i}] via {l2}"),
                    o => println!("PUT after.add failed: {o:?}"),
                }
            } else {
                println!("PUT after.add failed: {other:?}");
            }
        }
    }
    thread::sleep(Duration::from_millis(300));
    let mut after = late.get("after.add");
    for _ in 0..10 {
        if after.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
        after = late.get("after.add");
    }
    println!("late node GET after.add = {:?}", after);

    println!("\nRemovePeer demo: remove meta-2 from membership ...");
    let leader_now = MetaClient::wait_for_leader(&["127.0.0.1:7601", "127.0.0.1:7602"], 3000)
        .unwrap_or(leader);
    match MetaClient::new(&leader_now).remove_peer("127.0.0.1:7603") {
        PutResult::Committed(i) => println!("RemovePeer committed at log[{i}]"),
        other => println!("RemovePeer: {other:?}"),
    }
    println!(
        "leader peers now = {:?}",
        if leader_now == "127.0.0.1:7601" {
            n0.peers()
        } else {
            n1.peers()
        }
    );
    let _ = (n0, n1, n2);

    banner("B. QUERY — workers join mid-query");

    let table = Arc::new(ClusteredTable::from_blocks(gen_table(8_000_000)));
    let sql = "SELECT SUM(c1) FROM t GROUP BY c2";
    let q = parse(sql).expect("parse");
    println!("query: {sql}");
    println!("workers join at +5ms, +12ms, +20ms, +35ms\n");

    const ADDR: &str = "127.0.0.1:7611";
    let joins_ms = [5u64, 12, 20, 35];
    for delay in joins_ms {
        let st = table.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(delay));
            loop {
                match worker_main(ADDR, Some(st.clone())) {
                    Ok(_) => break,
                    Err(_) => thread::sleep(Duration::from_millis(1)),
                }
            }
        });
    }

    let tl = Arc::new(Timeline::new());
    tl.mark("query arrived — coordinator executing, workers still offline");
    let t0 = Instant::now();
    let rep = run_ramped(table, q, 1, ADDR, joins_ms.len(), false, tl);
    let wall = t0.elapsed().as_secs_f64() * 1e3;
    for (ms, ev) in &rep.timeline {
        println!("  [{:>8.3} ms] {ev}", ms);
    }
    println!(
        "\n{} morsels | wall {:.2} ms | {} groups",
        rep.morsels_executed,
        wall,
        rep.result.groups.len()
    );

    banner("SUMMARY");
    println!("  Raft AddPeer:     grows membership under old majority, then new config");
    println!("  Snapshot install: late peer receives full history (map + log)");
    println!("  Raft RemovePeer:  shrinks membership via config log entry");
    println!("  Query late join:  workers steal remaining morsels mid-query");
}
