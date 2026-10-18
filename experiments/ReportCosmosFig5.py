#!/usr/bin/env python3
"""Summarise the 34 W Cosmos Fig-5 grid and check it against the simulator.

ReportTPCC.py cannot be used here: its acceptance pass compares the measured
remote-read latency against g_sim_rd_latency_us, which is 0 on a real store, so
every Cosmos row would be reported as a failure. This script keeps the checks
that still mean something against Cosmos and drops the ones that do not.

  ReportCosmosFig5.py [output/exp_tpcc_cosmos_34w_std.txt]

Reference curves (all 34 W, o_cap=15000, standard mix):
  output/sim34w_final_fig5.txt -- both systems, on the page warm-up
  treatment that the Cosmos grid reproduces. NOT sim34w_oc15k_fig5.txt,
  whose PGBUF rows still carry the warm-up asymmetry.

Absolute throughput is not comparable across the two: the simulator has no RU
ceiling and no network jitter. The comparable quantities are the SHAPE of each
curve and the Nemo/B+Tree ratio at equal buffer.
"""
import json
import os
import sys

DATA_SZ = 4.93e9  # 34 W x 145 MB per warehouse
TXN_TYPES = ["neworder", "payment", "orderstatus", "delivery", "stocklevel"]
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

FNAME = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    REPO, "output", "exp_tpcc_cosmos_34w_std.txt")


def load(path):
    rows = []
    if not os.path.exists(path):
        return rows
    for ln, line in enumerate(open(path), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            print("WARN: %s line %d unparsable: %s" % (path, ln, e))
    return rows


def sysname(r):
    return "btree-ri" if r["g_buf_type"] == "PGBUF" else "nemo"


def digest(r):
    txn = max(r["txn_cnt_"], 1)
    pg = r["g_buf_type"] == "PGBUF"
    return {
        "system": sysname(r),
        "ratio": r["g_total_buf_sz"] / DATA_SZ,
        "warm": r["warmup_time_"],
        "run": r["runtime_"],
        "tput": r["throughput_txn_per_sec"],
        "miss_per_txn": r["misses_"] / txn,
        # PGBUF's tuple_hit_rate_ is a record-level counter and goes negative
        # under a page buffer; the page
        # column is read through idx_hit_rate_ instead.
        "hit": r["idx_hit_rate_"] if pg else r["tuple_hit_rate_"],
        "scans_per_txn": r["remote_scans_"] / txn,
        "scan_ms_op": r["avg_remote_scan_time__per_op (us)"] / 1000.0,
        "rd_ms_op": (r["remote_rd_time_ms"] / r["remote_rds_"]
                     if r["remote_rds_"] else 0.0),
        "rds_per_txn": r["remote_rds_"] / txn,
        "p50_ms": r["txn_latency_perc50_us"] / 1000.0,
        "p99_ms": r["txn_latency_perc99_us"] / 1000.0,
        "no_abort": r["neworder_abort_cnt_"],
        "io_share": (r["remote_io_time_ms"] / r["thd_runtime_ms"]
                     if r.get("thd_runtime_ms") else 0.0),
        "raw": r,
    }


def table(rows, title):
    print("--- %s ---" % title)
    print("%-9s %5s %6s %6s %10s %9s %8s %8s %10s %9s %9s %7s"
          % ("system", "buf%", "warm", "run", "txn/s", "miss/txn", "hit",
             "scan/tx", "scan ms/op", "rd ms/op", "p99 ms", "NOabrt"))
    for x in sorted(rows, key=lambda x: (x["system"], x["ratio"])):
        print("%-9s %4.0f%% %5ds %5ds %10.1f %9.2f %8.3f %8.3f %10.1f %9.2f "
              "%9.1f %7d"
              % (x["system"], x["ratio"] * 100, x["warm"], x["run"], x["tput"],
                 x["miss_per_txn"], x["hit"], x["scans_per_txn"],
                 x["scan_ms_op"], x["rd_ms_op"], x["p99_ms"], x["no_abort"]))
    print()


def by_ratio(rows, system):
    return {round(x["ratio"], 2): x
            for x in rows if x["system"] == system}


def main():
    cosmos = [digest(r) for r in load(FNAME)]
    if not cosmos:
        print("no runs in %s" % FNAME)
        return 1

    # ---- simulator reference -------------------------------------------
    # output/sim34w_final_fig5.txt is the consolidated official baseline
    # (commit c967e96): OBJBUF rows unchanged, PGBUF rows on the warm-up
    # page warm-up treatment. Prefer it; fall back to
    # the per-experiment files it was assembled from.
    final = load(os.path.join(REPO, "output", "sim34w_final_fig5.txt"))
    if final:
        sim_nemo = [digest(r) for r in final if r["g_buf_type"] == "OBJBUF"]
        sim_bt = [digest(r) for r in final if r["g_buf_type"] == "PGBUF"]
    else:
        sim_nemo = [digest(r) for r in load(os.path.join(
            REPO, "output", "sim34w_oc15k_fig5.txt"))
            if r["g_buf_type"] == "OBJBUF"]
        sim_bt = []
        for f in ("sim34w_pgbuf_warmfree_grid.txt",
                  "sim34w_pgbuf_warmfree_rep.txt", "sim34w_pgbuf_1pct.txt"):
            for r in load(os.path.join(REPO, "output", f)):
                # the 1 % file also holds the latency-charged control; keep
                # the treated row only
                if f.endswith("1pct.txt") and \
                        r.get("g_sim_warmup_no_latency") != "True":
                    continue
                sim_bt.append(digest(r))
    sim_bt_by_ratio = {}
    for x in sim_bt:
        sim_bt_by_ratio[round(x["ratio"], 2)] = x

    print("=" * 100)
    print("TPC-C 34 W, o_cap=15000, standard mix 45/43/4/4/4, 16 workers")
    print("Cosmos: %s" % FNAME)
    print("=" * 100)
    table(cosmos, "COSMOS (measured)")

    sim_rows = sim_nemo + list(sim_bt_by_ratio.values())
    if sim_rows:
        table(sim_rows, "SIMULATOR (reference; no RU ceiling, not comparable "
                        "in absolute terms)")

    # ---- speedup comparison --------------------------------------------
    cn, cb = by_ratio(cosmos, "nemo"), by_ratio(cosmos, "btree-ri")
    sn = {round(x["ratio"], 2): x for x in sim_nemo}
    print("--- Nemo / B+Tree speedup ---")
    print("%6s %12s %12s %10s %12s %10s"
          % ("buf%", "Cosmos Nemo", "Cosmos B+T", "Cosmos x", "sim x", "delta"))
    for k in sorted(set(cn) & set(cb)):
        cx = cn[k]["tput"] / cb[k]["tput"] if cb[k]["tput"] else float("nan")
        sx = (sn[k]["tput"] / sim_bt_by_ratio[k]["tput"]
              if k in sn and k in sim_bt_by_ratio and sim_bt_by_ratio[k]["tput"]
              else float("nan"))
        print("%5.0f%% %12.1f %12.1f %9.2fx %11.2fx %+9.0f%%"
              % (k * 100, cn[k]["tput"], cb[k]["tput"], cx, sx,
                 (cx / sx - 1) * 100 if sx == sx else float("nan")))
    print()

    # ---- is each column actually I/O-bound? ------------------------------
    # The figure claims an I/O effect, so the first question about any Cosmos
    # point is whether remote I/O is what the workers were doing. If a column
    # spends only a few percent of worker time on remote I/O, its throughput is
    # set by something else and the measured speedup understates the I/O one.
    # io_ms_per_txn is the quantity the figure is really about: how much remote
    # work one transaction costs. The last column projects the speedup a purely
    # I/O-bound run would have shown at this concurrency.
    print("--- I/O accounting (is the measured throughput an I/O measurement?) ---")
    print("%-9s %5s %10s %9s %11s %12s %14s"
          % ("system", "buf%", "io share", "rds/txn", "ms per rd",
             "io ms/txn", "io-bound txn/s"))
    for x in sorted(cosmos, key=lambda x: (x["system"], x["ratio"])):
        io_ms = x["rds_per_txn"] * x["rd_ms_op"] + \
            x["scans_per_txn"] * x["scan_ms_op"]
        x["io_ms_per_txn"] = io_ms
        workers = x["raw"].get("g_num_worker_threads", 16)
        print("%-9s %4.0f%% %9.0f%% %9.2f %11.2f %12.2f %14.0f"
              % (x["system"], x["ratio"] * 100, x["io_share"] * 100,
                 x["rds_per_txn"], x["rd_ms_op"], io_ms,
                 workers / (io_ms / 1000.0) if io_ms else float("nan")))
    print()
    print("--- speedup implied by measured per-transaction I/O cost ---")
    for k in sorted(set(cn) & set(cb)):
        a, b = cn[k].get("io_ms_per_txn"), cb[k].get("io_ms_per_txn")
        if a and b:
            print("  %4.0f%%  Nemo %.2f ms/txn vs B+Tree %.2f ms/txn -> %.2fx "
                  "(measured throughput ratio %.2fx)"
                  % (k * 100, a, b, b / a, cn[k]["tput"] / cb[k]["tput"]))
    print()

    # ---- acceptance ------------------------------------------------------
    problems, notes = [], []
    for x in cosmos:
        r = x["raw"]
        tag = "%s @%.0f%%" % (x["system"], x["ratio"] * 100)
        commits = {t: r["%s_commit_cnt_" % t] for t in TXN_TYPES}
        if sum(commits.values()) != r["txn_cnt_"]:
            problems.append("%s: per-type commit sum %d != txn_cnt_ %d"
                            % (tag, sum(commits.values()), r["txn_cnt_"]))
        if commits["neworder"]:
            rb = r["neworder_rollback_cnt_"] / commits["neworder"]
            if not 0.005 <= rb <= 0.02:
                problems.append("%s: NewOrder rollback %.3f (expect ~0.01)"
                                % (tag, rb))
        # NewOrder aborts are NOT by themselves slot exhaustion: TPCCTxn.cpp:48
        # returns ABORT when a district runs out of order slots, but ordinary
        # concurrency conflicts land in the same counter (btree-ri @1 % on
        # Cosmos: 482 aborts with the slot diagnostic never firing). Slot
        # exhaustion is identified by "order slots exhausted" in the driver
        # log; a high abort RATE is reported here as contention.
        attempts = commits["neworder"] + r["neworder_abort_cnt_"]
        if attempts and r["neworder_abort_cnt_"] / attempts > 0.05:
            notes.append("%s: NewOrder abort rate %.0f%% (%d/%d) -- contention, "
                         "check the driver log for 'order slots exhausted'"
                         % (tag, 100.0 * r["neworder_abort_cnt_"] / attempts,
                            r["neworder_abort_cnt_"], attempts))
        # warm-up adequacy: the page column must reach the simulator's
        # steady-state miss rate, else it measured a transient (this is the
        # whole point of the sized warm-up)
        if x["system"] == "btree-ri":
            ref = sim_bt_by_ratio.get(round(x["ratio"], 2))
            if ref and ref["miss_per_txn"] > 0 and \
                    x["miss_per_txn"] > ref["miss_per_txn"] * 1.5:
                problems.append(
                    "%s: miss/txn %.2f vs simulator steady state %.2f -- "
                    "warm-up too short, re-run this point"
                    % (tag, x["miss_per_txn"], ref["miss_per_txn"]))

    for name in ("nemo", "btree-ri"):
        xs = sorted([x for x in cosmos if x["system"] == name],
                    key=lambda x: x["ratio"])
        for a, b in zip(xs, xs[1:]):
            if b["tput"] < a["tput"] * 0.9:
                notes.append("%s: throughput %.1f -> %.1f from %.0f%% to %.0f%%"
                             % (name, a["tput"], b["tput"],
                                a["ratio"] * 100, b["ratio"] * 100))

    if notes:
        print("NON-MONOTONIC POINTS (%d) -- expected at btree-ri 100%%:"
              % len(notes))
        for n in notes:
            print("  - " + n)
        print()
    if problems:
        print("ACCEPTANCE PROBLEMS (%d):" % len(problems))
        for p in problems:
            print("  - " + p)
        return 1
    print("All acceptance checks passed (%d runs)." % len(cosmos))
    return 0


if __name__ == "__main__":
    sys.exit(main())
