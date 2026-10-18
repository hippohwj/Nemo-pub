#!/usr/bin/env python3
# Summarize a TPC-C sweep output file (one JSON line per run) into the
# report table: system x buffer ratio -> throughput, p50/p99, abort rate,
# remote reads/txn. Also validates the acceptance criteria
# (per-type commit sum == txn_cnt_, ~1%
# NewOrder rollback, injected remote-read latency, RI >= EI, monotonic
# throughput).
#
# Usage: python3 ReportTPCC.py [output/exp_tpcc_sim_10w_varybuf.txt]
import json
import sys

FNAME = sys.argv[1] if len(sys.argv) > 1 else "output/exp_tpcc_sim_10w_varybuf.txt"
TXN_TYPES = ["neworder", "payment", "orderstatus", "delivery", "stocklevel"]

# buffer ratios are defined against the logical dataset size (~145 MB per
# warehouse); infer the warehouse count from the file name (exp_tpcc_*_<N>w_*)
import re
_m = re.search(r"_(\d+)w_", FNAME)
NUM_WAREHOUSES = int(_m.group(1)) if _m else 10


def system_name(run):
    buf = run["g_buf_type"]
    if buf == "OBJBUF":
        return ("nemo-blind" if run.get("g_tpcc_blind_inserts") == "True"
                else "nemo")
    if buf == "PGBUF":
        return "btree-ri" if run["g_retain_idx_page"] == "True" else "btree-ei"
    return buf


def load_runs(fname):
    runs = []
    for ln, line in enumerate(open(fname), 1):
        line = line.strip()
        if not line:
            continue
        try:
            runs.append(json.loads(line))
        except json.JSONDecodeError as e:
            print("WARN: line %d unparsable: %s" % (ln, e))
    return runs


def main():
    runs = load_runs(FNAME)
    rows = []
    problems = []
    for r in runs:
        sysname = system_name(r)
        ratio = r["g_total_buf_sz"] / (NUM_WAREHOUSES * 145 * 1000 * 1000.0)
        commits = {t: r["%s_commit_cnt_" % t] for t in TXN_TYPES}
        aborts = {t: r["%s_abort_cnt_" % t] for t in TXN_TYPES}
        txn_cnt = r["txn_cnt_"]
        tag = "%s @%d%%" % (sysname, round(ratio * 100))
        if sum(commits.values()) != txn_cnt:
            problems.append("%s: per-type commit sum %d != txn_cnt_ %d"
                            % (tag, sum(commits.values()), txn_cnt))
        rollback = (r["neworder_rollback_cnt_"] / commits["neworder"]
                    if commits["neworder"] else 0.0)
        if not 0.005 <= rollback <= 0.02:
            problems.append("%s: NewOrder rollback rate %.3f (expect ~0.01)"
                            % (tag, rollback))
        rd_ms = (r["remote_rd_time_ms"] / r["remote_rds_"]
                 if r["remote_rds_"] else 0.0)
        if abs(rd_ms - r["g_sim_rd_latency_us"] / 1000.0) > 0.1:
            problems.append("%s: measured remote read %.3f ms vs %.1f ms knob"
                            % (tag, rd_ms, r["g_sim_rd_latency_us"] / 1000.0))
        rows.append({
            "system": sysname, "ratio": ratio,
            "tput": r["throughput_txn_per_sec"],
            "p50_ms": r["txn_latency_perc50_us"] / 1000.0,
            "p99_ms": r["txn_latency_perc99_us"] / 1000.0,
            "abort_per_commit": r["abort_cnt_"] / txn_cnt if txn_cnt else 0.0,
            "rollback": rollback,
            "rds_per_txn": r["remote_rds_"] / txn_cnt if txn_cnt else 0.0,
            "rd_ms": rd_ms,
        })

    rows.sort(key=lambda x: (x["system"], x["ratio"]))
    hdr = ("system", "buf%", "tput(txn/s)", "p50(ms)", "p99(ms)",
           "abort/commit", "NOrollback", "rds/txn", "rd(ms)")
    print(("%-10s %5s %12s %9s %10s %13s %11s %8s %7s") % hdr)
    for x in rows:
        print("%-10s %5.0f %12.1f %9.1f %10.1f %13.2f %11.3f %8.1f %7.2f"
              % (x["system"], x["ratio"] * 100, x["tput"], x["p50_ms"],
                 x["p99_ms"], x["abort_per_commit"], x["rollback"],
                 x["rds_per_txn"], x["rd_ms"]))

    # monotonicity (10% noise tolerance) and RI >= EI at equal ratio
    by_sys = {}
    for x in rows:
        by_sys.setdefault(x["system"], []).append(x)
    for name, xs in by_sys.items():
        for a, b in zip(xs, xs[1:]):
            if b["tput"] < a["tput"] * 0.9:
                problems.append("%s: throughput drops %.1f -> %.1f from ratio "
                                "%.0f%% to %.0f%%" % (name, a["tput"], b["tput"],
                                a["ratio"] * 100, b["ratio"] * 100))
    if "btree-ri" in by_sys and "btree-ei" in by_sys:
        ei = {round(x["ratio"], 3): x for x in by_sys["btree-ei"]}
        for x in by_sys["btree-ri"]:
            other = ei.get(round(x["ratio"], 3))
            if other and x["tput"] < other["tput"] * 0.9:
                problems.append("btree-ri %.1f < btree-ei %.1f at ratio %.0f%%"
                                % (x["tput"], other["tput"], x["ratio"] * 100))

    print()
    if problems:
        print("ACCEPTANCE PROBLEMS (%d):" % len(problems))
        for p in problems:
            print("  - " + p)
        sys.exit(1)
    print("All acceptance checks passed (%d runs)." % len(rows))


if __name__ == "__main__":
    main()
