#!/usr/bin/env python3
# "Speedup remaining" report for the sim-store experiment:
# side-by-side Nemo-vs-best-baseline
# speedup on Cosmos DB (the paper's default, throttled) and on the simulated
# store (stable uncontended latency, no request cap), YCSB point workload,
# theta=0.9, 16 workers, 60 s measured.
#
#   python3 ReportSimYCSB.py            # from the experiments/ directory
import json, os

OUT = os.path.join(os.path.dirname(__file__), "../output")
WORKING_SET = 12000000000

# ExpPointVaryBufForALLSim.py writes a different file per mode: in-process
# -> exp_point_buf_for_all_sim.txt, networked (NEMO_SIMNET_ADDR set) ->
# exp_point_buf_for_all_sim_net.txt. Export the same variable for this
# script so it reads the grid that was just measured, or name a file
# outright with NEMO_SIM_FILE.
SIM_FILE = os.environ.get(
    "NEMO_SIM_FILE",
    "exp_point_buf_for_all_sim_net.txt" if os.environ.get("NEMO_SIMNET_ADDR")
    else "exp_point_buf_for_all_sim.txt")
# D6 convention: two-tier read pricing is the citation
# convention — read-only page/2-Tree baselines come from the @4200 probe
# file (8 KB page reads billed at their own uncontended floor); the @2800
# page-baseline rows are deprecated for citation. nemo stays @2800 (record
# reads; no "nemo@4200" exists). NEMO_SINGLE_TIER=1 reproduces the old
# single-tier table.
PAGELAT_FILE = "ycsb-sim/20260809/exp_point_buf_pagelat_sim.txt"
COSMOS = {  # (system, read_perc) -> file
    ("nemo", 1):        "exp_point_10g_tuple_varybuf.txt",
    ("nemo", 0.8):      "exp_point_10g_tuple_varybuf_rw.txt",
    ("btree-ri", 1):    "exp_point_10g_page_varybuf.txt",
    ("btree-ri", 0.8):  "exp_point_10g_page_varybuf_rw.txt",
    ("btree-ei", 1):    "exp_point_10g_page_varybuf_idx_evict.txt",
    ("btree-ei", 0.8):  "exp_point_10g_page_varybuf_idx_evict_rw.txt",
    ("twotree-ri", 1):   "exp_point_10g_twotree_varybuf.txt",
    ("twotree-ri", 0.8): "exp_point_10g_twotree_varybuf_rw.txt",
    ("twotree-ei", 1):   "exp_point_10g_twotree_varybuf_idx_evict.txt",
    ("twotree-ei", 0.8): "exp_point_10g_twotree_varybuf_idx_evict_rw.txt",
}
BASELINES = ["btree-ri", "btree-ei", "twotree-ri", "twotree-ei"]


def sysname(d):
    t = d.get("g_buf_type")
    if t == "OBJBUF":
        return "nemo"
    ri = d.get("g_retain_idx_page") == "True"
    if t == "PGBUF":
        return "btree-ri" if ri else "btree-ei"
    if t == "HYBRBUF":
        return "twotree-ri" if ri else "twotree-ei"
    return t


def parse(path, sim=False):
    runs = {}
    if not os.path.exists(path):
        return runs
    for line in open(path):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        # the sim file accumulated superseded OBJBUF configs (record-CLOCK
        # strawman, evict-at-full validation); only batch eviction is Nemo
        # (paper Algorithm 2). Cosmos-era rows
        # predate the knob entirely, so filter sim rows only.
        if sim and d.get("g_buf_type") == "OBJBUF" \
               and d.get("g_batch_eviction") != "True":
            continue
        ratio = d.get("g_total_buf_sz", 0) / WORKING_SET
        key = (sysname(d), d.get("read_perc_"), round(ratio, 3))
        txn = d.get("txn_cnt_", 0)
        rds = d.get("remote_rds_", 0)
        # keep the last occurrence (reruns append)
        runs[key] = {"txn": txn, "rds_per_txn": rds / txn if txn else None}
    return runs


def main():
    sim_path = os.path.join(OUT, SIM_FILE)
    if not os.path.exists(sim_path):
        raise SystemExit("missing %s; run ExpPointVaryBufForALLSim.py first"
                         % sim_path)
    print("[data] sim grid from %s" % SIM_FILE)
    sim = parse(sim_path, sim=True)
    if os.environ.get("NEMO_SINGLE_TIER") != "1":
        pagelat_path = os.path.join(OUT, PAGELAT_FILE)
        if not os.path.exists(pagelat_path):
            # silently falling back would print the deprecated single-tier
            # numbers under the two-tier heading
            raise SystemExit(
                "missing %s, which the two-tier convention needs; set "
                "NEMO_SINGLE_TIER=1 for the deprecated single-tier table"
                % pagelat_path)
        print("[convention] two-tier (D6): ro baselines @4200 from %s; "
              "rw remains @2800 (out of the narrowed verification scope)"
              % PAGELAT_FILE)
        for (s, rp, r), v in parse(pagelat_path).items():
            if s != "nemo" and rp == 1:
                sim[(s, rp, r)] = v
    cosmos = {}
    for (s, rp), fname in COSMOS.items():
        for (s2, rp2, ratio), v in parse(os.path.join(OUT, fname)).items():
            cosmos[(s, rp, ratio)] = v  # per-system files: trust file identity

    ratios = sorted({k[2] for k in list(sim) + list(cosmos)})
    for rp, mix in [(1, "read-only"), (0.8, "read-write 80/20")]:
        print(f"\n== {mix} (zipf 0.9) ==")
        hdr = (f"{'ratio':>6} | {'cosmos nemo':>11} {'best-bl':>9} "
               f"{'(which)':>11} {'speedup':>8} | {'sim nemo':>9} "
               f"{'best-bl':>9} {'(which)':>11} {'speedup':>8} | {'n.rds/txn':>9}")
        print(hdr)
        print("-" * len(hdr))
        for ratio in ratios:
            cells = []
            for src in (cosmos, sim):
                nemo = src.get(("nemo", rp, ratio))
                bl = [(s, src[(s, rp, ratio)]["txn"]) for s in BASELINES
                      if (s, rp, ratio) in src]
                if nemo and bl:
                    bs, bt = max(bl, key=lambda x: x[1])
                    cells.append((nemo["txn"], bt, bs, nemo["txn"] / bt))
                else:
                    cells.append(None)
            if not any(cells):
                continue
            def fmt(c):
                if not c:
                    return f"{'-':>11} {'-':>9} {'-':>11} {'-':>8}"
                return (f"{c[0]:>11} {c[1]:>9} {c[2]:>11} {c[3]:>7.2f}x")
            nrds = sim.get(("nemo", rp, ratio), {}).get("rds_per_txn")
            print(f"{ratio*100:>5.0f}% | {fmt(cells[0])} | {fmt(cells[1])}"
                  f" | {nrds:>9.1f}" if nrds else
                  f"{ratio*100:>5.0f}% | {fmt(cells[0])} | {fmt(cells[1])} |")
    missing = [k for k in cosmos if k not in sim and abs(k[2] - 1.25) > .01]
    if missing:
        print("\nCosmos points with no sim counterpart:", sorted(missing))


if __name__ == "__main__":
    main()
