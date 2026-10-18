import json
import os
import socket
import subprocess
import time


def set_config(conf, db_conf, ycsb_conf, fname="sample.cfg"):
    template = json.load(open(fname))
    template.update(conf)
    template["db_config"].update(db_conf)
    template["ycsb_config"].update(ycsb_conf)
    out_fname = fname.split(".cfg")[0]+"_runtime.cfg"
    json.dump(template, open(out_fname, "w+"), indent=4)
    return out_fname


def try_compile(name):
    try:
        vcpkg_root = os.environ.get("VCPKG_ROOT", os.path.expanduser("~/mydata/vcpkg"))
        extra_flags = os.environ.get("NEMO_CMAKE_FLAGS", "")
        subprocess.run(f"cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE={vcpkg_root}/scripts/buildsystems/vcpkg.cmake {extra_flags} ../", shell=True, check=True)
        subprocess.run("make -j %s" % name, shell=True, check=True)
    except Exception as e:
        print("ERROR compiling, will exit. ")
        print(e)
        exit(1)


def _memcap_wrap(cmd, unit=None, cap=None):
    # NEMO_MEMCAP wraps each process in a systemd scope with MemoryMax set.
    # Network-sim convention: engine and SimStoreServer each get their own
    # scope, and the caps must SUM to the 32G machine budget — e.g.
    # NEMO_MEMCAP=18G NEMO_MEMCAP_SERVER=14G (the server holds the ~13G
    # dataset, the engine needs headroom for 100%-ratio buffers).
    memcap = cap or os.environ.get("NEMO_MEMCAP")
    if memcap:
        unit_arg = "--unit=%s" % unit if unit else ""
        # NEMO_RUNMAX_SEC: optional per-run wall-clock ceiling. A hung run
        # (e.g. the 2tree small-pool deadlock) gets
        # its scope killed by systemd, the caller sees a failure and its
        # retry logic proceeds -- without this a hang stalls a sweep forever.
        runmax = os.environ.get("NEMO_RUNMAX_SEC")
        runmax_arg = "-p RuntimeMaxSec=%s" % runmax if runmax else ""
        cmd = "systemd-run --user --scope -q %s %s -p MemoryMax=%s %s" % (
            unit_arg, runmax_arg, memcap, cmd)
    return cmd


_simnet_run_seq = 0


def simnet_addr(conf_file):
    try:
        with open(conf_file) as f:
            return json.load(f).get("db_config", {}).get("g_sim_server_addr", "")
    except Exception:
        return ""


def _wait_port(host, port, up, timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                if up:
                    return True
        except OSError:
            if not up:
                return True
        time.sleep(0.2)
    return False


def try_exec(name, conf_file, info):
    # network-sim configs (g_sim_server_addr set) get one fresh
    # SimStoreServer per run, started from the same config file: run N's
    # dirty state must never leak into run N+1
    addr = simnet_addr(conf_file)
    server = None
    if addr:
        global _simnet_run_seq
        _simnet_run_seq += 1
        host, port = addr.rsplit(":", 1)
        unit = "simstore-%d-%d" % (os.getpid(), _simnet_run_seq)
        server = subprocess.Popen(
            _memcap_wrap("./SimStoreServer %s" % conf_file, unit,
                         os.environ.get("NEMO_MEMCAP_SERVER")),
            shell=True, stdout=open("simstore_server.log", "a+"),
            stderr=subprocess.STDOUT)
        if not _wait_port(host, int(port), up=True):
            print("ERROR: SimStoreServer did not come up for {}".format(info))
            server.terminate()
            return False
    cmd = _memcap_wrap("./%s %s" % (name, conf_file))
    try:
        subprocess.run(cmd, shell=True, check=True)
        return True
    except Exception as e:
        print("ERROR executing: {}".format(info))
        print(e)
        err_file = open("error.log", "a+")
        err_file.write(info + ": ")
        err_file.write(str(e))
        err_file.write("\n")
        err_file.close()
        return False
    finally:
        if server is not None:
            subprocess.run("systemctl --user stop %s.scope 2>/dev/null" % unit,
                           shell=True)
            server.terminate()
            server.wait()
            # the next run rebinds the same port: wait for the release
            _wait_port(host, int(port), up=False)
