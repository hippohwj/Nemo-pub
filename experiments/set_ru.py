#!/usr/bin/env python3
"""Inspect / batch-set Cosmos DB Table API throughput using the account key.

Usage:
  set_ru.py list
  set_ru.py set <RU> [table-name-substring] [--exact]

--exact matches the table NAME instead of a substring. Per-table plans need
it: our TPC-C tables are "<dataset>-<oid>", so the substring "otpcc34w_123-1"
also matches -10, -1073741828, -1342177285, -1610612742 and -1879048199, and
setting the tiny table -1 to 400 would silently reset all of them (leaving a
big index-page table at the floor and losing shards to exhausted 429
retries). Dataset-wide restores still want the substring form.

Reads the connection string from build/configs/ifconfig_azure.txt
(override with NEMO_AZURE_CONF).
- If TablesDB has database-level (shared) throughput, `set` changes that one
  offer (the substring filter is ignored -- one pool covers every table).
- Otherwise `set` walks the tables whose name contains the substring
  (default "_123") and updates each dedicated offer.
- Autoscale offers are updated as autoscale max; manual offers as manual RU.
"""
import os
import re
import sys

_pydeps = os.environ.get("NEMO_PYDEPS")
if _pydeps and os.path.isdir(_pydeps):
    sys.path.insert(0, _pydeps)
from azure.cosmos import CosmosClient, ThroughputProperties
from azure.cosmos.exceptions import CosmosResourceNotFoundError, CosmosHttpResponseError

# NEMO_AZURE_CONF overrides the default location of the connection string.
CONF = os.environ.get("NEMO_AZURE_CONF",
                      "build/configs/ifconfig_azure.txt")


def connect():
    conn = open(CONF).read().strip()
    acct = re.search(r"AccountName=([^;]+)", conn).group(1)
    key = re.search(r"AccountKey=([^;]+)", conn).group(1)
    url = "https://%s.documents.azure.com:443/" % acct
    return CosmosClient(url, credential=key), acct


def describe(tp):
    if tp.auto_scale_max_throughput:
        return "autoscale max=%d (floor=%d)" % (
            tp.auto_scale_max_throughput, tp.auto_scale_max_throughput // 10)
    return "manual %d RU/s" % tp.offer_throughput


def get_tp(obj):
    try:
        return obj.get_throughput()
    except (CosmosResourceNotFoundError, CosmosHttpResponseError):
        return None
    except AttributeError:
        # azure-cosmos 4.9 bug: the offer-404 retry path crashes with
        # AttributeError when the database has no shared throughput
        return None


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "list"
    client, acct = connect()
    db = client.get_database_client("TablesDB")

    db_tp = get_tp(db)
    if mode == "list":
        print("account:", acct)
        print("TablesDB shared throughput:",
              describe(db_tp) if db_tp else "(none - per-table offers)")
        for c in db.list_containers():
            tp = get_tp(db.get_container_client(c["id"]))
            print("  %-28s %s" % (c["id"], describe(tp) if tp else "(shared pool)"))
        return

    if mode == "set":
        target = int(sys.argv[2])
        args = [a for a in sys.argv[3:] if a != "--exact"]
        exact = "--exact" in sys.argv[3:]
        pattern = args[0] if args else "_123"

        def apply(obj, tp):
            # manual offers MUST get a plain int: azure-cosmos 4.9 silently
            # no-ops on ThroughputProperties(offer_throughput=...) here
            if tp.auto_scale_max_throughput:
                obj.replace_throughput(
                    ThroughputProperties(auto_scale_max_throughput=target))
            else:
                obj.replace_throughput(target)
            after = get_tp(obj)  # verify the write actually took
            got = (after.auto_scale_max_throughput or after.offer_throughput
                   if after else None)
            return got

        if db_tp:
            got = apply(db, db_tp)
            print("TablesDB shared pool: %s -> %s %s" % (
                describe(db_tp), got, "OK" if got == target else "**NO-OP**"))
            return
        hits = 0
        for c in db.list_containers():
            name = c["id"]
            if (name != pattern) if exact else (pattern not in name):
                continue
            cc = db.get_container_client(name)
            tp = get_tp(cc)
            if not tp:
                print("  %-28s shared (skip)" % name)
                continue
            got = apply(cc, tp)
            print("  %-28s %s -> %s %s" % (
                name, describe(tp), got, "OK" if got == target else "**NO-OP**"))
            hits += 1
        print("updated %d tables matching '%s'%s"
              % (hits, pattern, " exactly" if exact else ""))
        if exact and hits == 0:
            print("**no table named '%s' -- nothing set**" % pattern)
            sys.exit(1)
        return

    print(__doc__)


if __name__ == "__main__":
    main()
