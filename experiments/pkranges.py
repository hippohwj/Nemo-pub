#!/usr/bin/env python3
"""Print the physical partition (pk-range) count of a Table API table.

    pkranges.py <table>            -> count on stdout, details on stderr

Reads the Cosmos SQL endpoint of the same account (TablesDB database)
with the AccountKey from NEMO_AZURE_CONF. Used by the import drivers to
wait for the 40k-RU-induced split before starting a bulk upload.
"""
import os
import re
import sys

from azure.cosmos import CosmosClient

conf = os.environ.get(
    "NEMO_AZURE_CONF",
    os.path.expanduser("~/nemo100g/configs/ifconfig_azure.txt"))
conn = open(conf).read().strip()
name = re.search(r"AccountName=([^;]+)", conn).group(1)
key = re.search(r"AccountKey=([^;]+)", conn).group(1)

client = CosmosClient("https://%s.documents.azure.com:443/" % name,
                      credential=key)
container = client.get_database_client("TablesDB") \
    .get_container_client(sys.argv[1])
ranges = list(container.client_connection._ReadPartitionKeyRanges(
    container.container_link))
print(len(ranges))
for r in ranges:
    print(" range id=%s [%s, %s)" % (r["id"], r.get("minInclusive", ""),
                                     r.get("maxExclusive", "")),
          file=sys.stderr)
