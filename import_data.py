#!/usr/bin/python3

import sys
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('dblp')

try:
    with open(sys.argv[1]) as f:
        count = 0
        for line in f:
            line = line.replace('"_id"', '"id"').replace("'", "''")
            query = "INSERT INTO bibliography JSON '{}'".format(line)
            session.execute(query)
            count += 1
            if (count % 1000) == 0:
                print("{} lines imported.".format(count))

except FileNotFoundError:
    print("Invalid file")

except IndexError:
    print("Usage: " + sys.argv[0] + " 'file'")
