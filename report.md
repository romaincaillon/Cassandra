# Report Cassandra - DBLP

### By BEGE Áron, CAILLON Romain, DAUVISSAT Corentin

---------

## Database creation

create_database.txt

```sql
DROP KEYSPACE IF EXISTS dblp;

CREATE KEYSPACE dblp
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 3 };

USE dblp;

CREATE TYPE IF NOT EXISTS journal_type (
	editor VARCHAR,
	isbn list<varchar>,
	series VARCHAR,
	volume VARCHAR
);

CREATE TYPE IF NOT EXISTS pages_type (
	end DOUBLE,
	start DOUBLE
);

CREATE TABLE IF NOT EXISTS bibliography (
	id VARCHAR,
	authors list<VARCHAR>,
	booktitle VARCHAR,
	cites list<varchar>,
	journal frozen<journal_type>,
	pages frozen<pages_type>,
	title VARCHAR,
	type VARCHAR,
	url VARCHAR,
	year DOUBLE,
	PRIMARY KEY (id)
);
```

```shell
cqlsh> SOURCE 'create_database.txt';
```

## Data importation

import_data.py

```python
#!/usr/bin/python3

import sys
from cassandra.cluster import Cluster
import json

cluster = Cluster()
session = cluster.connect('dblp')

try:
    with open(sys.argv[1]) as f:
        count = 0
        for line in f:
            dictio = json.loads(line)
            try:
                dictio["year"] = int(dictio["year"])
            except TypeError:
                pass
            try:
                dictio["pages"]["start"] = int(dictio["pages"]["start"])
            except TypeError:
                pass
            try:
                dictio["pages"]["end"] = int(dictio["pages"]["end"])
            except TypeError:
                pass
            line = json.dumps(dictio)
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
```

```shell
foo@bar:~$ ./import_data.py DBLP_clean.JSON
```
