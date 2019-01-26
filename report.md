# Report Cassandra - DBLP

### By BEGE Áron, CAILLON Romain, DAUVISSAT Corentin

---------

## Database creation

create_database.txt

```sql
DROP KEYSPACE IF EXISTS dblp;

CREATE KEYSPACE dblp
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 3 };

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
            dictio["id"] = dictio.pop("_id")
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
            line = line.replace("'", "''")
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
foo@bar:~$ ./import_data.py DBLP_clean.json
```

## Queries

#### Simple queries

```SQL
INSERT INTO bibliography (id, type, year, title, authors, pages, booktitle, journal, url, cites) VALUES (
    'series/cogtech/esilv420',
    'Article',
    2019,
    'Data Science explained for Children',
    ['Corentin Dauvissat', 'Romain Caillon', 'Aron Bege'],
    {start: 20, end: 60},
    'Mickey Mouse into the Big Data World',
    {isbn: []},
    'db/series/cogtech/364237376.html#esilv420',
    []
);
```

```SQL
SELECT * FROM bibliography WHERE id='series/cogtech/esilv420';
```

```SQL
SELECT COUNT(*) FROM bibliography WHERE type = 'Article' AND year = 2010 ALLOW FILTERING;
```

```SQL
SELECT title FROM bibliography WHERE authors CONTAINS 'Sven Horn' ALLOW FILTERING;
```

```SQL
CREATE INDEX fk_bibliography_type ON bibliography(type);
SELECT id FROM bibliography WHERE type = 'Article' LIMIT 20;
```

#### Complex queries

```SQL
UPDATE bibliography SET authors = authors + ['Corentin Dauvissat', 'Romain Caillon', 'Aron Bege']
WHERE id='conf/colcom/WolinskyLBF10';
```   

#### Hard queries

```SQL
CREATE OR REPLACE FUNCTION countGroupState ( state map<text,int>, val text )
CALLED ON NULL INPUT RETURNS map<text,int> LANGUAGE java AS '
    state.put(val, state.getOrDefault(val, 0) + 1);
    return state;
    ';

CREATE OR REPLACE AGGREGATE countGroup(text) SFUNC countGroupState STYPE map<text,int> INITCOND {};

CREATE INDEX ON bibliography (year);

SELECT countGroup(type) FROM bibliography WHERE year = 1999;
```

```SQL
CREATE FUNCTION myMin(current int, candidate int)
CALLED ON NULL INPUT RETURNS int LANGUAGE java AS '
    if (current == null) return candidate;
    else return Math.min(current, candidate);
    ';

CREATE AGGREGATE aggMin(int) SFUNC myMin STYPE int INITCOND null;

CREATE INDEX ON bibliography (authors);

SELECT aggMin(year) FROM bibliography WHERE authors CONTAINS 'Manoj Kumar';
```
