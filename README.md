# Elasticsearch Testing

A bunch of tools for testing Elasticsearch query performance. There are many like it, but this one is mine.

## load.py

Indexes the English Wikipedia dataset into Elasticsearch. Requires about 13G of disk space.

```
python load.py
```

## compare.py

Used to compare multiple queries. For example:

```
python compare.py example_queries/match_query.json example_queries/phrase_query.json --run-time=1800 --index=wiki-test
```

Outputs the median, max and min of both queries, plus a graph with both times plotted (sorted min to max).
