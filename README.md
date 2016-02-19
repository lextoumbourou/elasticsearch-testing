# Elasticsearch Testing

## WIP

A bunch of tools for testing Elasticsearch query performance.

## loader.py

Indexes the Wikipedia dataset into Elasticsearch. Requires about 13G of disk space.

## compare.py

Used to compare multiple queries. For example:

```
compare.py query_1.json query_2.json --run-time=1800
```

Outputs the median, max and min of both queries, plus a graph with both times plotted (sorted min to max)
