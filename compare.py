import argparse
import logging
import time
import sys
import json

import numpy as np
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch



def get_args():
    parser = argparse.ArgumentParser(
        description=(
            'Test and compare the performance of Elasticsearch queries.'))

    parser.add_argument('query_files', metavar='q', type=str, nargs='+')
    parser.add_argument(
        '--run-time', dest='run_time', action='store',
        help='Run time in seconds', type=int, default=30 * 60)
    parser.add_argument(
        '--index', dest='index', action='store', required=True,
        help='Index to perform query against')
    parser.add_argument(
        '--host', dest='host', action='store', default='localhost', type=str,
        help='Host')
    parser.add_argument(
        '--port', dest='port', action='store', default=9200, type=int,
        help='Port')
    args = parser.parse_args()
    return args


def runner(es, query, args):
    """
    Start by clearing the cache. Then, run query until we hit or exceed ``time_in_secs``.
    
    Displays a plot of query time and returns some stats about them.
    """
    results = []

    es.indices.clear_cache(args.index)
    
    time_start = time.time()

    while (time.time() - time_start) < args.run_time:
        r = es.search(index=args.index, body=query)
        results.append(r['took'])
    
    results = sorted(results)

    return np.array(results)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

    args = get_args()

    es = Elasticsearch(
        ['{host}:{port}'.format(host=args.host, port=args.port)])

    legend = []

    for count, query_file in enumerate(args.query_files):
        query = json.loads(open(query_file).read())
        logging.info('Test #{0}: {1}'.format(count + 1, query_file))
        logging.debug('Query body: {0}'.format(query))
        results = runner(es, query, args)
        logging.info('Median: {0}, Max: {1}, Min: {2}'.format(
            np.median(results), max(results), min(results)))
        plt.plot(np.arange(len(results)), results)
        legend.append(query_file)

    plt.legend(legend, loc='upper left')

    plt.show()
