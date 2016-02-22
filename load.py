import argparse
import sys
import time
import bz2
import logging
import xml.etree.ElementTree as ET

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def get_args():
    parser = argparse.ArgumentParser(
        description=(
            'Load Wikipedia dataset into Elasticsearch.'))
    parser.add_argument(
        'path_to_dataset', metavar='path_to_dataset', type=str,
        help='Path to Wikipedia dataset.')
    parser.add_argument(
        '--max-items', dest='max_items', action='store', default=None,
        help='Maximum number of items to fetch.')
    parser.add_argument(
        '--index', dest='index', action='store', required=True,
        help='Index to perform query against.')
    parser.add_argument(
        '--host', dest='host', action='store', default='localhost', type=str,
        help='Host')
    parser.add_argument(
        '--port', dest='port', action='store', default=9200, type=int,
        help='Port')
    return parser.parse_args()


def load_wiki(dump, args):
    found = 0
    for event, elem in ET.iterparse(dump, events=('start','end')):
        if event == 'start':
            if elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}page':
                output_text = None
                found += 1

                title = elem.find(
                    '{http://www.mediawiki.org/xml/export-0.10/}title')
                if title is None:
                    continue

                title_text = title.text

                page_id = elem.find(
                    '{http://www.mediawiki.org/xml/export-0.10/}id')
                if page_id is None:
                    continue

                page_id_text = page_id.text

                revision = elem.find(
                    '{http://www.mediawiki.org/xml/export-0.10/}revision')
                if revision is not None:
                    text = revision.find(
                        '{http://www.mediawiki.org/xml/export-0.10/}text')
                    if text is not None:
                        output_text = text.text

                    yield dict(
                        _id=page_id_text, title=title_text, text=output_text,
                        _index=args.index, _type='page')

                    if args.max_items and found >= args.max_items:
                        print("Found max item {0}".format(args.max_items))
                        return
        else:
            elem.clear()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    args = get_args()

    start = time.time()

    es = Elasticsearch(
        ['{host}:{port}'.format(host=args.host, port=args.port)])
    es.indices.delete(args.index, ignore=404)
    es.indices.create(args.index, {
        'settings': {
            'number_of_shards': 1
        },
        'mappings': {
            'page': {
                'properties': {
                    'title': {
                        'type': 'string'
                    },
                    'text': {
                        'type': 'string'
                    }
                }
            }
        }
    })

    logging.info("Start index")
    bulk(es, load_wiki(bz2.BZ2File(args.path_to_dataset), args))
    logging.info("Total time to index: {0}".format(time.time() - start))
