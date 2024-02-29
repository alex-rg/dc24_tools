#!/usr/bin/env python3
import os
import sys
import json
import urllib.request
import urllib.parse
import argparse

from multiprocessing.pool import ThreadPool

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dest_se', help="only fetch transfers with given dest se", default=None)
    parser.add_argument('-v', '--verbose', help="Be more verbose", action='store_true')
    parser.add_argument('-s', '--source_se', help="only fetch transfers with given source se", default=None)
    parser.add_argument('-S', '--state', help="only fetch transfers with given state", default=None)
    parser.add_argument('-t', '--time_window', help="time window to use", default=1)
    parser.add_argument('-T', '--threads', help="Number of threads to use", default=5, type=int)
    parser.add_argument('fts_endpoint', help="Fts server")
    return parser.parse_args()


def get_page(url):
    try:
        resp = urllib.request.urlopen(url)
    except:
        success = False
    else:
        success = resp.status == 200

    if success:
        data = json.loads(resp.read())
    else:
        data = {}
    return (success, data, url)


def get_urls(urls, pool, retries=2):
    urls_to_get = urls
    res = []
    for retr in range(retries):
        if urls_to_get:
            res += pool.map(get_page, urls_to_get)
        urls_to_get = []
        for val in res:
            if not val[0]:
                #last attempt, request failed
                if retr == retries -1:
                    print("Failed to get url {0}, no retries left".format(val[2]))
                else:
                    urls_to_get.append(val[2])
    return res

def get_page_range(base_url, rng, pool, retries=2):
    return get_urls([base_url + '&page={0}'.format(i) for i in rng], pool, retries)


if __name__ == '__main__':
    page_size = 50
    os.environ['SSL_CERT_DIR'] = '/etc/grid-security/certificates'
    args = parse_args()
    parms = {'page_size': page_size, 'vo': 'lhcb'}
    for parm in ('source_se', 'dest_se', 'time_window', 'state'):
        tval = getattr(args, parm)
        if tval:
            parms[parm] = tval
    base_url = args.fts_endpoint + '/jobs?' + urllib.parse.urlencode(parms)
    resp = urllib.request.urlopen(base_url)
    data = json.loads(resp.read())
    res = data['items']
    page_count = data['pageCount']

    pool = ThreadPool(args.threads)

    rng = range(2, page_count + 1)
    rest = get_page_range(base_url, rng, pool)
    for tres in rest:
        if tres[0]:
            res += tres[1]['items']

    print(f"Got the job list ({len(res)}), processing individual files now", file=sys.stderr)
    if args.verbose:
        print(json.dumps(res, indent=2), file=sys.stderr)
    jobs = [x['job_id'] for x in res]
    res = get_urls([args.fts_endpoint + '/jobs/' + x + '/files' for x in jobs], pool)
    rest = []
    for val in res:
        if val[0]:
            pc = val[1]['files']['pageCount']
            if pc > 1:
                job_id = val[1]['files']['items'][0]['job_id']
                rest += get_page_range(args.fts_endpoint + '/jobs/' + job_id + '/files?', range(2,pc+1), pool)
    res += rest
    res = [x for item in res for x in item[1]['files']['items'] if item[1]]
    print(json.dumps(res, indent=2))
