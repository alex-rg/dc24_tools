#!/usr/bin/env python3
import re
import sys
import json
import argparse

from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--gateway_json', help="JSON file with gateway data", required=True)
    parser.add_argument('-t', '--transfer_json', help="JSON file with transfer data", required=True)
    parser.add_argument('-m', '--mode', help="File access mode.", choices=['read', 'write'], default='read')
    parser.add_argument('-p', '--prefix', help="Prefix to remove in order to get LFN (file name in terms of gateway JSON)." \
            + " If you want to filter file's by path, include selective regexp in parenthesis," \
            + " e.g. to select only files form /testdir: 'https://se.domain:1094/vo/base/path(/testdir[^\"]*)'",
            required=True
        )
    return parser.parse_args()


def find_gw(gw_data, path, start_ts, end_ts):
    gws = []
    start = int(datetime.strptime(start_ts, '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))
    end = int(datetime.strptime(end_ts, '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))
    for gw, files in gw_data.items():
        if path in files:
            for ts in files[path]:
                 if ts >= start - 10 and ts <= end + 10:
                     gws.append(gw)
    if len(gws) == 1:
        res = gws[0]
    elif len(set(gws)) > 1:
        print(f"Multiple gateways for file {path}; {start}, {end}", file=sys.stderr)
        res = 'Multiple'
    else:
        print(f"GW not found for file {path}; {start}, {end}", file=sys.stderr)
        res = None
    return res



if __name__ == '__main__':
    args = parse_args()
    with open(args.gateway_json) as fd:
        gw_data = json.loads(fd.read())

    with open(args.transfer_json) as fd:
        transfer_data = json.loads(fd.read())

    url_key = 'dest_surl' if args.mode == 'write' else 'read'
    #If we already have group in prefix, just compile it, otherwise add.
    if re.match('.*\(.*\).*', args.prefix):
        path_rexp = re.compile(args.prefix)
    else:
        path_rexp = re.compile(args.prefix + '([^"]+)')

    for item in transfer_data:
        m = path_rexp.match(item['dest_surl'])
        if m:
            path = m.group(1)
            gw = find_gw(gw_data, path, item['start_time'], item['finish_time'])
            item['gateway'] = gw

    print(json.dumps(transfer_data, indent=2))
