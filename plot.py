#!/usr/bin/env python3

import sys
import json
import argparse

from datetime import datetime
from matplotlib import pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('data', help='JSON file to plot')
    parser.add_argument('-o', '--output', help='Plot output', default='./plot.png')
    parser.add_argument('-r', '--resolution', help='Resolution, in dpi', default=300, type=int)
    parser.add_argument('-x', '--xlim', help='X axis limits, comma-separated', default=None)
    parser.add_argument('-s', '--start_ts', help='Do not consider transfers that started before given time. Format: 2024-01-18T01:45:59', default=None)
    parser.add_argument('-e', '--end_ts', help='Do not consider transfers that finished after given time. Format: 2024-01-18T01:45:59', default=None)
    parser.add_argument('-S', '--successfull_only', help='Do not consider failed transfers.', action='store_true')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    with open(args.data) as fd:
        data = json.loads(fd.read())

    vos = {'all': []}
    for item in data:
        vo = item['vo_name']
        start_time = int(datetime.strptime(item['start_time'], '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))
        end_time = start_time + item['tx_duration']
        #datetime.strptime(item['finish_time'], '%Y-%m-%dT%H:%M:%SZ').strftime("%s")
        start_time = int(start_time)
        end_time = int(end_time)
        if (
               args.start_ts
               and
               start_time < int(datetime.strptime(args.start_ts, '%Y-%m-%dT%H:%M:%S').strftime("%s"))
               or
               args.end_ts
               and
               end_time > int(datetime.strptime(end_ts, '%Y-%m-%dT%H:%M:%S').strftime("%s"))
               or
               args.successfull_only
               and
               item['file_state'] != "FINISHED"  
           ):
            continue
        if vo in vos:
            vos[vo].append( (start_time, item['throughput']) )
            vos[vo].append( (end_time, -item['throughput']) )
        else:
            vos[vo] = [ (start_time, item['throughput']) ]
            vos[vo] = [ (end_time, -item['throughput']) ]
        vos['all'].append([start_time, item['throughput']])
        vos['all'].append([end_time, -item['throughput']])

    if len(vos['all']) == 0:
        print("No data found! Check filters.")
        sys.exit(1)

    for vo in vos:
        vos[vo].sort(key=lambda x: x[0])

    res = {k: ([], []) for k in vos}
    shift = vos['all'][0][0]
    for key, val in vos.items():
        cum_sum = 0
        for ts, thr in val:
            cum_sum += thr
            if len(res[key][0]) > 0 and res[key][0][-1] == ts:
                res[key][1][-1] += thr
            else:
                res[key][0].append(ts - shift)
                res[key][1].append(cum_sum)
    print(f"lasted: {res['all'][0][-1]}, start time: {shift}, end time: {shift + res['all'][0][-1]}") 
    legend = []
    for key, val in res.items():
        plt.plot(val[0], val[1])
        legend.append(key)
    if args.xlim:
        s, e = [int(x) for x in args.xlim.split(',')]
        plt.xlim([s,e])
    plt.legend(res)
    plt.savefig(args.output, dpi=args.resolution)
