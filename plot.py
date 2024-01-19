#!/usr/bin/env python3

import re
import sys
import json
import time
import argparse
import matplotlib.ticker as mtick

from datetime import datetime
from matplotlib import pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('data', help='JSON file to plot')
    parser.add_argument('-o', '--output', help='Plot output', default='./plot.png')
    parser.add_argument('-r', '--resolution', help='Resolution, in dpi', default=300, type=int)
    parser.add_argument('-x', '--xlim', help='X axis limits, comma-separated', default=None)
    parser.add_argument('-y', '--ylim', help='Y axis limits, comma-separated', default=None)
    parser.add_argument('-g', '--group_by', help='Group transfers by key', default='vo_name')
    parser.add_argument('-H', '--height', help='Figure height', default=None, type=int)
    parser.add_argument('-W', '--width', help='Figure width', default=None, type=int)
    parser.add_argument('-f', '--filter', help='Arbitrary filter for values. Should be string desc of a lambda which takes 1 arg (transfer description dict).', default=None)
    parser.add_argument('-s', '--start_ts', help='Do not consider transfers that started before given time. Format: 2024-01-18T01:45:59', default=None)
    parser.add_argument('-e', '--end_ts', help='Do not consider transfers that finished after given time. Format: 2024-01-18T01:45:59', default=None)
    parser.add_argument('-S', '--successfull_only', help='Do not consider failed transfers.', action='store_true')

    subparser = parser.add_subparsers(dest='subcommand')
    p1 = subparser.add_parser('plot_throughput', help="Plot cumulative throughput vs time, possibly with some grouping")
    p2 = subparser.add_parser('plot_not', help="Plot cumulative number of transfers vs time, possibly with some grouping")
    p3 = subparser.add_parser('plot_dist', help="Plot individual transfers throughput distribution")

    p3.add_argument('-m', '--multiple_bins', help='What to do with multiple bins', default='layer', choices=['layer', 'fill', 'dodge', 'stack'])
    p3.add_argument('-G', '--group_by_func', help='Aggreagate lambda, for scattered plots. Default is to group by key value', default=None)
    args = parser.parse_args()
    return args


class DataManager:
    def __init__(self, file_path):
        with open(args.data) as fd:
            self.data = json.loads(fd.read())

    def filter_data(self, filt=None, start_ts=None, end_ts=None, success_only=False):
        filtered_data = []
        for item in self.data:
            start_time = int(datetime.strptime(item['start_time'], '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))
            end_time = start_time + item['tx_duration']
            #datetime.strptime(item['finish_time'], '%Y-%m-%dT%H:%M:%SZ').strftime("%s")
            start_time = int(start_time)
            end_time = int(end_time)
            if (
                   args.start_ts
                   and
                   start_time < int(datetime.strptime(start_ts, '%Y-%m-%dT%H:%M:%S').strftime("%s"))
                   or
                   args.end_ts
                   and
                   end_time > int(datetime.strptime(end_ts, '%Y-%m-%dT%H:%M:%S').strftime("%s"))
                   or
                   success_only
                   and
                   item['file_state'] != "FINISHED"
                   or
                   filt and not filt(item)
               ):
                continue
            item['end_epoch'] = end_time
            item['start_epoch'] = start_time
            filtered_data.append(item)
        self.filtered_data = filtered_data

    def arrange(self, group_by):
        arranged_by_key = {'all': []}
        for item in self.filtered_data:
            key = item[group_by]
            start_time = item['start_epoch']
            end_time = item['end_epoch']
            end_time = int(end_time)
            if key in arranged_by_key:
                arranged_by_key[key].append( (start_time, item['throughput'], 1) )
                arranged_by_key[key].append( (end_time, -item['throughput'], -1) )
            else:
                arranged_by_key[key] = [ (start_time, item['throughput'], 1) ]
                arranged_by_key[key] = [ (end_time, -item['throughput'], -1) ]
            arranged_by_key['all'].append( (start_time, item['throughput'], 1) )
            arranged_by_key['all'].append( (end_time, -item['throughput'], -1) )

        if len(arranged_by_key['all']) == 0:
            print("No data found! Check filters.")
            sys.exit(1)

        for key in arranged_by_key:
            arranged_by_key[key].sort(key=lambda x: x[0])
        self.arranged_by_key = arranged_by_key

    def calculate_cumulatives(self):
        "Calculate cumulative values for plotting"
        res = {k: ([], [], []) for k in self.arranged_by_key}
        for key, val in self.arranged_by_key.items():
            cum_num = 0
            cum_thr = 0
            for ts, thr, tr_state in val:
                cum_thr = max(0, cum_thr + thr)
                cum_num = cum_num + tr_state
                ts = ts
                if len(res[key][0]) > 0 and res[key][0][-1] == ts:
                    res[key][1][-1] += thr
                    res[key][2][-1] += tr_state
                else:
                    res[key][0].append(ts)
                    res[key][1].append(cum_thr)
                    res[key][2].append(cum_num)
        print(f"lasted: {res['all'][0][-1]}, start time: {res['all'][0][0]}, end time: {res['all'][0][-1]}")
        self.res_cum = res



if __name__ == '__main__':
    args = parse_args()
    dm = DataManager(args.data)

    #Filter data
    if args.filter:
        filt = eval(args.filter)
    else:
        filt = None
    dm.filter_data(filt=filt, start_ts=args.start_ts, end_ts=args.end_ts, success_only=args.successfull_only)

    if args.subcommand == 'plot_dist':
        import seaborn as sns
        import pandas as pd
        #data = filter(lambda x: 'gateway' in x and x['gateway'], data)
        #data = filter(lambda x: not re.match('^ceph-(gw[0-9]+|svc9[789]):',  x['gateway']), data)
        #data_proc['group'].append('old' if re.match('^ceph-(gw[0-9]+|svc9[789]):', item['gateway']) else ('new' if item['gateway'] != 'Multiple' else 'mult'))
        gr_by = args.group_by
        gr_by_func = eval(args.group_by_func) if args.group_by_func else lambda x: x[gr_by]
        data_proc = {'thr': [], gr_by: []}
        for item in dm.filtered_data:
            data_proc['thr'].append(item['throughput'])
            data_proc[gr_by].append(gr_by_func(item))
        data = pd.DataFrame(data=data_proc)
        sns.displot(data, x='thr', hue=gr_by, bins=120, multiple=args.multiple_bins)
    else:
        dm.arrange(args.group_by)
        dm.calculate_cumulatives()
        legend = []
        for key, val in dm.res_cum.items():
            if args.subcommand == 'plot_throughput':
                yval = val[1]
            else:
                yval = val[2]
            plt.step(val[0], yval)
            legend.append(key)
        if args.xlim:
            s, e = [int(x) for x in args.xlim.split(',')]
            plt.xlim([s,e])
        if args.ylim:
            s, e = [int(x) for x in args.ylim.split(',')]
            plt.ylim([s,e])

        if args.subcommand == 'plot_throughput':
            ylabel, title = 'Throughput, MiB/s', f'Throughput by {args.group_by}'
        else:
            ylabel, title = 'Number Of Transfers', f'Transfers by {args.group_by}'

        plt.xlabel("Time")
        plt.ylabel(ylabel)
        plt.title(title)
        plt.legend(legend)
        plt.gca().xaxis.set_major_formatter(
                mtick.FuncFormatter(lambda pos,_: time.strftime("%d-%m %H:%M",time.localtime(pos)))
            )
        plt.xticks(rotation=90)
        if args.height:
            plt.gcf().set_figheight(args.height)
        if args.width:
            plt.gcf().set_figwidth(args.width)
    plt.savefig(args.output, dpi=args.resolution)
