#!/usr/bin/env python3
import re
import sys
import json
import argparse
import subprocess

from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', help="Only search files opened in the given mode", choices=['read', 'write'], default=None)
    parser.add_argument('-f', '--file_glob', help="Glob expression to use for log selection on server. Could be just file path. Can be given multiple times, resulting in multiple globs usage", required=True, action='append')
    parser.add_argument('-p', '--path_part', help="Only search files whose path have this value in it (e.g. matches [^ ]*<path_part[^ ]* regexp)", default=None)
    parser.add_argument('-u', '--user', help="SSH user", required=True)
    parser.add_argument('gw_host', help="Gateway host to ssh to and parse logs. Can be given multiple times.", nargs='+')
    return parser.parse_args()



if __name__ == '__main__':
    args = parse_args()
    res = {}
    gw = None
    glob = ' '.join(args.file_glob)
    path_rexp = ('[^ ]*' + args.path_part + '[^ ]*') if args.path_part else '[^ ]*'
    grep_mode_rexp = r'\(read\|write\)' if args.mode else args.mode
    mode_rexp = grep_mode_rexp.replace('\\', '')
    for gw in args.gw_host:
        cur_res = {}
        ssh_res = subprocess.run(['ssh', '-l', args.user, gw, "zgrep --binary-file=text 'File descriptor [0-9]\+ associated to file " + path_rexp + " opened in " + grep_mode_rexp + " mode' " + glob], stdout=subprocess.PIPE)
        if ssh_res.returncode == 0:
            for line in ssh_res.stdout.decode('utf-8').split('\n'):
                m = re.match('.*(?P<ts>[0-9]{6} [0-9:]{8}) File descriptor [0-9]+ associated to file (?P<filename>[^ ]+) opened in ' + mode_rexp + ' mode.*', line)
                if m:
                    epoch = int(datetime.strptime(m.group('ts'), "%y%m%d %H:%M:%S").strftime("%s"))
                    filename = m.group('filename')
                    if filename in cur_res:
                        cur_res[filename].append(epoch)
                    else:
                        cur_res[filename] = [ epoch ]
        res[gw] = cur_res

    print(json.dumps(res, indent=2))
