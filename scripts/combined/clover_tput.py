#!/usr/bin/env python3

import re
from argparse import ArgumentParser
from statistics import mean, median

from collections import defaultdict

clover_ops_pt = re.compile(r'Clover completed: (?P<ops>\d+(\.\d+)?)')
worker_id_pt = re.compile(r'Worker (?P<worker>\d+)')

sample_str = "I0116 17:25:29.593422 1565726 client.cc:240] Worker 4, HERD: 2.13475e+06/s, Clover completed: 409972.12/s, failures: 0/s"


def parse_line(line: str):
    m = re.search(clover_ops_pt, line)
    if m is None:
        return None
    ops = float(m.group('ops'))

    m = re.search(worker_id_pt, line)
    if m is None:
        return None
    worker = int(m.group('worker'))
    return (worker, ops)


def main(args):
    with open(args.log) as f:
        lines = f.read().splitlines()
    stats = defaultdict(lambda: [])
    for l in lines:
        stat = parse_line(l)
        if stat is None:
            continue
        worker, ops = stat
        stats[worker].append(ops)

    median_sum = 0
    avg_sum = 0
    for worker, ops_list in sorted(stats.items()):
        m = median(ops_list)
        median_sum += m
        a = mean(ops_list)
        avg_sum += a
        print("Worker {:d}: median={:.2f}, avg={:.2f}".format(worker, m, a))
    print(f"median={median_sum:.2f}, avg={avg_sum:.2f}")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("log")
    args = parser.parse_args()

    main(args)
