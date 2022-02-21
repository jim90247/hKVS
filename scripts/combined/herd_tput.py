#!/usr/bin/env python3
import re
from argparse import ArgumentParser
from collections import defaultdict
from pprint import pprint
from statistics import median, mean

# example_str = "main: Worker 1: 6605767.48 IOPS. Avg per-port postlist = 17.81. HERD lookup fail rate = 0.0085"

pattern = re.compile(
    r'^main: Worker (?P<worker_id>\d+): (?P<iops>\d+(\.\d+)?) IOPS. Avg per-port postlist = (?P<postlist>\d+(\.\d+)?)')


def parse_line(line: str):
    m = pattern.match(line)
    return m.groupdict() if m is not None else None


def main(args):
    with open(args.log) as f:
        lines = f.read().splitlines()
    iops_stats = defaultdict(lambda: [])
    postlist_stats = defaultdict(lambda: [])
    for line in lines:
        stats = parse_line(line)
        if stats is None:
            continue
        worker_id = int(stats['worker_id'])
        iops_stats[worker_id].append(float(stats['iops']))
        postlist_stats[worker_id].append(float(stats['postlist']))

    total_iops_median = 0.0
    total_iops_avg = 0.0
    for worker_id in sorted(iops_stats):
        x = median(iops_stats[worker_id])
        y = mean(iops_stats[worker_id])
        print("worker {:2d}: median={:.2f} op/s, avg={:.2f} op/s, avg postlist={:.2f}".format(
            worker_id, x, y, mean(postlist_stats[worker_id])))
        total_iops_median += x
        total_iops_avg += y
        pass
    print("median={:.2f} op/s, avg={:.2f} op/s".format(total_iops_median, total_iops_avg))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("log")
    args = parser.parse_args()

    main(args)
