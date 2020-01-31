import argparse
import json
import csv
from typing import Callable
import sys

from matplotlib import pyplot as plt
from matplotlib import cm
from matplotlib.axes import Axes
from matplotlib.colors import Normalize, DivergingNorm
from matplotlib.image import AxesImage
from matplotlib import gridspec

import numpy as np

# look at https://github.com/sethoscope/heatmap

parser = argparse.ArgumentParser(description="convert jmh json into csv files")
parser.add_argument('files', metavar='F', nargs='+', help='files to visualize')

reported_metrics = ['score', '50.0', '99.9']

def maybe_format(v):
    v = str(v)
    if str.isnumeric(v.replace('-', '')):
        is_float = '.' in v
        return float(v) if is_float else int(v)
    else:
        return v

args = parser.parse_args()
for fname in args.files:
    header = []
    param_header = []
    expanded_header = []
    with open(fname, 'r') as f:
        js = json.load(f)

    with open(fname + '.csv', 'w') as f:
        writer = csv.writer(f)

        for row in js:
            bench_name = row['benchmark']
            params: dict = row['params']
            expanded = {}
            for k, v in row['expandedParams'].items():
                expanded.update(v)
            # expanded = row['expandedParams']
            metrics = row['primaryMetric']
            key = frozenset(params.items())

            if not header:
                param_header = [n for n in params.keys()]
                expanded_header = [k for k in expanded.keys()]
                metric_header = reported_metrics
                header = ['bench'] + param_header + expanded_header + metric_header
                writer.writerow(header)


            row = [bench_name]
            for name in param_header:
                row.append(maybe_format(params[name]))

            for name in expanded_header:
                row.append(maybe_format(expanded[name]))

            for metric in reported_metrics:
                if metric == 'score':
                    row.append(maybe_format(metrics['score']))
                else:
                    row.append(maybe_format(metrics['scorePercentiles'][metric]))

            writer.writerow(row)

