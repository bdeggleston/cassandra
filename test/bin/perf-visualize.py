import argparse
import json
from typing import Callable
import sys

from matplotlib import pyplot as plt
from matplotlib import cm
from matplotlib.axes import Axes
from matplotlib.colors import Normalize, LogNorm, SymLogNorm
from matplotlib.image import AxesImage
from matplotlib import gridspec

import numpy as np

parser = argparse.ArgumentParser(description="visualize jmh json output")
parser.add_argument('--prettify', '-p', dest='prettify', action='store_true', help="don't process anything, just rewrite the given files in an indented format")
parser.add_argument('files', metavar='F', nargs='+', help='files to visualize')

args = parser.parse_args()

if args.prettify:
    for fname in args.files:
        with open(fname, 'r') as f:
            js = json.load(f)
        with open(fname, 'w') as f:
            json.dump(js, f, indent=4, sort_keys=True)
    sys.exit(0)

class Trial(object):
    def __init__(self, params: dict, expanded=None):
        self.params = params
        self.expanded = expanded or dict()
        self.results = []

    def add_result(self, metrics):
        result = {'score': metrics['score'],
                  '50.0': metrics['scorePercentiles']['50.0'],
                  '99.9': metrics['scorePercentiles']['99.9']}
        self.results.append(result)


class Benchmark(object):
    def __init__(self, name: str):
        self.name = name
        self.trials = {}

class ValueSet(object):
    def __init__(self, name):
        self.name = name
        self.values = set()

    def add(self, value):
        self.values.add(value)

    def sorted(self):
        if all([str.isnumeric(v.replace('-', '')) for v in self.values]):
            is_float = any(['.' in v for v in self.values])
            cmp = (lambda v: float(v)) if is_float else (lambda v: int(v))
            return sorted(self.values, key=cmp)
        else:
            return sorted(self.values)

    def tuple(self):
        assert len(self.values) == 1
        return (self.name, list(self.values)[0])

benchmarks = {}

x_param = 'concentration'
y_params = ['columnCount', 'rowOverlap']
unlisted_params = []
results = ['score', '50.0', '99.9']
param_values = {}

class ValueFormat(object):
    def __init__(self, simple_name, format: Callable[..., str] = None):
        self.simple_name = simple_name
        self._format = format

    def format(self, v) -> str:
        return self._format(v) if self._format else str(v)

value_format = {
    'columnCount': ValueFormat('cols'),
    'rowOverlap': ValueFormat('ovr', lambda v: '{:.1f}'.format(float(v))),
    'distribution': ValueFormat('dist', lambda v: v[0])
}

def format_param(name, value):
    return value_format[name].format(value) if name in value_format else str(value)

def is_listed_dimension(dim):
    return x_param == dim or dim in y_params

# parse and organize the data

for fname in args.files:
    with open(fname, 'r') as f:
        js = json.load(f)

    for row in js:
        bench_name = row['benchmark']
        benchmark = benchmarks.setdefault(bench_name, Benchmark(bench_name))
        params: dict = row['params']
        key = frozenset(params.items())

        trial = benchmark.trials.setdefault(key, Trial(params, row['expandedParams']))
        trial.add_result(row['primaryMetric'])

        for k, v in params.items():
            if k not in param_values:
                param_values[k] = ValueSet(k)

            vset = param_values[k]
            vset.add(v)

# https://matplotlib.org/3.1.0/gallery/images_contours_and_fields/image_annotated_heatmap.html

# check that there aren't any unlisted dimensions with multiple values
for k, v in param_values.items():
    if not is_listed_dimension(k):
        if len(v.values) > 1:
            print(k, "has multiple values and doesn't have a graphing dimension")
            sys.exit(1)
        else:
            unlisted_params.append(k)



def make_row_map(params, previous=None):
    if not params:
        return previous
    param = params[0]
    to_append = [(param, v) for v in param_values[param].sorted()]
    if previous is None:
        return make_row_map(params[1:], [[v] for v in to_append])
    else:
        appended = []
        for row in previous:
            for value in to_append:
                appended.append(row + [value])

        return make_row_map(params[1:], appended)

row_labels = make_row_map(y_params)

data_name = "99.9"

rows = []
column_labels = []
for bench_name, benchmark in benchmarks.items():
    for label in row_labels:
        columns = []
        row_column_labels = []
        for value in param_values[x_param].sorted():
            key = set(label)
            key.add((x_param, value))
            for unlisted_param in unlisted_params:
                key.add(param_values[unlisted_param].tuple())

            key = frozenset(key)
            if key not in benchmark.trials:
                columns.append(0)
                row_column_labels.append('n/a')
                continue
            trial = benchmark.trials[frozenset(key)]
            columns.append(trial.results[0][data_name])
            expanded = trial.expanded['concentration']
            row_column_labels.append('{}/{}/{}'.format(expanded['partitionCount'], expanded['rowCount'], expanded['valueSize']))
        rows.append(np.array(columns))
        column_labels.append(row_column_labels)
rows = np.array(rows)

def heatmap(data: np.ndarray, ax):
    im: AxesImage = ax.imshow(data,
                              cmap=cm.rainbow,
                              aspect=2,
                              norm=Normalize(vmin=data.min(), vmax=data.max(), clip=True))
    return im

def make_color_bar(im, ax, label):
    cbar = ax.figure.colorbar(im, ax=ax)
    return cbar

def annotate(data, ax, name, labels, columns):
    ax.set_title("{} [{:.4f} - {:.4f}]".format(name, data.min(), data.max()))
    ax.set_yticks(np.arange(len(labels)))
    ax.set_yticklabels([' | '.join([format_param(*t) for t in tuples]) for tuples in labels])

    ax.set_xticks(np.arange(len(columns)))
    ax.set_xticklabels(columns)
    plt.setp(ax.get_xticklabels(), rotation=-60, ha="left", rotation_mode="anchor")

class BenchmarkViz(object):
    def __init__(self, benchmark: Benchmark):
        self.benchmark = benchmark

fig_name = data_name + " ops/ms"
fig = plt.figure(figsize=[10, 8], tight_layout=True)
split = 1
spec = gridspec.GridSpec(ncols=1, nrows=split, figure=fig)
rr = 0
for (data, labels, column_names) in zip(np.split(rows, split),
                                        np.split(np.array(row_labels), split),
                                        np.split(np.array(column_labels), split)):

    ax = fig.add_subplot(spec[rr, 0])
    im = heatmap(data, ax)
    annotate(data, ax, fig_name, labels, column_names[-1])
    cbar = make_color_bar(im, ax, data_name)
    rr += 1

plt.show()
