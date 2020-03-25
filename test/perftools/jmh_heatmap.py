import argparse
import json
import os
import sys
from typing import Tuple

try:
    import numpy as np

    from visualize.benchmarks import Benchmark, Trial, ValueSet, BenchResults
    from visualize.format import ValueFormat
except ImportError as e:
    print("Some dependencies not installed. Run `pip install -r test/perftools/requirements.txt")
    sys.exit(1)


class Profile(object):
    _SHORT_NAMES = {
        'columnCount': 'columns',
        'columnType': 'col-type',
        'rowOverlap': 'overlap',
        'partitionCount': 'partitions',
        'rowCount': 'rows',
        'valueSize': 'val-sz'
    }

    _VALUE_FORMATS = {
        'columnCount': ValueFormat('cols'),
        'rowOverlap': ValueFormat('ovr', lambda v: '{:.1f}'.format(float(v))),
        'distribution': ValueFormat('dist', lambda v: v[0])
    }

    @staticmethod
    def _to_tuple(vals) -> Tuple[str]:
        if not isinstance(vals, (list, tuple)):
            return tuple(str(vals))
        return tuple((str(val) for val in vals))

    def __init__(self, name, x_params, y_params, metrics,
                 short_names=None, value_formats=None, expansions=None, split_on=None) -> None:
        super().__init__()
        self.name = name
        self.x_params = Profile._to_tuple(x_params)
        self.y_params = Profile._to_tuple(y_params)
        self.metrics = Profile._to_tuple(metrics)
        self.short_names = dict(Profile._SHORT_NAMES)
        self.short_names.update(short_names or {})
        self.value_formats = dict(Profile._VALUE_FORMATS)
        self.value_formats.update(value_formats or {})
        self.expansions = expansions or {}
        self.split_on = split_on

    def _maybe_expand(self, params):
        rval = []
        for param in params:
            if param in self.expansions:
                rval = rval + [(e, param) for e in self.expansions[param]]
            else:
                rval.append(param)
        return rval

    @property
    def expanded_x_params(self):
        return self._maybe_expand(self.x_params)

    @property
    def expanded_y_params(self):
        return self._maybe_expand(self.y_params)

    def is_graphed_dimension(self, dim):
        return dim in self.x_params or dim in self.y_params

    def format_param(self, name, value):
        return self.value_formats[name].format(value) if name in self.value_formats else str(value)

    def format_params(self, param_tuples):
        return ', '.join(['{}: {}'.format(self.short_names.get(n, n), v) for n, v in param_tuples])

    def with_formatted_params(self, params):
        result = {}
        for k, v in params.items():
            result[k + '_out'] = self.format_params(v)

        result.update(params)
        return result

    @staticmethod
    def _get_value_tuples(params, values, previous=None):
        if not params:
            return previous
        param = params[0]
        to_append = [(param, v) for v in values[param].sorted()]
        if previous is None:
            return Profile._get_value_tuples(params[1:], values, [[v] for v in to_append])
        else:
            appended = []
            for row in previous:
                for value in to_append:
                    appended.append(row + [value])

            return Profile._get_value_tuples(params[1:], values, appended)

    def get_x_labels(self, values):
        return Profile._get_value_tuples(self.x_params, values)

    def get_y_labels(self, values):
        return Profile._get_value_tuples(self.y_params, values)

    def get_row_splits(self, param_values):
        rval = []
        if self.split_on is None:
            rval.append(RowSplit(self.get_y_labels(param_values)))
        else:
            for val in sorted(param_values[self.split_on].values):
                rval.append(RowSplit(self.get_y_labels(param_values),
                                     (self.split_on, val),
                                     lambda d: d[self.split_on] == val))
        return rval


class RowSplit(object):
    def __init__(self, all_rows, split=None, filt_func=None):
        self.split = split
        self.labels = all_rows if split is None else [r for r in all_rows if filt_func({k: v for k, v in r})]


PROFILES = {
    'DataFileDeserializationBench': Profile('DataFileDeserializationBench',
                                            x_params=['concentration'],
                                            y_params=['columnType', 'valueSize', 'rowOverlap'],
                                            metrics=['score', '50.0', '99.9'],
                                            expansions={'concentration': ['partitionCount', 'rowCount', 'colCount']},
                                            split_on='columnType'),
    'CompactionIteratorBench':      Profile('CompactionIteratorBench',
                                            x_params=['concentration'],
                                            y_params=['columnType', 'streamCount', 'valueSize', 'overlap'],
                                            metrics=['score', '50.0', '99.9'],
                                            expansions={'concentration': ['partitionCount', 'rowCount', 'colCount'],
                                                        'overlap': ['rowOverlap', 'partitionOverlap']},
                                            split_on='columnType')
}

parser = argparse.ArgumentParser(prog='jmh-heatmap', description="generate interactive svg heatmap from jmh json output")
parser.add_argument('--profile', '-p', dest='profile', required=True, choices=sorted(PROFILES.keys()),
                    help='name of benchmark being visualized (add Profile entry if it isn\'t listed)')
parser.add_argument('--out', '-o', dest='output', required=True, help='output directory')
parser.add_argument('file', metavar='file', help='directory of jmh data to visualize')
parser.add_argument('file2', metavar='file2', nargs='?', help='optional second directory of jmh data.'
                                                            'If present, it will be compared against the first')

args = parser.parse_args()

profile = PROFILES[args.profile]


def require_or_die(c: bool, msg: str, *args) -> None:
    if not c:
        print(msg, *args)
        sys.exit(1)


out_dir = os.path.expanduser(args.output or 'jmhviz')
if os.path.exists(out_dir):
    require_or_die(os.path.isdir(out_dir), out_dir, "exists, but is not directory")
else:
    os.mkdir(out_dir)

directories = [args.file, args.file2] if args.file2 else [args.file]

src_paths = [os.path.expanduser(p) for p in directories]
src_names = [os.path.split(p)[-1] for p in src_paths]
require_or_die(1 <= len(src_names) <= 2, "can't visualize more than 2 files")

benchmarks = {}
param_values = {}
unlisted_params = set()

# load in the data
for src_path in src_paths:
    with open(src_path, 'r') as f:
        js = json.load(f)

    for row in js:
        bench_name = row['benchmark']
        benchmark = benchmarks.setdefault(bench_name, Benchmark(bench_name, row['primaryMetric']['scoreUnit']))
        params: dict = row['params']
        key = frozenset(params.items())

        trial = benchmark.trials.setdefault(key, Trial(params, row['expandedParams']))
        trial.add_result(row['primaryMetric'])

        for k, v in params.items():
            if k not in param_values:
                param_values[k] = ValueSet(k)

            vset = param_values[k]
            vset.add(v)

# check that there aren't any unlisted dimensions with multiple values
for k, v in param_values.items():
    if not profile.is_graphed_dimension(k):
        require_or_die(len(v.values) <= 1, k, "has multiple values but isn't graphed by", profile.name)
        unlisted_params.add(k)

row_labels = profile.get_y_labels(param_values)

bench_results = []
for bench_name, benchmark in benchmarks.items():
    for metric in profile.metrics:
        for row_split in profile.get_row_splits(param_values):
            results = BenchResults(bench_name,
                                   metric,
                                   row_split.split,
                                   benchmark.units,
                                   out_dir,
                                   row_split.labels,
                                   profile.format_param,
                                   unlisted_params)
            if row_split.split:
                k, v = row_split.split
                results.set_desc("{}: {}".format(profile.short_names.get(k, k), v))

            for run in range(len(src_paths)):
                trial_rows = []
                expanded_data = []
                param_data = []
                keys = []
                for y_values in row_split.labels:
                    columns = []
                    row_column_labels = []
                    row_params = []
                    row_keys = []
                    for x_values in profile.get_x_labels(param_values):
                        key = set(y_values + x_values)
                        for unlisted_param in unlisted_params:
                            key.add(param_values[unlisted_param].tuple())

                        def unexpand(l):
                            if isinstance(l, str): return l
                            return l[0]

                        key = frozenset(key)
                        row_keys.append(key)
                        if key not in benchmark.trials:
                            columns.append(0)
                            row_column_labels.append('n/a')
                            row_params.append(profile.with_formatted_params({
                                'x': [(unexpand(n), '?') for n in profile.expanded_x_params],
                                'y': [(unexpand(n), '?') for n in profile.expanded_y_params]
                            }))
                            continue

                        trial = benchmark.trials[frozenset(key)]
                        value = trial.results[run][metric]
                        results.update_min_max(value)
                        columns.append(value)
                        x_params = [(unexpand(n), trial.get_value(n)) for n in profile.expanded_x_params]
                        y_params = [(unexpand(n), trial.get_value(n)) for n in profile.expanded_y_params]
                        row_column_labels.append('/'.join([str(p[1] for p in x_params)]))
                        row_params.append(profile.with_formatted_params({'x': x_params, 'y': y_params}))

                    param_data.append(row_params)
                    trial_rows.append(np.array(columns))
                    keys.append(row_keys)
                    if run == 0:
                        results.column_labels.append(row_column_labels)
                results.run_data.append(trial_rows)
                results.param_data = param_data
                results.keys = keys
            results.finalize()
            bench_results.append(results)

    break
# rows = np.array(rows)

for bench_result in bench_results[:2]:
    bench_result.show()
