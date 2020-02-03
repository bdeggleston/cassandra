import argparse
import enum
import json
import os
from typing import Callable
import sys
#
# from matplotlib import pyplot as plt
# from matplotlib import cm
# from matplotlib.axes import Axes
# from matplotlib.colors import Normalize, DivergingNorm
# from matplotlib.image import AxesImage
# from matplotlib import gridspec
from skimage import color

import numpy as np

# look at https://github.com/sethoscope/heatmap

parser = argparse.ArgumentParser(description="visualize jmh json output as svg")
parser.add_argument('--prettify', '-p', dest='prettify', action='store_true', help="don't process anything, just rewrite the given json files in an indented format")
parser.add_argument('files', metavar='F', nargs='+', help='files to visualize')

args = parser.parse_args()

INDENT_SIZE = 4
INDENT = ' ' * INDENT_SIZE

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
    def __init__(self, name: str, units: str):
        self.name = name
        self.units = units
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
x_expansions = ['partitionCount', 'rowCount','valueSize']
unlisted_params = []
reported_metrics = ['score', '50.0', '99.9']
param_values = {}
param_short_names = {
    'columnCount': 'columns',
    'rowOverlap': 'overlap',
    'partitionCount': 'partitions',
    'rowCount': 'rows',
    'valueSize': 'val-sz'
}

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

def format_value(v):
    if isinstance(v, float):
        return '{:.2f}'.format(float(v))
    elif (isinstance(v, str) and str.isnumeric(v.replace('-', '').replace('.', ''))):
        is_float = '.' in v
        return '{:.2f}'.format(float(v)) if is_float else str(v)
    else:
        return str(v)

def format_param(name, value):
    return value_format[name].format(value) if name in value_format else str(value)

def is_listed_dimension(dim):
    return x_param == dim or dim in y_params

# parse and organize the data
out_dir = args.files[-1]
if os.path.exists(out_dir):
    assert os.path.isdir(out_dir)
else:
    os.mkdir(out_dir)

src_paths = args.files[:-1]
src_names = [os.path.split(p)[-1] for p in src_paths]
assert len(src_paths)

for fname in src_paths:
    with open(fname, 'r') as f:
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

class Color(object):

    def __init__(self, r, g, b):
        self.arr = np.array([int(v + 0.5) for v in [r, g, b]])
        assert all([0<=int(v)<=255 for v in self.arr]), self.arr

    @property
    def r(self):
        return self.arr[0]

    @property
    def g(self):
        return self.arr[1]

    @property
    def b(self):
        return self.arr[2]

    def darken(self, amount):
        new: np.ndarray = self.arr * amount
        new.clip(0, 255)
        return Color(*new)

    def as_svg(self):
        return 'rgb({},{},{})'.format(*[int(v+0.5) for v in (self.arr)])

    def __repr__(self):
        return 'Color({},{},{})'.format(*self.arr)


class Colors(object):
    BLACK = Color(0, 0, 0)
    PURPLE = Color(128, 0, 255)
    RED = Color(255, 0, 0)
    GREEN = Color(0, 255, 0)
    ORANGE = Color(255, 128, 0)
    YELLOW = Color(255, 255, 0)
    BLUE = Color(0, 0, 255)
    WHITE = Color(255, 255, 255)

INDENT_SIZE = 4
INDENT_STR = ' ' * INDENT_SIZE
COLOR_BAR_WIDTH = 20
X_PADDING = 30
Y_PADDING = 30
TXT_Y_PAD = 5
VAL_WIDTH = 15
VAL_HEIGHT = 30

def indent(lines, times):
    return '\n'.join([(INDENT_STR*times) + l
                      for l in (lines if isinstance(lines, (list, tuple)) else lines.split('\n'))])

def px(v):
    return str(v) + 'px'

class XML(object):
    _CURRENT_INDENT = 0

    @staticmethod
    def to_xml_props(**kwargs):
        props = []
        for k, v in kwargs.items():
            k = k.replace('_', '-').replace('classes', 'class')
            if isinstance(v, (list, tuple)):
                v = ' '.join([str(i) for i in v])

            props.append('{}="{}"'.format(k, v))
        return ' '.join(props)

    @staticmethod
    def _tag(fmt: str, indent: int, name: str, **kwargs):
        props = ' ' + XML.to_xml_props(**kwargs) if kwargs else ''
        return (INDENT_STR * indent) + fmt.format(name, props)

    @staticmethod
    def open_tag(name: str, **kwargs):
        out = XML._tag('<{}{}>', XML._CURRENT_INDENT, name, **kwargs)
        XML._CURRENT_INDENT += 1
        return out

    @staticmethod
    def close_tag(name: str, **kwargs):
        XML._CURRENT_INDENT -= 1
        return XML._tag('</{}{}>', XML._CURRENT_INDENT, name, **kwargs)

    @staticmethod
    def single_tag(name: str, **kwargs):
        return XML._tag('<{}{}/>', XML._CURRENT_INDENT, name, **kwargs)

    @staticmethod
    def indent(lines):
        return indent(lines, XML._CURRENT_INDENT)

    @staticmethod
    def text(txt, **kwargs):
        params = {'dx': 5, 'dy': -2}
        params.update(kwargs)
        return '\n'.join([
            XML.open_tag('text', **params),
            XML.indent(txt),
            XML.close_tag('text'),
        ])

class Gradient(object):

    def __init__(self, *colors):
        self.colors = colors
        self.lch_colors = color.rgb2lab([[c.arr/255 for c in self.colors]])[0]

    def color_at(self, pos):
        """
        :param pos: 0..1
        :return:
        """
        assert 0 <= pos <= 1
        if pos == 1.0:
            return self.colors[-1]

        interval = 1.0 / (len(self.colors) - 1)
        idx = int(pos / interval)
        fade = (pos - (interval * idx)) / interval
        c1 = self.lch_colors[idx]
        c2 = self.lch_colors[idx + 1]

        lch = c1 + ((c2 - c1) * fade)
        rgb = color.lab2rgb([[lch]])[0][0] * 255

        return Color(*rgb)

    def as_svg(self, name, **kwargs):
        rows = []
        kwargs['id'] = name
        interval = 1.0 / (len(self.colors) - 1)
        rows.append(XML.open_tag('linearGradient', **kwargs))
        for i, color in enumerate(self.colors):
            rows.append(XML.single_tag('stop', offset=str(int(i*interval*100)) + '%', stop_color=color.as_svg()))
        rows.append(XML.close_tag('linearGradient'))
        return '\n'.join(rows)

    def reversed(self):
        return Gradient(*reversed(self.colors))

    def _clip0_1(self, v):
        if v > 1.0:
            return 1.0
        if v < 0:
            return 0.0
        return v

    def _transform_hsv(self, idx, adj):
        hsv = color.rgb2hsv([[c.arr/255 for c in self.colors]])[0]
        for i in range(len(hsv)):
            hsv[i][idx] = self._clip0_1(hsv[i][idx] * adj)
        rgb = color.hsv2rgb([hsv])[0] * 255
        return Gradient(*[Color(*c) for c in rgb])

    def lightness(self, adj):
        return self._transform_hsv(2, adj)

    def saturation(self, adj):
        return self._transform_hsv(1, adj)



# PREDATOR = Gradient(Colors.BLACK, Colors.BLUE, Colors.GREEN, Colors.RED, Colors.WHITE)
# PREDATOR = Gradient(Colors.PURPLE, Colors.BLUE, Colors.GREEN, Colors.ORANGE, Colors.RED, Colors.WHITE)
# PREDATOR = Gradient(Colors.PURPLE, Colors.GREEN, Colors.ORANGE, Colors.RED, Colors.WHITE)
# PREDATOR = Gradient(Color(128, 0, 255), Color(0, 255, 150), Colors.RED)
# PREDATOR = Gradient(Colors.PURPLE, Colors.GREEN, Colors.RED, Colors.WHITE)
# PREDATOR = Gradient(Colors.BLUE, Colors.GREEN, Colors.RED, Colors.WHITE)
# PREDATOR = Gradient(Colors.PURPLE, Colors.BLUE, Colors.GREEN, Colors.YELLOW, Colors.RED)
# PREDATOR = Gradient(Colors.PURPLE, Colors.GREEN, Colors.RED)
PREDATOR = Gradient(Colors.PURPLE, Color(0, 255, 160), Colors.RED)
PREDATOR = Gradient(Color(128, 0, 255), Color(0, 148, 242), Color(0, 255, 160), Color(255, 180, 60), Colors.RED).lightness(0.8)
DIFF = Gradient(Colors.RED, Color(50,50,50), Colors.GREEN)

class HeatMap(object):

    BORDER_WIDTH = 1
    TITLE_HEIGHT = 40
    HIGHLIGHT_BORDER = 1
    LABEL_WIDTH = 70

    def __init__(self, data: np.ndarray, title: str, map_id: str, vmin=None, vmax=None, xlabels=None, ylabels=None, units=None, trace_color='white', gradient=PREDATOR):
        self.data = data
        self.title = title
        self.map_id = map_id
        self.title_height = HeatMap.TITLE_HEIGHT
        self.label_width = self.LABEL_WIDTH if ylabels is not None else 0
        self.xlabels = xlabels
        self.ylabels = ylabels
        self.units = units
        self.trace_color = trace_color
        self.min_val = vmin or self.data.min()
        self.max_val = vmax or self.data.max()
        self.gradient = gradient

    def dimensions(self):
        height, width = self.data.shape
        return (width*VAL_WIDTH, height*VAL_HEIGHT)

    @property
    def width(self):
        return self.data_width + self.label_width

    @property
    def height(self):
        return self.data_height + self.title_height + self.output_height

    @property
    def data_width(self):
        return self.dimensions()[0] + (self.BORDER_WIDTH * 2)

    @property
    def data_height(self):
        return self.dimensions()[1] + (self.BORDER_WIDTH * 2)

    @property
    def output_height(self):
        return VAL_HEIGHT * 4

    @property
    def output_y(self):
        return self.data_y + self.data_height

    @property
    def data_x(self):
        return self.label_width

    @property
    def data_y(self):
        return self.title_height

    def fmt_value(self, v):
        v = format_value(v)
        if self.units:
            v = v + ' ' + self.units

        return v

    def _val_x(self, idx):
        return (VAL_WIDTH*idx) + self.BORDER_WIDTH + self.data_x

    def _val_y(self, idx):
        return (VAL_HEIGHT*idx) + self.BORDER_WIDTH + self.data_y


    def as_svg(self):
        normalized = (self.data - self.min_val) / (self.max_val - self.min_val)
        height, width = self.data.shape
        lines = []
        # lines.append(XML.text(self.title, height=self.TITLE_HEIGHT, classes='hm-title'))
        lines.append(XML.text(self.title, x=self.data_x, y=self.title_height - TXT_Y_PAD, height=self.title_height, width=self.width))
        lines.append(XML.single_tag('rect',
                                    x=self.data_x, y=self.data_y,
                                    width=self.data_width, height=self.data_height,
                                    # fill='white', stroke='black',
                                    # stroke_width=self.BORDER_WIDTH,
                                    z=200,
                                    onmouseout='hmapMouseout()'))
        for x in range(width):
            for y in range(height):
                val = self.data[y][x]
                norm = normalized[y][x]
                color = self.gradient.color_at(norm)
                lines.append(XML.open_tag('rect',
                                          x=self._val_x(x),
                                          y=self._val_y(y),
                                          fill=color.as_svg(),
                                          stroke=color.darken(0.97).as_svg(),
                                          stroke_width=1,
                                          width=VAL_WIDTH,
                                          height=VAL_HEIGHT,
                                          onmouseover="dataMouseover('{}', {}, {})".format(self.map_id, x, y),
                                          onmouseout="dataMouseout('{}', {}, {})".format(self.map_id, x, y),
                                          ))
                lines.append(XML.open_tag('title'))
                lines.append(XML.indent(self.fmt_value(str(val))))
                lines.append(XML.close_tag('title'))
                lines.append(XML.close_tag('rect'))

        # make labels
        if self.ylabels is not None:
            lines.append(XML.single_tag('line', x1=0, x2=self.label_width, y1=self._val_y(0) - 1, y2=self._val_y(0), stroke='grey'))
            # for i, label in enumerate([' | '.join([format_param(*t) for t in tuples]) for tuples in self.ylabels]):
            for i, label_pairs in enumerate(self.ylabels):
                lines.append(XML.open_tag('text',
                                          x=self.data_x,
                                          y=self._val_y(i + 1) - (VAL_HEIGHT/3),
                                          height=VAL_HEIGHT,
                                          width=self.label_width,
                                          dx=-5,
                                          text_anchor='end',
                                          classes='row-label'))
                lines.append(XML.indent(' | '.join([format_param(*t) for t in label_pairs])))
                lines.append(XML.open_tag('title'))
                # title = '\n'.join(['{} = {}'.format(k, format_param(k, v)) for k, v in self.ylabels])
                lines.append(XML.indent('\n'.join(['{} = {}'.format(k, format_param(k, v)) for k, v in label_pairs])))
                lines.append(XML.close_tag('title'))
                lines.append(XML.close_tag('text'))
                lines.append(XML.single_tag('line', x1=0, x2=self.label_width, y1=self._val_y(i+1), y2=self._val_y(i+1), stroke='grey'))

        # make highlights
        for x in range(width):
            lines.append(XML.single_tag('rect',
                                        width=VAL_WIDTH,
                                        height=self.data_height,
                                        y=self.data_y,
                                        x=self._val_x(x),
                                        fill='none',
                                        stroke=self.trace_color,
                                        stroke_width=self.HIGHLIGHT_BORDER,
                                        opacity=0,
                                        classes='col{}'.format(x),
                                        z=100))

        for y in range(height):
            lines.append(XML.single_tag('rect',
                                        width=self.data_width,
                                        height=VAL_HEIGHT,
                                        y=self._val_y(y),
                                        x=self.data_x,
                                        fill='none',
                                        stroke=self.trace_color,
                                        stroke_width=self.HIGHLIGHT_BORDER,
                                        opacity=0,
                                        classes='row{}'.format(y),
                                        z=100))
        # lines.append(XML.close_tag('rect'))
        lines.append(XML.text('', x=self.data_x, y=self.output_y + VAL_HEIGHT, height=VAL_HEIGHT, width=self.data_width, dy=-10, id=self.map_id + '-xy-out'))
        lines.append(XML.text('', x=self.data_x, y=self.output_y + (VAL_HEIGHT * 2), height=VAL_HEIGHT, width=self.data_width, dy=-10, id=self.map_id + '-data-out'))
        lines.append(XML.text('', x=self.data_x, y=self.output_y + (VAL_HEIGHT * 3), height=VAL_HEIGHT, width=self.data_width, dy=-10, id=self.map_id + '-x-out', classes="row-label"))
        lines.append(XML.text('', x=self.data_x, y=self.output_y + (VAL_HEIGHT * 4), height=VAL_HEIGHT, width=self.data_width, dy=-10, id=self.map_id + '-y-out', classes="row-label"))

        return '\n'.join(lines)

class SVGSnippet(object):
    def __init__(self, markup, x, y, width, height):
        self.markup = '\n'.join(markup) if isinstance(markup, (list, tuple)) else markup
        self.x = x
        self.y = y
        self.width = width
        self.height = height


class BenchResults(object):

    # VSPLITS = [1, 4]
    VSPLITS = [1]
    # VSPLITS = [4]

    def __init__(self, name, metric, units):
        self.name = name
        self.metric = metric
        self.map_id = 'name' + '-' + metric
        self.units = units
        self.unit = None
        self.run_data = []
        self.diff_data = []
        self.column_labels = []
        self.param_data = None
        self.finalized = False

    def finalize(self):
        assert len(self.run_data) in [1, 2], len(self.run_data)
        self.run_data = np.array(self.run_data)
        self.column_labels = np.array(self.column_labels)
        old, new = self.run_data
        if len(self.run_data) > 1:
            self.diff_data = ((new - old) / new) * 100
        self.finalized = True

    @property
    def x_plots(self):
        assert self.finalized
        return 1 if len(self.run_data) == 1 else 3

    def _set_yticks(self, ax, labels):
        ax.set_yticks(np.arange(len(labels)))
        ax.set_yticklabels([' | '.join([format_param(*t) for t in tuples]) for tuples in labels])
        pass

    def show(self):
        for num_splits in self.VSPLITS:
            out_name = "{}-{}.svg".format(self.metric, num_splits)
            doc_rows = []
            defs = []
            value_data = {}
            for split in range(num_splits):
                split_data = [np.split(d, num_splits)[split] for d in self.run_data]
                data_min = min(*[d.min() for d in split_data])
                data_max = max(*[d.max() for d in split_data])

                heat_maps = []
                for data_idx, data in enumerate(split_data):
                    ylabels = np.split(np.array(row_labels), num_splits)[split]
                    xlabels = np.split(np.array(self.column_labels), num_splits)[split]
                    map_id = (self.map_id + str(data_idx)).replace('.', '')
                    heat_map = HeatMap(data,
                                       "({:.4f} - {:.4f})".format(data.min(), data.max()),
                                       map_id,
                                       vmin=data_min,
                                       vmax=data_max,
                                       units=self.units,
                                       xlabels=xlabels,
                                       ylabels=ylabels if data_idx == 0 else None)
                    heat_maps.append(heat_map)

                    # format data for mouseover output
                    fmt_data = []
                    for row in data:
                        fmt_row = []
                        for column in row:
                            fmt_row.append(heat_map.fmt_value(column))
                        fmt_data.append(fmt_row)
                    value_data[map_id] = fmt_data


                split_diff = np.split(self.diff_data, num_splits)[split] if len(self.diff_data) else None
                # norm = DivergingNorm(vcenter=0, vmin=split_diff.min() or -1, vmax=split_diff.max() or 1)
                diff_range = max(abs(split_diff.min()), abs(split_diff.max())) or 100.0


                map_id = (self.map_id + '-diff').replace('.', '')
                heat_map = HeatMap(split_diff,
                                   "({:.4f}% - {:.4f}%)".format(split_diff.min(), split_diff.max()),
                                   map_id,
                                   gradient=DIFF,
                                   vmin=-diff_range,
                                   vmax=diff_range,
                                   units='%')
                heat_maps.append(heat_map)

                # format data for mouseover output
                fmt_data = []
                for row in split_diff:
                    fmt_row = []
                    for column in row:
                        col =heat_map.fmt_value(column)
                        if (column > 0): col = '+' + col

                        fmt_row.append(col)
                    fmt_data.append(fmt_row)
                value_data[map_id] = fmt_data


                width = 0
                height = 0
                markup = []
                for i, heat_map in enumerate(heat_maps):
                    # padding = X_PADDING if i > 0 else 0
                    add_color_bar = i > 0
                    padding = X_PADDING
                    element_y = Y_PADDING
                    element_height = heat_map.height + Y_PADDING
                    markup.append(XML.open_tag('svg', x=width + padding, y=element_y, height=element_height, width=heat_map.width))
                    markup.append(heat_map.as_svg())
                    markup.append(XML.close_tag('svg'))
                    width += heat_map.width + (padding / 2 if add_color_bar else padding)
                    height = max(height, element_height + Y_PADDING)

                    # add color bar
                    if i > 0:
                        gradient_name = 'gradient-' + str(i)
                        defs.append(heat_map.gradient.reversed().as_svg(gradient_name, gradientTransform="rotate(90)"))
                        markup.append(XML.open_tag('svg', x=width + padding, y=element_y, height=heat_map.height, width=COLOR_BAR_WIDTH, transform="rotate(180)"))
                        markup.append(XML.single_tag('rect',
                                                     y=heat_map.data_y,
                                                     width=COLOR_BAR_WIDTH,
                                                     height=heat_map.data_height,
                                                     fill='url(#' + gradient_name + ')'))
                        markup.append(XML.close_tag('svg'))
                        width += COLOR_BAR_WIDTH + padding
                width += X_PADDING



                doc_rows.append(SVGSnippet('\n'.join(markup), 0, 0, width, height))


            markup = []
            width = 0
            height = 0
            for row in doc_rows:
                row: SVGSnippet
                markup.append(XML.open_tag('svg', x=0, y=height, height=row.height, width=row.width))
                markup.append(XML.indent(row.markup))
                markup.append(XML.close_tag('svg'))
                height += row.height
                width = max(width, row.width)

            resource_path = os.path.join(*os.path.split(sys.argv[0])[:-1])
            with open(os.path.join(resource_path, '_perf.js')) as f:
                script = '// <![CDATA[\n' + f.read() + '\n// ]]>'
            with open(os.path.join(resource_path, '_perf.css')) as f:
                style = f.read()

            doc_body = SVGSnippet(markup, 0, 0, width, height)
            output = '\n'.join([
                XML.open_tag('svg',
                             viewBox='0 0 {} {}'.format(width, height),
                             xmlns="http://www.w3.org/2000/svg",
                             height=px(height),
                             width=px(width),
                             fill='rgb(50,50,50'),
                XML.open_tag('script'),
                XML.indent(script),
                XML.indent('valueData = {};'.format(json.dumps(value_data, indent=4, sort_keys=True))),
                XML.indent('paramData = {};'.format(json.dumps(self.param_data, indent=4, sort_keys=True))),
                XML.close_tag('script'),
                XML.open_tag('style'),
                XML.indent(style),
                XML.close_tag('style'),
                XML.open_tag('defs'),
                XML.indent(defs),
                XML.close_tag('defs'),
                XML.single_tag('rect', width='100%', height='100%', fill='rgb(50, 50, 50'),
                XML.indent(doc_body.markup),
                XML.close_tag('svg'),
            ])

            out_path = os.path.join(out_dir, out_name)
            with open(out_path, 'w') as f:
                print('writing', out_path)
                f.write(output)

def format_params(param_tuples):
    return ', '.join(['{}: {}'.format(param_short_names.get(n, n), v) for n, v in param_tuples])

def with_formatted_params(params: dict):
    result = {}
    for k, v in params.items():
        result[k + '_out'] = format_params(v)

    result.update(params)
    return result

bench_results = []
for bench_name, benchmark in benchmarks.items():
    for metric in reported_metrics:
        results = BenchResults(bench_name, metric, benchmark.units)
        for run in range(len(src_paths)):
            trial_rows = []
            expanded_data = []
            param_data = []
            for label in row_labels:
                columns = []
                row_column_labels = []
                row_params = []
                for value in param_values[x_param].sorted():
                    key = set(label)
                    key.add((x_param, value))
                    for unlisted_param in unlisted_params:
                        key.add(param_values[unlisted_param].tuple())

                    key = frozenset(key)
                    if key not in benchmark.trials:
                        columns.append(0)
                        row_column_labels.append('n/a')
                        row_params.append(with_formatted_params({
                            'y': [(n, '?') for n in y_params],
                            'x': [(n, '?') for n in x_expansions]
                        }))
                        continue
                    trial = benchmark.trials[frozenset(key)]
                    columns.append(trial.results[run][metric])
                    expanded = trial.expanded['concentration']
                    row_column_labels.append('{}/{}/{}'.format(expanded['partitionCount'], expanded['rowCount'], expanded['valueSize']))
                    row_params.append(with_formatted_params({
                        'y': [(n, trial.params[n]) for n in y_params],
                        'x': [(n, expanded[n]) for n in x_expansions]
                    }))
                param_data.append(row_params)
                trial_rows.append(np.array(columns))
                if run == 0:
                    results.column_labels.append(row_column_labels)
            results.run_data.append(trial_rows)
            results.param_data = param_data
        results.finalize()
        bench_results.append(results)

    break
# rows = np.array(rows)

for bench_result in bench_results:
    bench_result.show()
    # break


