import json
import os
import re
from string import Template

import numpy as np

from .colors import BG_COLOR, Gradients
from .common import get_common_css
from .format import format_percent
from .heatmap import HeatMap
from .svg import XML, SVGSnippet, JOINER

COLOR_BAR_WIDTH = 20
X_PADDING = 30
Y_PADDING = 30
TXT_Y_PAD = 5


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

    def __getitem__(self, item):
        if item in self.params:
            return self.params[item]
        if item in self.expanded:
            return self.expanded[item]
        raise KeyError(item)

    def get_value(self, key):
        if isinstance(key, str):
            return self.params[key]
        if isinstance(key, tuple):
            return self.expanded[key[1]][key[0]]


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


class BenchResults(object):

    # VSPLITS = [1, 4]
    VSPLITS = [1]
    # VSPLITS = [4]

    def __init__(self, name, metric, split, units, out_dir, row_labels, format_param, unlisted_params):
        self.name = name
        self.metric = metric
        self.split = split
        self.map_id = 'name' + '-' + metric
        self.units = units
        self.out_dir = out_dir
        self.row_labels = row_labels
        self.format_param = format_param
        self.unlisted_params = unlisted_params
        self.unit = None
        self.run_data = []
        self.diff_data = None
        self.column_labels = []
        self.keys = []
        self.param_data = None
        self.finalized = False
        self.desc = None
        self.data_min = None
        self.data_max = None

    def update_min_max(self, v):
        self.data_min = v if self.data_min is None else min(self.data_min, v)
        self.data_max = v if self.data_max is None else max(self.data_max, v)

    def set_desc(self, desc):
        self.desc = desc

    def finalize(self):
        assert len(self.run_data) in [1, 2], len(self.run_data)
        self.run_data = np.array(self.run_data)
        self.column_labels = np.array(self.column_labels)
        if len(self.run_data) > 1:
            old, new = self.run_data
            self.diff_data = np.nan_to_num(((new - old) / old) * 100)
        self.finalized = True

    @property
    def x_plots(self):
        assert self.finalized
        return 1 if len(self.run_data) == 1 else 3

    def _set_yticks(self, ax, labels):
        ax.set_yticks(np.arange(len(labels)))
        ax.set_yticklabels([' | '.join([self.format_param(*t) for t in tuples]) for tuples in labels])
        pass

    @property
    def fname(self):
        if self.split is None:
            return self.metric + '.svg'
        return self.metric + '-' + '-'.join(self.split) + '.svg'

    def _header(self):
        height = 25
        lines = [XML.text(self.name, classes='report-title', y=height)]
        height += 20
        if self.desc is not None:
            lines.append(XML.text(self.desc, classes='report-desc', y=height))
            height += 20

        height += 5
        lines.append(XML.text('', y=height, id='x-out', classes="report-xy"))
        height += 20
        lines.append(XML.text('', y=height, id='y-out', classes="report-xy"))
        height += 25
        lines.append(XML.text('', y=height, id='params-out', classes="report-params"))
        height += 5
        return SVGSnippet(JOINER.join(lines), 0, 0, 0, height)

    def show(self):
        doc_rows = []
        defs = []
        value_data = {}

        heat_maps = []
        for data_idx, data in enumerate(self.run_data):
            map_id = (self.map_id + str(data_idx)).replace('.', '')
            heat_map = HeatMap(data,
                               "({:.4f} - {:.4f})".format(self.data_min, self.data_max),
                               map_id,
                               self.format_param,
                               vmin=self.data_min,
                               vmax=self.data_max,
                               units=self.units,
                               xlabels=self.column_labels)
            heat_maps.append(heat_map)

            # format data for mouseover output
            fmt_data = []
            for row in data:
                fmt_row = []
                for column in row:
                    fmt_row.append(heat_map.fmt_value(column))
                fmt_data.append(fmt_row)
            value_data[map_id] = fmt_data

        if self.diff_data is not None:
            # norm = DivergingNorm(vcenter=0, vmin=split_diff.min() or -1, vmax=split_diff.max() or 1)
            diff_range = max(abs(self.diff_data.min()), abs(self.diff_data.max())) or 100.0
            map_id = (self.map_id + '-diff').replace('.', '')
            heat_map = HeatMap(self.diff_data,
                               "({} - {})".format(format_percent(self.diff_data.min()),
                                                  format_percent(self.diff_data.max())),
                               map_id,
                               self.format_param,
                               gradient=Gradients.DIFF,
                               vmin=-diff_range,
                               vmax=diff_range,
                               units='%')
            heat_maps.append(heat_map)

            # format data for mouseover output
            fmt_data = []
            for row in self.diff_data:
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
            element_y = 0
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

        doc_rows.append(SVGSnippet(JOINER.join(markup), 0, 0, width, height))

        width = 0
        height = 0
        for row in doc_rows:
            row: SVGSnippet
            markup.append(XML.open_tag('svg', x=0, y=height, height=row.height, width=row.width))
            markup.append(XML.indent(row.markup))
            markup.append(XML.close_tag('svg'))
            height += row.height
            width = max(width, row.width)

        def is_float(v):
            if '.' not in v:
                return False
            try:
                float(v)
                return True
            except ValueError:
                return False

        param_args = []
        for key_row in self.keys:
            row_params = []
            for key in key_row:
                args = []
                for k, v in sorted(key, key=lambda k: k[0]):
                    if k in self.unlisted_params:
                        continue
                    if is_float(v):
                        v = '{:.4f}'.format(float(v))

                    args.append('-p {}={}'.format(k, v))
                row_params.append(' '.join(args))
            param_args.append(row_params)

        script = get_benchmark_js(value_data, self.param_data, param_args)
        style = get_benchmark_css()

        # make header
        header = self._header()
        hmap = SVGSnippet(markup, 0, 0, width, height)
        header_svg = JOINER.join([XML.open_tag('svg', x=40), XML.indent(header.markup), XML.close_tag('svg')])
        hmap_svg = JOINER.join([XML.open_tag('svg', y=header.height), XML.indent(hmap.markup), XML.close_tag('svg')])
        output = JOINER.join([
            XML.open_tag('svg',
                         viewBox='0 0 {} {}'.format(width, height + header.height),
                         xmlns="http://www.w3.org/2000/svg",
                         # height=px(height),
                         # width=px(width),
                         fill=BG_COLOR.as_svg()),
            XML.open_tag('script'),
            XML.indent(script),
            XML.close_tag('script'),
            XML.open_tag('style'),
            XML.indent(style),
            XML.close_tag('style'),
            XML.open_tag('defs'),
            XML.indent(defs),
            XML.close_tag('defs'),
            XML.single_tag('rect', x=0, y=0, width='100%', height='100%', fill=BG_COLOR.as_svg()),
            XML.indent(header_svg),
            XML.indent(hmap_svg),
            XML.close_tag('svg'),
        ])

        out_path = os.path.join(self.out_dir, self.fname)
        with open(out_path, 'w') as f:
            print('writing', out_path)
            f.write(output)


def get_benchmark_css():
    with open(re.sub(r'\.py$', '.css', __file__)) as f:
        contents = f.read();
    return get_common_css() + '\n' + Template(contents).substitute()


def get_benchmark_js(value_data, param_data, param_args):
    with open(re.sub(r'\.py$', '.js', __file__)) as f:
        contents = f.read()
    rendered = Template(contents).substitute(VALUE_DATA=json.dumps(value_data, sort_keys=True),
                                             PARAM_DATA=json.dumps(param_data, sort_keys=True),
                                             PARAM_ARGS=json.dumps(param_args, sort_keys=True))
    return '// <![CDATA[\n' + rendered + '\n// ]]>'
