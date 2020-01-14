import re
from enum import Enum
from string import Template
from typing import List

from visualize.colors import Gradient, Gradients, BG_COLOR
from visualize.svg import JOINER

FONT_SIZE = 8

FG_OUTPUT_ATTR = 'fo'
FG_PARENT_PERCENT_ATTR = 'fpp'
FG_PARENT_DIFF_ATTR = 'fpd'
FG_TOTAL_PERCENT_ATTR = 'ftp'
FG_TOTAL_DIFF_ATTR = 'ftd'
FG_SAMPLES_ATTR = 'fs'
FG_LEVEL_ATTR = 'fl'
FG_TEXT_ATTR = 'ft'


def _flt(v):
    return '{:.2f}'.format(v)


def _pct(v):
    return '{:.2f}%'.format(v)


def _escape(s):
    if '&' in s: s = s.replace("&", "&amp;")
    if '>' in s: s = s.replace(">", "&gt;")
    if '<' in s: s = s.replace("<", "&lt;")
    return s


class NodeType(Enum):
    KERNEL = 'kern'
    JVM = 'jvm'
    JAVA = 'java'
    UNKNOWN = 'unk'


class Node(object):

    def __init__(self, parent, name, level, graph):
        self.parent = parent
        if name is None:
            self.name = None
            self.type = None
            self.long_name = self.name
        elif name.startswith('::'):
            self.name = name
            self.long_name = parent.name + name
            self.type = parent.type
        elif name.startswith('L'):
            self.name = name[1:].replace('/', '.')
            self.type = NodeType.JAVA
            self.long_name = self.name
        elif name.endswith('[k]'):
            self.name = name[:-3]
            self.type = NodeType.KERNEL
            self.long_name = self.name
        else:
            self.name = name
            self.type = NodeType.JVM
            self.long_name = self.name

        self.level = level
        self.graph = graph
        self.samples = 0
        self.children = {}

        self.compared = False
        self.parent_diff = None
        self.total_diff = None

        self.parent_percent = None
        self.total_percent = None

    def __repr__(self) -> str:
        return "Node{" + self.name + "}"

    def calculate_totals(self, total_samples):
        self.parent_percent = float(self.samples) / float(self.parent.samples) if self.parent else 1.0
        self.total_percent = float(self.samples) / float(total_samples)
        for child in self.children.values():
            child.calculate_totals(total_samples)

    def add(self, stack: List[str], samples: int):
        self.samples += samples

        while True:
            if not stack:
                return

            # skip jmh boilerplate
            if self.name == "call_stub" and "::perform" in stack:
                stack = stack[1:]
                continue

            next = stack[0]
            stack = stack[1:]

            if next in ['Interpreter']:
                continue

            if "JavaCalls::call_" in next:
                continue

            if next not in self.children:
                node = Node(self, next, self.level + 1, self.graph)
                self.children[next] = node
                if self.name is not None and next.startswith('::'):
                    node.long_name = self.name + node.name
                    node.type = self.type

            next_node = self.children[next]
            next_node.add(stack, samples)
            return

    def _as_rect(self, x, y, width, height, level, **r_kwargs):
        if self.name is None:
            return ''

        title_text = "[{}] {}".format(self.type.name, _escape(self.long_name))
        color = self.graph.colors.fill(self)
        g_kwargs = {
            FG_OUTPUT_ATTR:         title_text,
            FG_PARENT_PERCENT_ATTR: _flt(self.parent_percent*100),
            FG_PARENT_DIFF_ATTR:    _flt(self.parent_diff*100) if self.parent_diff else "",
            FG_TOTAL_PERCENT_ATTR:  _flt(self.total_percent*100),
            FG_TOTAL_DIFF_ATTR:     _flt(self.total_diff*100) if self.total_diff else "",
            FG_SAMPLES_ATTR:        self.samples,
            FG_TEXT_ATTR:           _escape(self.name)
        }
        r_kwargs[FG_LEVEL_ATTR]=level
        out = [
            XML.open_tag('g', **g_kwargs),
            XML.open_tag('rect',
                         x=x, y=y,
                         width=width, height=height,
                         # stroke='black', fill=color.as_svg(),
                         fill=color.as_svg(),
                         **r_kwargs),
            XML.close_tag('rect'),
            # XML.text("", x=x, y=y, dy=(height/2) + 2),
            XML.close_tag('g')
        ]
        return JOINER.join(out)

    def as_svg(self, level: int, remaining: int, x: int, width: int, height: int):
        output = [self._as_rect(x, remaining*height, max(1, width - 1), height - 1, level)]

        child_samples = sum([c.samples for c in self.children.values()])
        start_x = x + (float(width) * (float(self.samples - child_samples) / self.samples))
        for key in sorted(self.children.keys()):
            child = self.children[key]
            child_width = float(width) * (float(child.samples) / float(self.samples) )
            output.append(child.as_svg(level + 1, remaining - 1, start_x, child_width, height))
            start_x += child_width
        return JOINER.join(output)

    def depth(self, min_samples=0):
        if self.samples < min_samples:
            return 0
        if not self.children:
            return 1
        return 1 + max([c.depth(min_samples=min_samples) for c in self.children.values()])

    @property
    def min_diff(self):
        return min([self.parent_diff or 0, self.total_diff or 0] + [c.min_diff for c in self.children.values()])

    @property
    def max_diff(self):
        return max([self.parent_diff or 0, self.total_diff or 0] + [c.max_diff for c in self.children.values()])

    def _compare_to(self, that):
        that: Node = that
        self.parent_diff = (self.parent_percent - that.parent_percent) / that.parent_percent
        self.total_diff = (self.total_percent - that.total_percent) / that.total_percent
        self.compared = True

    @staticmethod
    def calculate_diffs(left, right):
        left: Node = left
        right: Node = right

        if left is None:
            right.diff = None
            right.compared = True
            return

        if right is None:
            left.diff = None
            left.compared = True
            return

        if left.name is None:
            assert right.name is None
        else:
            left._compare_to(right)
            right._compare_to(left)

        all_children = set(left.children.keys()).union(right.children.keys())
        for key in all_children:
            Node.calculate_diffs(left.children.get(key), right.children.get(key))


class FlameColors(object):
    MID_COLOR = BG_COLOR.lightness(0.5)
    GRADIENT = Gradients.PREDATOR.reversed()

    def __init__(self, max_depth):
        self.max_depth = max_depth

    def fill(self, node: Node):
        pos = float(node.level) / float(self.max_depth)
        return FlameColors.GRADIENT.color_at(pos)


class DiffColors(object):

    GRADIENT = Gradient(Gradients.DIFF.colors[0].saturation(0.8).lightness(0.8),
                        Gradients.DIFF.colors[1],
                        Gradients.DIFF.colors[2].saturation(0.8).lightness(0.8))
    NEW = Gradients.PREDATOR.colors[0].saturation(0.8).lightness(0.8)

    def fill(self, node: Node):
        diff = node.parent_diff
        if diff is None:
            return DiffColors.NEW

        pos = diff / 2
        pos = 0.5 - pos
        pos = max(0, min(1, pos))
        return DiffColors.GRADIENT.color_at(pos)


class Flamegraph(object):
    SPLIT_COUNT = re.compile(r'(.*) (\d+)$')

    def __init__(self, fg_id, width, height):
        self.fg_id = fg_id
        self.root = Node(None, None, 0, self)
        self.width = width
        self.height = height
        self.colors = None
        self.norm_depth = None

    def _find_first_frame(self, regex, stack):
        for i, frame in enumerate(stack):
            if regex.search(frame):
                return i
        return -1

    def parse_data(self, data, from_frame=None):
        from_frame = re.compile(from_frame) if from_frame else None
        for line in data.split('\n'):
            groups = Flamegraph.SPLIT_COUNT.search(line)
            if not groups:
                continue

            stack, count = groups.groups()
            stack = stack.split(';')
            if from_frame:
                idx = self._find_first_frame(from_frame, stack)
                if idx < 0:
                    continue
                stack = stack[idx:]

            self.root.add(stack, int(count))
        self.root.calculate_totals(self.root.samples)
        self.colors = FlameColors(self.root.depth())

    def parse_file(self, fname, from_frame=None):
        with open(fname) as f:
            return self.parse_data(f.read(), from_frame=from_frame)

    @property
    def total_samples(self):
        return self.root.samples

    def _header(self):
        return JOINER.join([
            XML.open_tag('svg', width=_pct(100), height=_pct(5)),

            XML.close_tag('svg')
        ])

    def _footer(self):
        out = JOINER.join([
            XML.open_tag('svg', id=self.fg_id + '-foot', width=_pct(100), height=_pct(5), x=0, y=_pct(95)),
            XML.single_tag('rect', width=_pct(100), height=_pct(100), stroke='black'),
            XML.text('', id=self.fg_id + '-output', classes='fg-output', x=15, y=_pct(30)),
            # XML.open_tag('a', href='#', id=self.fg_id + '-unzoom'),
            XML.text('', id=self.fg_id+'-detail', classes='fg-reset', x=15, y=_pct(90)),
            XML.text('Reset Zoom', id=self.fg_id + '-unzoom', classes='fg-reset', x=_pct(80), y=_pct(90)),
            # XML.close_tag('a'),
            XML.close_tag('svg')
        ])
        return out

    @property
    def _depth(self):
        return self.root.depth(int(self.total_samples * 0.001))

    def _graph(self):
        graph_depth = self.norm_depth or self._depth
        height = float(self.height) / graph_depth
        total_samples = self.root.samples
        graph_id = self.fg_id + '-graph'

        return JOINER.join([
            XML.open_tag('svg',
                         viewBox="0 0 {} {}".format(self.width, self.height),
                         id=graph_id,
                         fg_vb_width=self.width,
                         fg_vb_height=self.height,
                         fg_depth=graph_depth,
                         classes='flame-graph',
                         x=0, y=0, width=_pct(100), height=_pct(95)),
            self.root.as_svg(0, graph_depth, 0, self.width, height),
            XML.close_tag('svg')
        ])

    def as_svg(self, **kwargs):
        out = JOINER.join([
            XML.open_tag('svg',
                         viewBox="0 0 {} {}".format(self.width, self.height),
                         onload="fg_init('{}')".format(self.fg_id),
                         width=self.width,
                         height=self.height,
                         **kwargs),
            self._graph(),
            self._footer(),
            XML.close_tag('svg')
        ])
        return out

    def _setup_for_diffs(self):
        self.colors = DiffColors()

    @staticmethod
    def calculate_diffs(left, right):
        Node.calculate_diffs(left.root, right.root)
        left._setup_for_diffs()
        right._setup_for_diffs()
        norm_depth = max(left._depth, right._depth)
        left.norm_depth = norm_depth
        right.norm_depth = norm_depth

_template_ctx = dict(FONT_SIZE=FONT_SIZE,
                     FG_OUTPUT_ATTR=FG_OUTPUT_ATTR,
                     FG_PARENT_PERCENT_ATTR=FG_PARENT_PERCENT_ATTR,
                     FG_PARENT_DIFF_ATTR=FG_PARENT_DIFF_ATTR,
                     FG_TOTAL_PERCENT_ATTR=FG_TOTAL_PERCENT_ATTR,
                     FG_TOTAL_DIFF_ATTR=FG_TOTAL_DIFF_ATTR,
                     FG_SAMPLES_ATTR=FG_SAMPLES_ATTR,
                     FG_LEVEL_ATTR=FG_LEVEL_ATTR,
                     FG_TEXT_ATTR=FG_TEXT_ATTR)

assert len(set(_template_ctx.values())) == len(_template_ctx)

def get_flamegraph_css():
    with open(re.sub(r'\.py$', '.css', __file__)) as f:
        contents = f.read()
    return Template(contents).substitute(**_template_ctx)


def get_flamegraph_js():
    with open(re.sub(r'\.py$', '.js', __file__)) as f:
        contents = f.read()
    return '// <![CDATA[\n' + Template(contents).substitute(**_template_ctx) + '\n// ]]>'


if __name__ == '__main__':
    import argparse
    import os
    import time

    from visualize.colors import BG_COLOR
    from visualize.common import get_common_css
    from visualize.svg import XML

    parser = argparse.ArgumentParser(description="jbpf profiler flamegraph")
    parser.add_argument('--out', '-o', dest='output', required=True, help='output file')
    parser.add_argument('file', metavar='F', nargs='+', help='file of jmh data to visualize')

    start = time.time()
    args = parser.parse_args()

    height = 800
    fg_width = 1000 if len(args.file) == 1 else 700
    width = fg_width

    fg = Flamegraph("fg", fg_width, height)
    fg.parse_file(os.path.expanduser(args.file[0]))

    fg2 = None
    if len(args.file) > 1:
        fg2 = Flamegraph("fg2", fg_width, height)
        fg2.parse_file(os.path.expanduser(args.file[1]))
        Flamegraph.calculate_diffs(fg, fg2)
        width += fg_width


    script = get_flamegraph_js()
    style = get_common_css() + '\n' + get_flamegraph_css()

    output = '\n'.join([
        XML.open_tag('svg',
                     viewBox='0 0 {} {}'.format(width, height),
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
        XML.single_tag('rect', x=0, y=0, width='100%', height='100%', fill=BG_COLOR.as_svg()),
        fg.as_svg(),
        fg2.as_svg(x=fg_width) if fg2 else '',
        XML.close_tag('svg'),
    ])

    with open(os.path.expanduser(args.output), 'w') as f:
        f.write(output)


    print ("completed in ", time.time() - start, "seconds")
