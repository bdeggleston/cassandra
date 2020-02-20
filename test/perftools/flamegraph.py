import argparse
import os
import time

from visualize.colors import BG_COLOR
from visualize.common import get_common_css
from visualize.flamegraph import Flamegraph, get_flamegraph_css
from visualize.flamegraph import get_flamegraph_js
from visualize.svg import XML

parser = argparse.ArgumentParser(description="jbpf profiler flamegraph")
parser.add_argument('--out', '-o', dest='output', required=True, help='output file')
parser.add_argument('--from', '-f', dest='from_frame', help='generate graph from this frame up')
parser.add_argument('file', metavar='F', nargs='+', help='file of jmh data to visualize')

start = time.time()
args = parser.parse_args()

height = 800
fg_width = 1000 if len(args.file) == 1 else 700
width = fg_width

fg = Flamegraph("fg", fg_width, height)
fg.parse_file(os.path.expanduser(args.file[0]), from_frame=args.from_frame)

fg2 = None
if len(args.file) > 1:
    fg2 = Flamegraph("fg2", fg_width, height)
    fg2.parse_file(os.path.expanduser(args.file[1]), from_frame=args.from_frame)
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

