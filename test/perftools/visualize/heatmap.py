import numpy as np

from .colors import Gradients
from .format import format_value
from .svg import XML, JOINER

VAL_WIDTH = 20
VAL_HEIGHT = 25
TXT_Y_PAD = 5


class HeatMap(object):

    BORDER_WIDTH = 1
    TITLE_HEIGHT = 40
    HIGHLIGHT_BORDER = 1
    LABEL_WIDTH = 150

    def __init__(self, data: np.ndarray, title: str, map_id: str, format_param, vmin=None, vmax=None, xlabels=None, ylabels=None, units=None, trace_color='white', gradient=Gradients.PREDATOR):
        self.data = data
        self.title = title
        self.map_id = map_id
        self.format_param = format_param
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
        return width * VAL_WIDTH, height * VAL_HEIGHT

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
        normalized = np.clip((self.data - self.min_val) / (self.max_val - self.min_val), a_min=0, a_max=1)
        height, width = self.data.shape
        lines = []
        lines.append(XML.text(self.title, x=self.data_x, y=self.title_height - TXT_Y_PAD, height=self.title_height, width=self.width))
        lines.append(XML.single_tag('rect',
                                    x=self.data_x, y=self.data_y,
                                    width=self.data_width, height=self.data_height,
                                    z=200,
                                    onmouseout='hmapMouseout()'))
        DATA_STROKE = 1
        for x in range(width):
            for y in range(height):
                val = self.data[y][x]
                norm = normalized[y][x]
                color = self.gradient.color_at(norm)
                lines.append(XML.open_tag('rect',
                                          x=self._val_x(x),
                                          y=self._val_y(y),
                                          fill=color.as_svg(),
                                          stroke=color.lightness(0.97).as_svg(),
                                          stroke_width=DATA_STROKE,
                                          width=VAL_WIDTH,
                                          height=VAL_HEIGHT,
                                          onmouseover="dataMouseover('{}', {}, {})".format(self.map_id, x, y),
                                          onmouseout="dataMouseout('{}', {}, {})".format(self.map_id, x, y),
                                          onclick='dataClick();'
                                          ))
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
                lines.append(XML.indent(' | '.join([self.format_param(*t) for t in label_pairs])))
                lines.append(XML.open_tag('title'))
                # title = JOINER.join(['{} = {}'.format(k, format_param(k, v)) for k, v in self.ylabels])
                lines.append(XML.indent(JOINER.join(['{} = {}'.format(k, self.format_param(k, v)) for k, v in label_pairs])))
                lines.append(XML.close_tag('title'))
                lines.append(XML.close_tag('text'))
                lines.append(XML.single_tag('line', x1=0, x2=self.label_width, y1=self._val_y(i+1), y2=self._val_y(i+1), stroke='grey'))

        # make highlights
        for x in range(width):
            lines.append(XML.single_tag('rect',
                                        width=VAL_WIDTH - DATA_STROKE,
                                        height=self.data_height - (DATA_STROKE * 2),
                                        y=self.data_y + DATA_STROKE,
                                        x=self._val_x(x),
                                        fill='none',
                                        # stroke=self.trace_color,
                                        # stroke_width=self.HIGHLIGHT_BORDER,
                                        # opacity=0,
                                        classes='data-highlight col{}'.format(x),
                                        z=100))

        for y in range(height):
            lines.append(XML.single_tag('rect',
                                        width=self.data_width - (DATA_STROKE * 2),
                                        height=VAL_HEIGHT - DATA_STROKE,
                                        y=self._val_y(y),
                                        x=self.data_x + DATA_STROKE,
                                        fill='none',
                                        # stroke=self.trace_color,
                                        # stroke_width=self.HIGHLIGHT_BORDER,
                                        # opacity=0,
                                        classes='data-highlight row{}'.format(y),
                                        z=100))
        # lines.append(XML.close_tag('rect'))
        lines.append(XML.text('', x=self.data_x, y=self.output_y + (VAL_HEIGHT),
                              height=VAL_HEIGHT, width=self.data_width,
                              dy=-10, id=self.map_id + '-data-out'))

        return JOINER.join(lines)
