
import numpy as np
from skimage import color

from .svg import XML


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

    def as_svg(self):
        return 'rgb({},{},{})'.format(*[int(v+0.5) for v in (self.arr)])

    def __repr__(self):
        return 'Color({},{},{})'.format(*self.arr)

    def _clip0_1(self, v):
        if v > 1.0:
            return 1.0
        if v < 0:
            return 0.0
        return v

    def _transform_hsv(self, idx, adj):
        hsv = color.rgb2hsv([[self.arr / 255]])[0][0]
        hsv[idx] = self._clip0_1(hsv[idx] * adj)
        rgb = color.hsv2rgb([[hsv]])[0][0] * 255
        return Color(*rgb)

    def lightness(self, adj):
        return self._transform_hsv(2, adj)

    def saturation(self, adj):
        return self._transform_hsv(1, adj)


class Gradient(object):

    def __init__(self, *colors):
        self.colors = colors
        self.lch_colors = color.rgb2lab([[c.arr/255 for c in self.colors]])[0]

    def color_at(self, pos):
        """
        :param pos: 0..1
        :return:
        """
        if np.isnan(pos) or np.isinf(pos):
            return Colors.WHITE
        assert 0 <= pos <= 1, pos
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

    def lightness(self, adj):
        return Gradient(*[c.lightness(adj) for c in self.colors])

    def saturation(self, adj):
        return Gradient(*[c.saturation(adj) for c in self.colors])


class Colors(object):
    BLACK = Color(0, 0, 0)
    PURPLE = Color(128, 0, 255)
    RED = Color(255, 0, 0)
    GREEN = Color(0, 255, 0)
    ORANGE = Color(255, 128, 0)
    YELLOW = Color(255, 255, 0)
    BLUE = Color(0, 0, 255)
    WHITE = Color(255, 255, 255)


BG_COLOR = Color(50, 50, 50)


class Gradients(object):
    PREDATOR = Gradient(Color(128, 0, 255).lightness(0.8),
                        Color(0, 148, 242).lightness(0.85),
                        Color(0, 255, 160),
                        Color(255, 180, 60), Colors.RED)
    DIFF = Gradient(Colors.RED, BG_COLOR.lightness(0.6), Colors.GREEN)

