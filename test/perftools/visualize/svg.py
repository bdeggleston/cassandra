INDENT_SIZE = 4
# INDENT_SIZE = 0
INDENT_STR = ' ' * INDENT_SIZE

JOINER = '\n'
# JOINER = ''


def px(v):
    return str(int(v)) + 'px'


def _indent(lines, indent_str):
    if isinstance(lines, str) and '\n' not in lines:
        return indent_str + lines
    if isinstance(lines, (list, tuple)):
        return JOINER.join([indent_str + l for l in lines])
    else:
        return '\n'.join([indent_str + l for l in lines.split('\n')])


def indent(lines, times):
    return _indent(lines, INDENT_STR * times)


class XML(object):
    _CURRENT_INDENT = 0
    _INDENT_STR = ''

    @staticmethod
    def to_xml_props(**kwargs):
        props = []
        for k, v in kwargs.items():
            if '_' in k: k = k.replace('_', '-')
            if k == 'classes': k = 'class'
            if isinstance(v, (list, tuple)):
                v = ' '.join([str(i) for i in v])

            props.append('{}="{}"'.format(k, v))
        return ' '.join(props)

    @staticmethod
    def _tag(fmt: str, name: str, noindent=False, **kwargs):
        props = ' ' + XML.to_xml_props(**kwargs) if kwargs else ''
        if noindent:
            return fmt.format(name, props)
        return XML._INDENT_STR + fmt.format(name, props)


    @staticmethod
    def _indent():
        XML._CURRENT_INDENT += 1
        XML._INDENT_STR += INDENT_STR

    @staticmethod
    def _unindent():
        XML._CURRENT_INDENT -= 1
        XML._INDENT_STR = XML._INDENT_STR[:-4]

    @staticmethod
    def open_tag(name: str, noindent=False, **kwargs):
        out = XML._tag('<{}{}>', name, noindent=noindent, **kwargs)
        if not noindent: XML._indent()
        return out

    @staticmethod
    def close_tag(name: str, noindent=False, **kwargs):
        if not noindent: XML._unindent()
        return XML._tag('</{}{}>', name, noindent=noindent, **kwargs)

    @staticmethod
    def single_tag(name: str, noindent=False, **kwargs):
        return XML._tag('<{}{}/>', name, noindent=noindent, **kwargs)

    @staticmethod
    def indent(lines):
        return _indent(lines, XML._INDENT_STR)

    @staticmethod
    def text(txt, **kwargs):
        params = {'dx': 5, 'dy': -2}
        params.update(kwargs)
        return JOINER.join([
            XML.open_tag('text', **params),
            XML.indent(txt),
            XML.close_tag('text'),
        ])


class SVGSnippet(object):
    def __init__(self, markup, x, y, width, height):
        self.markup = JOINER.join(markup) if isinstance(markup, (list, tuple)) else markup
        self.x = x
        self.y = y
        self.width = width
        self.height = height

