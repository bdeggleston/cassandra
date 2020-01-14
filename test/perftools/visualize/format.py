from typing import Callable


def format_value(v):
    if isinstance(v, float):
        return '{:.2f}'.format(float(v))
    elif isinstance(v, str) and str.isnumeric(v.replace('-', '').replace('.', '')):
        is_float = '.' in v
        return '{:.2f}'.format(float(v)) if is_float else str(v)
    else:
        return str(v)


def format_percent(v):
    s = '{:.4f}%'.format(v)
    return '+' + s if v > 0 else s


class ValueFormat(object):
    def __init__(self, simple_name, format: Callable[..., str] = None):
        self.simple_name = simple_name
        self._format = format

    def format(self, v) -> str:
        return self._format(v) if self._format else str(v)

