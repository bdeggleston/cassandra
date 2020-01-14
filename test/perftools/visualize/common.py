import re


def get_common_css():
    with open(re.sub(r'\.py$', '.css', __file__)) as f:
        return f.read()

