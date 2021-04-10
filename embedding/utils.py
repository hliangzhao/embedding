"""
Utilities.

    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""
import sys


class ProgressBar:
    def __init__(self, width=50):
        self.last = -1
        self.width = width

    def update(self, current):
        assert 0 <= current <= 100
        if self.last == current:
            return
        self.last = int(current)
        pos = int(self.width * (current / 100.0))
        sys.stdout.write('\r%d%% [%s]' % (int(current), '#' * pos + '.' * (self.width - pos)))
        sys.stdout.flush()
        if current == 100:
            print('')


def reverse_dict(d):
    """ Reverses direction of dependence dict.
    e.g.:
    d = {'a': (1, 2), 'b': (2, 3), 'c':()}
    reverse_dict(d) = {1: ('a',), 2: ('a', 'b'), 3: ('b',)}
    """
    result = {}
    for key in d:
        for val in d[key]:
            result[val] = result.get(val, tuple()) + (key, )
    return result
