# copyright (c) 2021 Jason Forbes

import collections.abc
from contextlib import contextmanager



class PerishablesMapMixin:
    def __init__(self, test_valid, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_valid = test_valid
        self.iters_open = 0
        self.expired = collections.deque(maxlen=10000)

    def release_expired(self):
        if self.iters_open != 0:
            return False
        while self.expired:
            k = self.expired.popleft()
            self._release_action(k)
        return True

    def _release_action(self, k):
        "Override this in classes that don't have __delitem__ implemented."
        try:
            del self[k]
        except KeyError:
            pass

    @contextmanager
    def _iter_context(self):
        self.iters_open += 1
        try:
            yield
        finally:
            self.iters_open -= 1
            self.release_expired()
        
    def __iter__(self):
        with self._iter_context():
            for k in super().__iter__():
                if self.test_valid(k):
                    yield k
                else:
                    self.expired.append(k)

    def __getitem__(self, k):
        r = super().__getitem__(k)
        if type(k) is slice or self.test_valid(k):
            return r
        else:
            self.expired.append(k)
            self.release_expired()
            raise KeyError()

    def __contains__(self, k):
        if super().__contains__(k):
            if self.test_valid(k):
                return True
            else:
                self.expired.append(k)
                self.release_expired()
        return False

    def __len__(self):
        # what a horrible function
        return sum(1 for _ in iter(self))



class PerishablesMap(PerishablesMapMixin, collections.UserDict):
    pass
