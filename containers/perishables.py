# copyright (c) 2021 Jason Forbes

import collections.abc, sys
from contextlib import contextmanager
from .searchtree import SearchTreeMap, SearchTreeMapSliceView



class PerishablesContainerInterfaceMixin:
    def __iter__(self):
        return self.perishables_owner._iter_wrapper(super().__iter__())

    def __contains__(self, k):
        if super().__contains__(k):
            if self.perishables_owner.test_valid(k):
                return True
            else:
                self.perishables_owner.expired.add(k)
                self.perishables_owner.try_release_expired()
        return False

    def __len__(self):
        # what a horrible function
        return sum(1 for _ in iter(self))

    def __bool__(self):
        for _ in iter(self):
            return True
        return False

class PerishablesMapInterfaceMixin(PerishablesContainerInterfaceMixin):
    def __getitem__(self, k):
        r = super().__getitem__(k)
        if type(k) is slice or self.perishables_owner.test_valid(k):
            return r
        else:
            self.perishables_owner.expired.add(k)
            self.perishables_owner.try_release_expired()
            raise KeyError()



class PerishablesContainerMixin(PerishablesContainerInterfaceMixin):
    def __init__(self, test_valid, *args, **kwargs):
        """test_valid(key) is only used to test keys that currently exist in the
        underlying map. It is not called for non-existant/prospective keys."""
        super().__init__(*args, **kwargs)
        self.perishables_owner = self
        if test_valid is not None:
            self.__dict__['test_valid'] = test_valid
        self.iters_open = 0
        self.expired = set()

    def try_release_expired(self):
        if self.iters_open != 0:
            return False
        for k in self.expired:
            self._release_action(k)
        self.expired.clear()
        return True

    @contextmanager
    def _iter_context(self):
        self.iters_open += 1
        try:
            yield
        finally:
            self.iters_open -= 1
            self.try_release_expired()

    def _iter_wrapper(self, iterator):
        with self._iter_context():
            for k in iterator:
                if self.test_valid(k):
                    yield k
                else:
                    self.expired.add(k)

class PerishablesSetMixin(PerishablesContainerMixin):
    def _release_action(self, k):
        self.discard(k)

class PerishablesMapMixin(PerishablesContainerMixin, PerishablesMapInterfaceMixin):
    def _release_action(self, k):
        try:
            del self[k]
        except KeyError:
            pass



# class TruthyValuesMapMixin(PerishablesMapMixin):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, test_valid=None, **kwargs)

#     def test_valid(self, k):
#         return bool(super(PerishablesMapInterfaceMixin, self).__getitem__(k))



class AutoContainerMapInterfaceMixin(PerishablesMapInterfaceMixin):
    def __getitem__(self, k):
        self.perishables_owner.expired.discard(k)
        try:
            v = super(PerishablesMapInterfaceMixin, self).__getitem__(k)
        except:
            new = self.perishables_owner.container_factory()
            self[k] = new
            return new
        return v



class AutoContainerMapMixin(AutoContainerMapInterfaceMixin, PerishablesMapMixin):
    def __init__(self, container_factory, *args, **kwargs):
        self.container_factory = container_factory
        super().__init__(*args, test_valid=None, **kwargs)

    def test_valid(self, k):
        v = super(PerishablesMapInterfaceMixin, self).__getitem__(k)
        if len(v):
            return True
        # Additionally, the AutoContainer shall be considered valid if there are
        # any refs held by users. This is to avoid a sort of race condition that
        # would accidentally delete an AutoContainer that is empty, but that the
        # user had previously retrieved and is about to put content in.
        #
        # I believe outstanding refs exist if there are more than 3 total refs,
        # accounting for the expected refs held by each of the following:
        # 1) Our AutoContainerMap (self)
        # 2) v, defined in this function
        # 3) The argument of sys.getrefcount() itself
        return sys.getrefcount(v) > 3



class OrderedSet(collections.abc.MutableSet):
    __slots__ = ("data","last_add")

    def __init__(self, iterable=()):
        self.data = {}
        for k in iterable:
            self.add(k)

    def __contains__(self, k):
        return k in self.data

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def add(self, k):
        self.data[k] = None
        self.last_add = k

    def discard(self, k):
        self.data.pop(k, None)

    def __repr__(self):
        c = f"<{self.__class__.__module__}.{self.__class__.__qualname__}"
        return f"<{c} {list(self.data)}>"



# Prebuilts

class PerishablesSet(PerishablesSetMixin, OrderedSet):
    pass

class PerishablesMap(PerishablesMapMixin, collections.UserDict):
    pass

class PerishablesSearchTreeMapSliceView(PerishablesMapInterfaceMixin,
                                        SearchTreeMapSliceView):
    def __init__(*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.perishables_owner = self.tree

class PerishablesSearchTreeMap(PerishablesMapMixin, SearchTreeMap):
    def _getsliceview(self, ksslice):
        return PerishablesSearchTreeMapSliceView(self, ksslice)

class AutoContainerMap(AutoContainerMapMixin, collections.UserDict):
    pass

class AutoContainerSearchTreeMapSliceView(AutoContainerMapInterfaceMixin,
                                          PerishablesSearchTreeMapSliceView):
    pass

class AutoContainerSearchTreeMap(AutoContainerMapMixin, PerishablesSearchTreeMap):
    def _getsliceview(self, ksslice):
        return AutoContainerSearchTreeMapSliceView(self, ksslice)
