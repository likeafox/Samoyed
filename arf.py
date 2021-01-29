#! Python3.7

# Copyright: Jason Forbes

# Require Python 3.7 or later
import sys
assert (sys.version_info.major, sys.version_info.minor) >= (3, 7)

import io



# Data defs

class DataDef:
    @staticmethod
    def byte_length():
        raise NotImplementedError()
    @staticmethod
    def validate(v):
        raise NotImplementedError()
    @staticmethod
    def pack(v):
        raise NotImplementedError()
    @staticmethod
    def unpack(b):
        raise NotImplementedError()

class UInt(DataDef):
    @classmethod
    def byte_length(cls):
        assert cls.bit_length > 0
        return (cls.bit_length - 1) // 8 + 1
    @classmethod
    def validate(cls, v):
        if type(v) is not int or v < 0 or v.bit_length() > cls.bit_length:
            raise TypeError()
    @classmethod
    def pack(cls, v):
        cls.validate(v)
        return v.to_bytes(cls.byte_length(), 'little')
    @classmethod
    def unpack(cls, b):
        v = int.from_bytes(b, 'little')
        cls.validate(v)
        return v

class UnitTypeID(UInt):
    bit_length = 8
    app_defined_range = range(128,256)



# Units

class unit_metaclass(type):
    def __new__(cls, name, bases, attrs):
        dds = {}
        for base in bases:
            if base.__class__ is unit_metaclass and hasattr(base, "data_defs"):
                dds.update(base.data_defs)
                break
        if 'additional_data_defs' in attrs:
            add_dds = attrs['additional_data_defs']
            if not set(dds).isdisjoint(set(add_dds)):
                raise ValueError("Unit definition has conflicting data labels")
            dds.update(add_dds)
        attrs["data_defs"] = dds

        new_cls = super(unit_metaclass, cls).__new__(cls, name, bases, attrs)

        return new_cls

class UnitFormatError(ValueError): pass

class Unit(metaclass=unit_metaclass):
    additional_data_defs = {'id':UnitTypeID}

    @classmethod
    def calc_piece_slices(cls, buffer):
        r = []
        pos = 0
        for dd in cls.data_defs.values():
            len_spec = dd.byte_length()
            if type(len_spec) is int and len_spec >= 0:
                l = len_spec
            elif len_spec == "variable":
                l = int.from_bytes(buffer[pos:pos+2], 'little')
                pos += 2
            else:
                raise TypeError("Invalid byte length spec")
            r.append(slice(pos, pos + l))
            pos += l
        if pos > len(buffer):
            raise UnitFormatError("Would read past end of buffer")
        return r

    @classmethod
    def actual_byte_length_from_slices(cls, slices):
        try:
            return slices[-1].stop
        except IndexError:
            return 0

    def __init__(self, *pieces):
        if len(pieces) != len(self.data_defs):
            raise UnitFormatError("Wrong number of pieces")
        for dd,p in zip(self.data_defs.values(), pieces):
            dd.validate(p)
        self.pieces = pieces

    def __getitem__(self, key):
        if type(key) is int:
            return self.pieces[key]
        if type(key) is str:
            ks = self.data_defs.keys()
        elif isinstance(key, DataDef):
            ks = self.data_defs.values()
        else:
            raise TypeError()
        return next(p for k,p in zip(ks, self.pieces) if k == key)

    def __repr__(self):
        return f"<{self.__class__.__name__} {id(self)}"\
               f" ({tuple(self.data_defs.values())})>"

    def pack(self):
        for dd,p in zip(self.data_defs.values(), self.pieces):
            packed = dd.pack(p)
            if dd.byte_length() == "variable":
                yield from len(packed).to_bytes(2, 'little')
            yield from packed

    @classmethod
    def unpack(cls, packed):
        slices = calc_piece_slices(packed)
        if len(packed) != cls.actual_byte_length_from_slices(slices):
            raise UnitFormatError("Packed data is longer than spec")
        pieces = (dd.unpack(packed[s]) for dd,s in zip(cls.data_defs.values(), slices))
        return cls(*pieces)

class UnitTypeListing:
    def __init__(self, inherit=None):
        self._list = {} if inherit is None else inherit._list.copy()

    def __getitem__(self, key):
        return self._list[key]

    def register(self, id):
        UnitTypeID.validate(id)
        if id in self._list:
            raise ValueError("Unit type already registered")
        def d(c):
            if not c.__module__ == __name__ and \
               id not in UnitTypeID.app_defined_range:
                raise ValueError("Unit Type ID out of acceptable range")
            c.unit_type_id = id
            self._list[id] = c
            return c
        return d

    def __repr__(self):
        return f"<{self.__class__.__name__} {id(self)} ({self._list})>"

base = UnitTypeListing()

# Built-in Unit types

@base.register(1)
class Uh(Unit):
    additional_data_defs = {"some": UnitTypeID}



# Defaults, Local Cache

class LocalCache: pass

class MemoryOnlyLocalCache:
    def __init__(self, name, est_size, est_msg_size, **options):
        self.reset()

    def append(self, unit):
        if type(unit) not in (bytes, bytearray):
            raise TypeError()
        self.cache[self.ctr] = unit
        self.ctr += 1

    def discard(self, id):
        if id not in self.cache:
            raise ValueError()
        del self.cache[id]

    def discard_all_before(self, id):
        return dict((k,v) for k,v in self.cache.items() if k >= id)

    def get(self, id, callback):
        if id not in self.cache:
            raise KeyError()

    def reset(self):
        self.ctr = 1
        self.cache = {}

    def __iter__(self):
        #dict items are iterated in insertion order in py 3.7
        yield from self.cache.items()

class DefaultLocalCache(LocalCache):
    def __init__(self, name, est_size, est_msg_size, **options):
        assert path in options
        raise NotImplementedError()
        #self.max_items = options.get('max_items', None)
        #if self.max_items is not None and len(self.cache) > self.max_items:
        #    del self.cache[next(iter(self.cache))]



# Mappings

# ??



# Default Transit Handler

class LoopbackTransitHandler:
    def __init__(self, cache, **options):
        pass

    def send(self, units):
        pass



# ARF?

class ARFStrand(io.BytesIO):
    pass

class ARFManager:
    def __init__(self, cache=None,
                       transit=None,
                       unit_type_listing=base):
        pass

    def e(self) -> io.BytesIO:
        pass

print(Uh(1,1))
