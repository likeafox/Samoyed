#! Python3.7

# Copyright: Jason Forbes

# Require Python 3.7 or later
import sys
assert (sys.version_info.major, sys.version_info.minor) >= (3, 7)

from itertools import islice
import collections.abc
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
    def _unsafe_pack(v):
        raise NotImplementedError()
    @staticmethod
    def _unsafe_unpack(b):
        raise NotImplementedError()
    @classmethod
    def pack(cls, v): # v: (python-typed) Value; "unpacked value"
        cls.validate(v)
        return cls._unsafe_pack(v)
    @classmethod
    def unpack(cls, b): # b: Byte-representation; "packed value"
        v = cls._unsafe_unpack(b)
        cls.validate(v)
        return v

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
    def _unsafe_pack(cls, v):
        return v.to_bytes(cls.byte_length(), 'little')
    @classmethod
    def _unsafe_unpack(cls, b):
        v = int.from_bytes(b, 'little')
        return v

class UInt8(UInt):
    bit_length = 8

class UInt64(UInt):
    bit_length = 64

class UnitTypeID(UInt):
    bit_length = 8
    app_defined_range = range(128,256)

class TxScopeID(UInt): # transaction-scope id
    bit_length = 16
    @classmethod
    def to_strand_group_mask(cls, v):
        return -1 << v

class StrandID(UInt64): pass

class Bool(DataDef):
    @classmethod
    def byte_length():
        return 1
    @classmethod
    def validate(cls, v):
        if type(v) is not bool:
            raise TypeError()
    @classmethod
    def _unsafe_pack(cls, v):
        return int(v).to_bytes(1,'little')
    @classmethod
    def _unsafe_unpack(cls, b):
        return bool(b[0])

class ByteData(DataDef):
    @classmethod
    def validate(cls, v):
        l = cls.byte_length()
        if not isinstance(v, collections.abc.ByteString) or \
            (type(l) is int and len(v) != l):
            raise TypeError()
    @classmethod
    def _unsafe_pack(cls, v):
        return v
    @classmethod
    def _unsafe_unpack(cls, b):
        return b

class StrandData(ByteData):
    block_size_bytes = 512
    @classmethod
    def byte_length(cls):
        return 'variable'
    @classmethod
    def validate(cls, v):
        ByteData.validate(v)
        if len(v) not in range(1, cls.block_size_bytes + 1):
            return TypeError()



# Units

class unit_metaclass(type):
    def __new__(cls, name, bases, attrs):
        for base in bases:
            if base.__class__ is unit_metaclass and hasattr(base, "data_spec"):
                ds = base.data_spec.copy()
                pns = base.piece_names.copy()
                break
        else:
            ds = [] #data_spec
            pns = {} #piece_names

        try:
            add_dds = attrs['additional_data_defs']
        except KeyError:
            pass
        else:
            base_len = len(ds)
            if not set(pns).isdisjoint(set(add_dds)):
                raise ValueError("Unit definition has conflicting data labels")
            ds.extend(add_dds.values())
            pns.update((name,i) for i,name in enumerate(add_dds, base_len))
        
        attrs['data_spec'] = ds
        attrs['piece_names'] = pns
        new_cls = super(unit_metaclass, cls).__new__(cls, name, bases, attrs)
        return new_cls

class UnitFormatError(ValueError): pass

def read_exact_or_fail(stream, sz):
    r = stream.read(sz)
    assert len(r) == sz
    return r

class Unit(metaclass=unit_metaclass):
    additional_data_defs = {'typeid':UnitTypeID}

    @classmethod
    def _key_to_piece_index(cls, key):
        if type(key) is int:
            return key
        if type(key) is str:
            return self.piece_names[key]
        if issubclass(key, DataDef):
            return self.data_spec.index(key)
        raise TypeError()

    @property
    def roles():
        raise NotImplementedError()

    @classmethod
    def calc_piece_slices_in_buf(cls, buffer):
        r = []
        pos = 0
        for dd in cls.data_spec:
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

    @staticmethod
    def actual_byte_length_from_slices(slices):
        try:
            return slices[-1].stop
        except IndexError:
            return 0

    def __init__(self, *pieces):
        if len(pieces) != len(self.data_defs):
            raise UnitFormatError("Wrong number of pieces")
        validate_pcs = (dd.validate(p) for dd,p in zip(self.data_spec, pieces))
        collections.deque(validate_pcs,maxlen=0) # exhaust generator
        self.pieces = pieces

    def __getitem__(self, key):
        return self.pieces[self._key_to_piece_index(key)]

    def __repr__(self):
        return f"<{self.__class__.__name__} {id(self)} {tuple(self.pieces)}>"

    def pack(self):
        def gen():
            for dd,p in zip(self.data_spec, self.pieces):
                packed = dd.pack(p)
                if dd.byte_length() == "variable":
                    yield from len(packed).to_bytes(2, 'little')
                yield from packed
        return bytes(gen())

    @classmethod
    def unpack(cls, packed):
        slices = calc_piece_slices_in_buf(packed)
        if len(packed) != cls.actual_byte_length_from_slices(slices):
            raise UnitFormatError("Packed data is longer than spec")
        pieces = (dd.unpack(packed[s]) for dd,s in zip(cls.data_spec, slices))
        return cls(*pieces)

class UnitTypeIndexer:
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

    def read_unit_in_stream(self, stream, *get_pieces, complete=True):
        """Assuming stream cursor is at the start of a byte-encoded Unit,
        read the unit pieces specified in get_pieces, and then return a
        list of their values. It's possible to only pass `stream` to this
        function which is useful if the only thing you want to do is
        seek past the Unit (probably in order to read a subsequent Unit).
        If `complete` is set to True, the function will always return
        with the stream cursor at the end of the Unit. If `complete` is
        set to False, the function will exit as soon as all the sought
        pieces have been retrieved."""
        raise NotImplementedError()

    def stat_unit_in_buf(self, buffer, offset=0):
        "Get the type id and length of a Unit in buffer at specified offset"
        if offset != 0:
            buffer = memoryview(buffer)[offset:]
        type_id = UnitTypeID.unpack(buffer[:UnitTypeID.byte_length()])
        slices = self[type_id].calc_piece_slices_in_buf(buffer)
        length = Unit.actual_byte_length_from_slices(slices)
        return type_id, length

base = UnitTypeIndexer()

# Built-in Unit types

@base.register(1)
class TxScopeMarker(Unit):
    additional_data_defs = {'prev-txs': TxScopeID, 'next-txs': TxScopeID}
    roles = {'relationship:': 'SCOPE-CONTROLLER',
             'scope': 'GLOBAL',
             'persistence': 'ELAPSING'}

@base.register(2)
class TxScopeFinalize(Unit):
    additional_data_defs = {'commit-or-release': Bool}
    roles = {'relationship': 'MODIFIER',
             'scope': 'TX',
             'persistence': 'ELAPSING'}

@base.register(3)
class StrandSelect(Unit):
    additional_data_defs = {'strand-id': UInt64}
    roles = {'relationship': 'MODIFIER',
             'scope': 'TX',
             'persistence': 'REFRESHING'}

@base.register(4)
class StrandWriteDataBlock(Unit):
    additional_data_defs = {'offset': UInt64, 'data': StrandData}
    roles = {'relationship': 'OBJECT',
             'scope': 'TX',
             'persistence': 'REFRESHING'}

@base.register(5)
class StrandCreateUpdate(Unit):
    additional_data_defs = {'strd-size-bytes': UInt64}
    roles = {'relationship': 'OBJECT',
             'scope': 'TX',
             'persistence': 'REFRESHING'}

@base.register(6)
class StrandDiscard(Unit):
    additional_data_defs = {'strd-group-member-sub-id-sz-bits': UnitTypeID}
    roles = {'relationship':'OBJECT',
             'scope':'TX',
             'persistence': 'ELAPSING'}



# Defaults, Local Cache

class LocalCache: pass

class MemoryOnlyLocalCache:
    def __init__(self, name, est_size, est_msg_size, **options):
        self.reset()

    def append(self, unit):
        if type(unit) not in (bytes, bytearray):
            raise TypeError()
        self.ctr += 1
        self.cache[self.ctr] = unit
        return self.ctr

    def discard(self, id):
        if id not in self.cache:
            raise ValueError()
        del self.cache[id]

    def discard_all_before(self, id):
        return dict((k,v) for k,v in self.cache.items() if k >= id)

    def get(self, id, callback):
        return self.cache[id]
        # if id not in self.cache:
        #     raise KeyError()

    def reset(self):
        self.ctr = 0 # first id will be 1
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

# class UnitMapInfo:
#     def __init__(self, cached_id):
#         self.cached_id = cached_id

# class ARFMap:
#     def __init__(self, unit_type_listing, *includes):
#         self.cur_txscope = None
#         self.pre_mapping_content = []
#         self.unit_type_listing = unit_type_listing
#         #self.unit_types

#     def include()



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

print("hi")