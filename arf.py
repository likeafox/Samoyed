#! Python3.7

# Copyright: Jason Forbes

# Require Python 3.7 or later
import sys
assert (sys.version_info.major, sys.version_info.minor) >= (3, 7)

from itertools import islice
import collections.abc
import io

# local imports
import selfdelimitedblob



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
    @staticmethod
    def bit_length_to_byte_length(bits):
        assert bits > 0
        return (bits - 1) // 8 + 1
    @classmethod
    def byte_length(cls):
        return bit_length_to_byte_length(cls.bit_length)
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

class TxScopeID(UInt): # transaction-scope id
    bit_length = 16

class StrandID(UInt):
    bit_length = 64

class StrandSize(UInt):
    bit_length = 64

class RangedUInt(UInt):
    @classmethod
    def byte_length(cls):
        assert 0 <= cls.valid_range.start < cls.valid_range.stop
        bits = (cls.valid_range.stop - 1).bit_length()
        return UInt.bit_length_to_byte_length(bits)
    @classmethod
    def validate(cls, v):
        if v not in cls.valid_range:
            raise TypeError()

class ByteInt(RangedUInt):
    valid_range = range(0,256)
    @staticmethod
    def byte_length():
        assert valid_range.stop <= 256
        return 1
    @classmethod
    def _unsafe_pack(cls, v):
        return bytes((v,))
    @classmethod
    def _unsafe_unpack(cls, b):
        return b[0]

class UnitTypeID(ByteInt):
    deleted_range = range(0, 2)
    arf_base_defined_range = range(2, 128)
    app_defined_range = range(128,256)

class StrandGroupMagnitude(ByteInt):
    valid_range = range(1, StrandID.bit_length)
    @classmethod
    def to_strand_group_mask(cls, v):
        return -1 << v

class Bool(ByteInt):
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
    @classmethod
    def byte_length(cls):
        return StrandDataLength
    @classmethod
    def validate(cls, v):
        ByteData.validate(v)
        cls.byte_length().validate(len(v))

class StrandDataLength(RangedUInt):
    valid_range = (1, 512 + 1)



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

class UnitDataFormatError(ValueError): pass

class UnitBase(metaclass=unit_metaclass):
    @classmethod
    def key_to_piece_index(cls, key):
        if type(key) is int:
            return key
        if type(key) is str:
            return self.piece_names[key]
        if issubclass(key, DataDef):
            return self.data_spec.index(key)
        raise TypeError()

    def __init__(self, *pieces):
        if len(pieces) != len(self.data_spec):
            raise UnitDataFormatError("Wrong number of pieces")
        self.pieces = pieces
        self.__class__._validate(self)

    @classmethod
    def _validate(cls, v):
        if not type(v) is cls:
            raise TypeError()
        for dt,pn,p in zip(self.data_spec, self.piece_names, pieces):
            try: dt.validate(p)
            except TypeError: raise UnitDataFormatError(f"failed validation of {pn}")

    def __getitem__(self, key):
        return self.pieces[self.key_to_piece_index(key)]

    def __repr__(self):
        pcs = repr(tuple(self.pieces))
        return f"<{self.__class__.__name__} {id(self)}" \
               f" {pcs[:50]+(pcs[50:] or ' ...')}>"

class Unit(UnitBase):
    additional_data_defs = {'typeid':UnitTypeID}



# ARF

class ARFSpec():
    def __init__(self, inherit=None):
        self._list = {} if inherit is None else inherit._list.copy()

    def __getitem__(self, key):
        return self._list[key]

    def __contains__(self, key):
        return key in self._list or \
               (issubclass(key, Unit) and key in self._list.values())

    def register(self, id):
        UnitTypeID.validate(id)
        if id in self._list:
            raise ValueError("Unit type already registered")
        def d(c):
            assert issubclass(c, Unit)
            ok_range = UnitTypeID.arf_base_defined_range if \
                (c.__module__ == __name__) else UnitTypeID.app_defined_range
            if id not in ok_range:
                raise ValueError("Unit Type ID out of acceptable range " \
                                f"{ok_range.start}..{ok_range.stop-1}")
            self._list[id] = c
            return c
        return d

    def __repr__(self):
        return f"<{self.__class__.__name__} {id(self)} ({self._list})>"

base = ARFSpec()

# Built-in Unit types

@base.register(2)
class TxScopeMarker(Unit):
    additional_data_defs = {'prev-txs': TxScopeID, 'next-txs': TxScopeID}
    roles = {'relationship:': 'SCOPE-CONTROLLER',
             'scope': 'GLOBAL',
             'persistence': 'ELAPSING'}

@base.register(3)
class TxScopeFinalize(Unit):
    additional_data_defs = {'commit-or-release': Bool}
    roles = {'relationship': 'MODIFIER',
             'scope': 'TX',
             'persistence': 'ELAPSING'}

@base.register(4)
class StrandSelect(Unit):
    additional_data_defs = {'strand-id': StrandID}
    roles = {'relationship': 'MODIFIER',
             'scope': 'TX',
             'persistence': 'REFRESHING'}

@base.register(5)
class StrandGroupSelect(Unit):
    additional_data_defs = {'strand-group': StrandID,
                            'strand-group-mag': StrandGroupMagnitude}
    roles = {'relationship': 'MODIFIER',
             'scope': 'TX',
             'persistence': 'REFRESHING'}

@base.register(6)
class StrandWriteDataBlock(Unit):
    additional_data_defs = {'offset': StrandSize, 'data': StrandData}
    roles = {'relationship': 'OBJECT',
             'scope': 'TX',
             'persistence': 'REFRESHING'}

@base.register(7)
class StrandCreateUpdate(Unit):
    additional_data_defs = {'strd-size-bytes': StrandSize}
    roles = {'relationship': 'OBJECT',
             'scope': 'TX',
             'persistence': 'REFRESHING'}

@base.register(8)
class StrandDiscard(Unit):
    additional_data_defs = {'strd-group-member-sub-id-sz-bits': UnitTypeID}
    roles = {'relationship':'OBJECT',
             'scope':'TX',
             'persistence': 'ELAPSING'}



# IO

class ARFIOWrapper(selfdelimitedblob.IO):
    def __init__(self, spec:ARFSpec, stream:io.BufferedIOBase=None):
        super().__init__(stream)
        self.spec = spec
        assert list(Unit.data_spec) == [UnitTypeID] #required constraint for deletion functionality

    # read/load interface

    def _get_next_piece_length(self, datatype):
        len_spec = datatype.byte_length()
        if type(len_spec) is int and len_spec >= 0:
            return len_spec
        if issubclass(len_spec, UInt):
            return self._read_data(len_spec)
        else:
            raise TypeError("Invalid byte length spec")

    def _read_data(self, datatype):
        sz = self._get_next_piece_length(datatype)
        data = self.stream.read(sz)
        if len(data) != sz:
            raise UnitDataFormatError("reading past end of buffer")
        return datatype.unpack(data)

    def read_next(self, opt=None):
        """Read data from the unit next in stream. If the unit has been
        deleted, return None. Otherwise, the default behaviour is to return
        the data as a new Unit instance. Optionally, opt can be a list of
        unit pieces to read, in which case a list of the resulting pieces is
        returned. Only data that is requested is read so if an empty list is
        provided, the function will prerform only the minimal reads necessary
        to seek to the end of the unit in stream."""
        unit_pcs = [self._read_data(dt) for dt in Unit.data_spec]
        unit_typeid = unit_pcs[Unit.key_to_piece_index('typeid')]

        if unit_typeid in UnitTypeID.deleted_range:
            if unit_typeid == 1:
                while self._read_data(Bool):
                    pass # skip to end of deleted unit
            return None

        unit_type = self.spec[unit_typeid]
        if opt is None:
            choice_indices = range(len(unit_type.data_spec))
        else:
            choice_indices = [unit_type.key_to_piece_index(k) for k in opts]
            assert len(choice_indices) == len(set(choice_indices))

        for i,dt in islice(enumerate(unit_type.data_spec), start=len(unit_pcs)):
            if i in choice_indices:
                p = self._read_data(dt)
            else:
                self.stream.seek(self._get_next_piece_length(dt),1)
                p = None
            unit_pcs.append(p)

        if opt is None:
            return unit_type(*unit_pcs) # unit instance by default
        return [unit_pcs[i] for i in opt] # optionally, pick specific pieces

    # skip interface

    def skip_next(self):
        return None if (self._read_next([]) is None) else True

    # write interface

    def _write_data(self, data, datatype):
        packed = datatype.pack(data)
        len_spec = datatype.byte_length()
        if type(len_spec) is int:
            assert len(packed) == len_spec
        else:
            self._write_data(len(packed), len_spec)
        self.stream.write(packed) #dw, io.BufferedIOBase.write always writes everything

    def write_unit(self, unit):
        for dt,p in zip(unit.data_spec, unit.pieces):
            self._write_data(p, dt)

    def write_obj(self, obj):
        return self.write_unit(obj)

    # delete interface

    def delete_next(self):
        start_pos = self.stream.tell()
        if self.skip_next() is None:
            return
        sz = self.stream.tell() - start_pos
        self.stream.seek(start_pos, 0)
        id_sz = UnitTypeID.byte_length()
        if sz == id_sz:
            self._write_data(0, UnitTypeID)
        else:
            self._write_data(1, UnitTypeID)
            self.stream.write(b'\x01' * (sz - id_sz - 1))
            self.stream.write(b'\0')



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