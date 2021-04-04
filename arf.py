#! Python3.7

# Copyright: Jason Forbes

# Require Python 3.7 or later
import sys
assert (sys.version_info.major, sys.version_info.minor) >= (3, 7)

import collections.abc, io, heapq, itertools, weakref, random
from itertools import islice
from dataclasses import dataclass

# local imports
import selfdelimitedblob
import containers.searchtree
from containers.perishables import PerishablesSet, PerishablesMap, \
    PerishablesSearchTreeMap, AutoContainerMap, AutoContainerSearchTreeMap
from containers.dense import DenseIntegerSet



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

class UInt128(UInt):
    bits = 128

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
        return (1 << v) - 1 # can also be expressed ~(-1 << v)

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
    valid_range = range(1, 512 + 1)



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
            if not pns.keys().isdisjoint(add_dds.keys()):
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
        self.pieces = tuple(pieces)
        self.__class__._validate(self)

    @classmethod
    def _validate(cls, v):
        if not type(v) is cls:
            raise TypeError()
        for dt,pn,p in zip(self.data_spec, self.piece_names, self.pieces):
            try: dt.validate(p)
            except TypeError: raise UnitDataFormatError(f"failed validation of {pn}")

    def __getitem__(self, key):
        return self.pieces[self.key_to_piece_index(key)]

    def __repr__(self):
        pcs = repr(tuple(self.pieces))
        return f"<{self.__class__.__name__} {id(self)}" \
               f" {pcs[:50]+(pcs[50:] or ' ...')}>"

    def __eq__(self, other):
        return type(self) == type(other) and self.pieces == other.pieces

    def __hash__(self):
        return hash(self.pieces)

class Unit(UnitBase):
    additional_data_defs = {'typeid':UnitTypeID}
    cached = None

class TXUnit(Unit):
    pass

class TXSubject(TXUnit):
    grammar = 'SUBJECT'

class TXModifier(TXUnit):
    grammar = 'MODIFIER'



# ARF

class ARFSpec():
    def __init__(self, inherit=None):
        if inherit is None:
            self._listing = {}
            self.txmods = []
        else:
            self._listing = inherit._listing.copy()
            self.txmods = inherit.txmods.copy()

    def __getitem__(self, key):
        return self._listing[key]

    def reverse_lookup(self, datatype):
        if isinstance(datatype, Unit):
            datatype = datatype.__class__
        elif not issubclass(datatype, Unit):
            raise TypeError()
        for k,v in self._listing:
            if v is datatype:
                return k
        raise LookupError()

    def __iter__(self):
        return iter(self._listing)

    # def has_index_or_unit_type(self, key):
    #     return key in self._listing or \
    #            (issubclass(key, Unit) and key in self._listing.values())

    def unit_types(self):
        return self._listing.values()

    def register(self, id):
        UnitTypeID.validate(id)
        if id in self._listing:
            raise ValueError("Unit type already registered")
        def d(ut):
            assert issubclass(ut, Unit)
            ok_range = UnitTypeID.arf_base_defined_range if \
                (ut.__module__ == __name__) else UnitTypeID.app_defined_range
            if id not in ok_range:
                raise ValueError("Unit Type ID out of acceptable range " \
                                f"{ok_range.start}..{ok_range.stop-1}")
            self._listing[id] = ut

            if issubclass(ut, TXModifier):
                self.txmods.append(ut)

            return ut
        return d

    def __repr__(self):
        return f"<{self.__class__.__name__} {id(self)} ({self._listing})>"

    def new(self, ut, *args, **kwargs):
        typeid = self.reverse_lookup(ut)
        return ut(typeid, *args, **kwargs)

base_spec = ARFSpec()

# Built-in Unit types

@base_spec.register(2)
class TxScopeMarker(Unit):
    additional_data_defs = {'prev-txs': TxScopeID, 'next-txs': TxScopeID}

@base_spec.register(3)
class TxScopeFinalize(TXModifier):
    additional_data_defs = {'is-commit': Bool}
    cached = 'is-commit'

@base_spec.register(4)
class StrandSelect(TXModifier):
    additional_data_defs = {'strd-id': StrandID}
    cached = 'strd-id'

    # def to_range(self):
    #     id = self['strd-id']
    #     return range(id, id+1)

@base_spec.register(5)
class StrandGroupSelect(TXModifier):
    additional_data_defs = {'strd-group': StrandID,
                            'strd-group-mag': StrandGroupMagnitude}
    cached = ('strd-group','strd-group-mag')

    def to_range(self):
        id = self['strd-group']
        mask = StrandGroupMagnitude.to_strand_group_mask(self['strd-group-mag'])
        start = id & ~mask
        stop = (id | mask) + 1
        return range(start, stop)

class StrandCompositeSelection:
    def __init__(self, *select_mods):
        self.singles = DenseIntegerSet()
        self.containers = [self.singles]
        for m in select_mods:
            self.add(m)

    def add(self, select_mod):
        if isinstance(select_mod, StrandSelect):
            self.singles.add(select_mod['strd-id'])
        elif isinstance(select_mod, StrandGroupSelect):
            self.containers.append(select_mod.to_range())

    def __contains__(self, strand_id):
        return any(strand_id in c for c in reversed(self.containers))

@base_spec.register(6)
class StrandWriteDataBlock(TXSubject):
    additional_data_defs = {'offset': StrandSize, 'data': StrandData}
    cached = 'offset'
    strand_selector = StrandSelect

@base_spec.register(7)
class StrandCreate(TXSubject):
    additional_data_defs = {'strd-size-bytes': StrandSize}
    cached = 'strd-size-bytes'
    strand_selector = StrandSelect

@base_spec.register(8)
class StrandDiscard(TXSubject):
    strand_selector = StrandGroupSelect

@base_spec.register(16)
class FrameMeta(Unit):
    additional_data_defs = {'stream-offset': UInt128}



# IO

class ARFIOWrapper(selfdelimitedblob.IO):
    def __init__(self, stream:io.BufferedIOBase, spec:ARFSpec):
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

    def read_next(self, select=None):
        """Read data from the unit next in stream. If the unit has been deleted,
        return None. Otherwise, the default behaviour is to return the data as a
        new Unit instance. Optionally, `select` can be a list of unit pieces to
        read, in which case a list of the resulting pieces is returned. Only
        data that is requested is read so if an empty list is provided, the
        function will perform only the minimal reads necessary to seek to the
        end of the unit in stream (at minimum, the unit type id will be read).
        """
        unit_pcs = [self._read_data(dt) for dt in Unit.data_spec]
        unit_typeid = unit_pcs[Unit.key_to_piece_index('typeid')]

        if unit_typeid in UnitTypeID.deleted_range:
            if unit_typeid == 1:
                while self._read_data(Bool):
                    pass # skip to end of deleted unit
            return None

        unit_type = self.spec[unit_typeid]
        if select is None:
            choice_indices = range(len(unit_type.data_spec))
        else:
            choice_indices = [unit_type.key_to_piece_index(k) for k in select]
            assert len(choice_indices) == len(set(choice_indices))

        for i,dt in islice(enumerate(unit_type.data_spec), start=len(unit_pcs)):
            if i in choice_indices:
                p = self._read_data(dt)
            else:
                self.stream.seek(self._get_next_piece_length(dt),1)
                p = None
            unit_pcs.append(p)

        if select is None:
            return unit_type(*unit_pcs) # unit instance by default
        return [unit_pcs[i] for i in select] # optionally, pick specific pieces

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
            #these bytes will be read as Bools:
            self.stream.write(b'\x01' * (sz - id_sz - 1))
            self.stream.write(b'\0')



# Mapping

class Queryable:
    # Queryables have the following defined:
    #
    #   iter_with_constraints (constraints:dict={})
    #
    #   __iter__ ()
    #
    #   mapper
    #
    pass

class ARFMapper(Queryable):

    class UnitInfo:
        __slots__ = ('store_id','txs','typeid','cached_pcs','mod_assoc')
        READ_REQUIRED = object()

        def __getitem__(self, k):
            """Get cached unit data, with a fall-back to a read operation on the stored
            unit.

            The various forms of `k`, and what the function returns for each:

            "store_id" : The id of the unit, assigned by the storage.
            "txs" :     Id of the transaction scope the unit is in (or None if the unit
                        is in the global scope).
            "typeid" :  Id of the unit's unit type.
            "cached_pcs" : Internal cached unit piece(s). Do not use.
            "mod_assoc" : Internal modifier associativity data. Do not use.
            "type" :    The unit's class.
            <Applicable mod class> : For a TXSubject, this returns the modifier id
                        (assigned by the mapper) for the modifier of the applicable
                        type. This can also return valid ids for future modifiers that
                        don't exist yet.
            "mod_id" :  If the unit is a TXModifier, return its modifier id.
            <Piece index or name> : The unit piece's value. Reads from storage if the
                        value is not cached.
            None :      Return the Unit itself, reading from storage if necessary.
            <tuple of other non-None `k` values> : A tuple containing results
                        corresponding to each key.
            """
            if k is None:
                ut = self.unit_type
                recursing_k = tuple(range(len(ut.data_spec)))
                return ut(self[recursing_k])

            multi = type(k) is tuple
            ks = k if multi else (k,)

            cache_results = [self._get_single_no_read(k) for k in ks]
            read_select = [k for k,r in zip(ks, cache_results)
                            if r is self.READ_REQUIRED]
            if read_select:
                read_results = iter(self.mapper.storage.read(self.store_id,
                                                        select=read_select))
            results = tuple((r if r is not self.READ_REQUIRED else \
                            next(read_results)) for r in cache_results)
            return results if multi else results[0]

        def _get_single_no_read(self, k):
            """Does single-item queries about the unit, but only returns values
            from cache, never storage. Returns UnitInfo.READ_REQUIRED if the
            item has existed, and may still, but isn't available in cache."""
            if k in self.__class__.__slots__:
                return getattr(self, k)

            ut = self.unit_type

            if k == 'type':
                return ut
            if issubclass(k, TXModifier) and issubclass(ut, TXSubject) and \
                k in self.mapper.ut_listing.txmods:
                return self.mod_assoc[self.mapper.ut_listing.txmods.index(k)]
            if k == 'mod_id' and issubclass(ut, TXModifier):
                return self.mod_assoc

            try:
                i = ut.key_to_piece_index(k)
            except TypeError:
                pass
            else:
                piece_name = ut.piece_names[i]
                if piece_name == ut.cached:
                    return self.cached_pcs
                if isinstance(ut.cached, (tuple,list)) and piece_name in ut.cached:
                    return self.cached_pcs[ut.cached.index(piece_name)]
                return self.READ_REQUIRED

            raise LookupError(f"No results for key {k}>")

        def __init__(self, store_id, unit_type, cur_txscope, txmod_ids):
            ut = unit_type
            ut_listing = self.mapper.ut_listing

            self.store_id = store_id
            self.txs = cur_txscope if issubclass(ut, TXUnit) else None
            self.typeid = ut_listing.reverse_lookup(ut)

            # load/set cached unit pieces
            if ut.cached is None:
                self.cached_pcs = None
            else:
                multi = isinstance(ut.cached, (tuple, list))
                select = ut.cached if multi else [ut.cached]
                data = self.mapper.storage.read(self.store_id, select=select)
                self.cached_pcs = data if multi else data[0]

            # set modifier associativity
            if issubclass(ut, TXModifier):
                self.mod_assoc = txmod_ids[ut_listing.txmods.index(ut)]
            elif issubclass(ut, TXSubject):
                self.mod_assoc = list(txsmod_ids) #copy
            else:
                self.mod_assoc = None

        @property
        def unit_type(self):
            return self.mapper.ut_listing[self.typeid]

    class Feed:
        def _nop(*args, **kwargs):
            pass

        def __init__(self, recv_extend = _nop, recv_delete = _nop):
            self.last_sync_id = -1
            self.recv_extend = recv_extend
            self.recv_delete = recv_delete

        def iter_new_units(self):
            for k,ui in self.mapper.iter_units(self.last_sync_id + 1):
                self.last_sync_id = k
                yield ui

        def notify_extend(self):
            self.recv_extend(self.iter_new_units())

        def notify_delete(self, k):
            if k <= self.last_sync_id:
                self.recv_delete(k)

    class UnitsMap(PerishablesSearchTreeMap):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.total_lifetime_units_mapped = 0
            self.last_notify_sync_id = -1
            self.last_sync_id = -1

        def __setitem__(self, k, v):
            if k <= self.last_sync_id:
                raise ValueError("Keys must be added to UnitsMap in order")
            super().__setitem__(k, v)
            self.last_sync_id = k
            self.total_lifetime_units_mapped += 1

        def maybe_send_notify_extend(self):
            if self.last_notify_sync_id != self.last_sync_id:
                for feed in self.mapper.all_feeds:
                    feed.notify_extend()
                self.last_notify_sync_id = self.last_sync_id

        def __delitem__(self, k):
            super().__delitem__(k)
            for feed in self.mapper.all_feeds:
                feed.notify_delete(k)

    # ARFMapper methods

    def __init__(self, unit_type_listing, storage):
        self.ut_listing = unit_type_listing
        self.storage = storage

        for c in (self.UnitInfo, self.Feed, self.UnitsMap):
            attrs = dict([('mapper',self), ('__slots__',())]
                            [:1 + hasattr(c, "__slots__")])
            newtype = type(c.__name__, (c,), attrs)
            newtype.__qualname__ = f"{repr(self)}.{newtype.__name__}"
            setattr(self, newtype.__name__, newtype)

        self.all_feeds = weakref.WeakSet()
        self.units = self.UnitsMap(self._unit_valid_test)

        self.cur_txscope = None
        self.mod_next_ids_per_txs = collections.defaultdict(
            lambda: [0] * len(unit_type_listing.txmods))

        self._sync_gen = _sync_gen_func()

    def _unit_valid_test(self, store_id):
        return store_id in self.storage

    def _map_unit(self, store_id, ut):
        assert not (issubclass(ut, TXUnit) and self.cur_txscope is None)
        mod_nexts:list = self.mod_next_ids_per_txs[self.cur_txscope] if \
                            (self.cur_txscope is not None) else None
        ui = self.UnitInfo(store_id, ut, self.cur_txscope, mod_nexts)
        self.units[store_id] = ui

        if ut is TxScopeMarker:
            prev_txs, next_txs = self.storage.read(
                store_id, select=['prev-txs','next-txs'])
            assert prev_txs == self.cur_txscope
            self.cur_txscope = next_txs
        elif issubclass(ut, TXModifier):
            if ut is TxScopeFinalize:
                for mod_i in range(len(mod_nexts)):
                    mod_nexts[mod_i] += 1
            else:
                mod_i = self.ut_listing.txmods.index(ut)
                mod_nexts[mod_i] += 1

    def _sync_gen_func(self):
        read_it = lambda st: self.storage.multi_read_iter(st, select=['typeid'])

        # sync global units, until any tx unit comes up
        cont_glob = True
        while cont_glob:
            for store_id, (typeid,) in read_it(self.units.last_sync_id + 1):
                ut = self.ut_listing[typeid]
                if issubclass(ut, TXUnit):
                    cont_glob = False
                    break
                self._map_unit(store_id, ut)
            else:
                yield

        # next, idle until a txs marker shows up
        last_scan_ahead_id = self.units.last_sync_id
        txs_marker_typeid = self.ut_listing.reverse_lookup(TxScopeMarker)
        while self.cur_txscope is None:
            for store_id, (typeid,) in read_it(last_scan_ahead_id + 1):
                last_scan_ahead_id = store_id
                if typeid == txs_marker_typeid:
                    self.cur_txscope = self.storage.read(store_id, select=['prev-txs'])
                    break
            else:
                yield

        # main loop
        while True:
            for store_id, (typeid,) in read_it(self.units.last_sync_id + 1):
                self._map_unit(store_id, self.ut_listing[typeid])
            yield

    # ARFMapper interface!

    def sync(self):
        next(self._sync_gen)
        self.units.maybe_send_notify_extend()

    def __getitem__(self, k):
        if k > self.units.last_sync_id:
            self.sync()
        return self.units[k]

    def get(self, k):
        try:
            return self[k]
        except KeyError:
            return None

    def __contains__(self, k):
        return self.get(k) is not None

    def iter_units(self, start=0):
        self.units.try_release_expired()
        yield from self.units[start:].items()

        last_id_before_sync = self.units.last_sync_id
        self.sync()
        yield from self.units[max(last_id_before_sync + 1, start):].items()

    def getfeed(self, *args, **kwargs):
        feed = self.mapper.Feed(*args, **kwargs)
        self.all_feeds.add(feed)
        return feed

    def __iter__(self):
        return (k for k,v in self.iter_units())

    def iter_with_constraints(self, constraints:dict={}):
        if constraints != {}:
            raise ValueError("Mappers are not capable of subselections" \
                             " using constraints.")
        return iter(self)

    @property
    def mapper(self):
        return self



class ARFMapperIndex(Queryable):
    @dataclass
    class KeyDef:
        name: collections.abc.Hashable
        sliceable: bool = False

    class UniquesMapMixin:
        def test_valid(self, k):
            v = super(PerishablesMapInterfaceMixin, self).__getitem__(k)
            return self.__dict__['test_valid'](v)
    class UniquesMap(UniquesMapMixin, PerishablesMap): pass
    class UniquesSearchTreeMap(UniquesMapMixin, PerishablesSearchTreeMap): pass

    def __init__(self, keydefs, mapper:ARFMapper, unique=False, selector=None):
        self.keydefs = keydefs
        self.unique = unique
        self.selector = selector or (lambda x: True)
        self.mapper = mapper
        self.well_sorted = True

        def map_factory_for(keydef_i=0):
            keydef = keydefs[keydef_i]
            cont_cls = { False: AutoContainerMap,
                         True: AutoContainerSearchTreeMap }[keydef.sliceable]
            uniq_cls = { False: self.UniquesMap,
                         True: self.UniquesSearchTreeMap }[keydef.sliceable]

            if keydef_i != (len(keydefs) - 1):
                inner = map_factory_for(keydef_i + 1)
                return lambda: cont_cls(inner)
            # else, it's the final keydef
            test = self.mapper._unit_valid_test
            if unique:
                return lambda: uniq_cls(test)
            return lambda: cont_cls(lambda: PerishablesSet(test))

        self.maps = map_factory_for()() # fails if no keydefs

    def maybe_add_unit(self, unit_info):
        assert unit_info.mapper is self.mapper
        if self.selector(unit_info):
            self._add_unit(unit_info)

    def mapkey_for(self, unit_info):
        map_ = self.maps
        for kd in self.keydefs:
            k = unit_info[kd.name]
            if kd is not self.keydefs[-1]:
                map_ = map_[k]
        return map_, k

    def _add_unit(self, unit_info):
        map_, k = self.mapkey_for(unit_info)
        store_id = unit_info['store_id']

        if self.unique:
            assert k not in map_
            map_[k] = store_id
        else:
            if store_id < getattr(map_[k], "last_add", -1):
                self.well_sorted = False
            map_[k].add(store_id)

    def discard_unit(self, unit_info):
        map_, k = self.mapkey_for(unit_info)

        if self.unique:
            if k in map_:
                if map_.try_release_expired():
                    # k should still exist after try_release_expired. If not it
                    # implies unit_info isn't valid because k isn't in storage.
                    del map_[k]
                else:
                    raise RuntimeError(f"Can't delete {k}, map is locked.")
        else:
            map_.discard(k)

    def iter_with_constraints(self, constraints:dict={}):
        if not constraints.keys() <= set(kd.name for kd in self.keydefs):
            ValueError("a key doesn't exist")
        def search_gen(map_, kds):
            kd, *next_kds = kds
            constraint = constraints.get(kd.name)
            if constraint is None:
                it = map_.values()
            elif type(constraint) is slice:
                if not kd.sliceable:
                    raise TypeError("key is not a slicing type")
                it = map_.islice(constraint.start, constraint.stop)
            else:
                if not isinstance(constraint, frozenset):
                    constraint = (constraint,)
                it = (map_[k] for k in constraint if k in map_)

            if not next_kds:
                yield from it
            else:
                for m in it:
                    yield from search_gen(m, next_kds)

        results = search_gen(self.maps, self.keydefs)
        if self.unique:
            return iter(sorted(results))
        else:
            if self.well_sorted:
                return heapq.merge(*results)
            else:
                return iter(sorted(itertools.chain(*results)))

    def __iter__(self):
        return self.iter_with_constraints({})

    def unique_keys_on(self, keydef_name):
        for i,kd in enumerate(self.keydefs):
            if kd.name == keydef_name:
                on_keydef_i = i
        else:
            raise KeyError("Index doesn't have a keydef by that name")

        def k_gen(map_=self.maps, kd_i=0):
            if kd_i == on_keydef_i:
                yield from map_.keys()
            else:
                for m in map_.values():
                    yield from search_gen(m, kd_i+1)

        results = set()
        for k in k_gen():
            if k not in results:
                yield k
            results.add(k)



class Query:
    def __init__(self, queryable:Queryable, constraints:dict={}):
        self.queryable = queryable
        self.constraints = constraints
        self.ops = []

    def _plus_op(self, op):
        new = self.__class__(self.queryable, self.constraints)
        new.ops = self.ops.copy()
        new.ops.append(op)
        return new

    def _filter_ids_impl(self, iterator, f):
        return filter(f, iterator)

    def _join_impl(self, iterator, other_query):
        it = (iterator, iter(other_query))
        try:
            v = [next(it[0]), next(it[1])]
            while True:
                s = v[1]<v[0], v[0]<v[1]
                if s == (0,0): yield v[0]
                for i in (0,1): v[i] = v[i] if s[i] else next(it[i])
        except StopIteration:
            pass

    def _merge_impl(self, iterator, other_queries):
        prev = object()
        for id_ in heapq.merge(iterator, *other_queries):
            if id_ != prev:
                yield id_
            prev = id_

    def filter_ids(self, f):
        return self._plus_op((self._filter_ids_impl, f))

    def join(self, other_query):
        return self._plus_op((self._join_impl, other_query))

    def merge(self, *other_queries):
        return self._plus_op((self._merge_impl, other_queries))

    def _keys_iter(self):
        it = self.queryable.iter_with_constraints(self.constraints)
        for func, *args in self.ops:
            it = func(it, *args)
        return it

    def one(self):
        r = list(islice(self._keys_iter(), 2))
        if len(r) != 1:
            raise LookupError("Result set is not exactly one element.")
        return self.queryable.mapper[r[0]]

    def __iter__(self):
        return (self.queryable.mapper[k] for k in self._keys_iter())

    def exists(self):
        return bool(list(islice(self._keys_iter(), 1)))

    def count(self):
        return sum(1 for _ in self._keys_iter())



class SubjectWithContext:
    def __init__(self, subj:ARFMapper.UnitInfo, mods:Query, strict=True):
        self.subj = subj
        ut = self.unit_type = subj["type"]

        if subj.mapper != mods.queryable.mapper:
            raise TypeError()
        if not issubclass(ut, TXSubject):
            raise TypeError()

        self.mods = {}
        for m in mods:
            if not issubclass(m["type"], TXModifier):
                if strict:
                    raise TypeError("Not all units in `mods` are modifiers.")
                else:
                    continue
            if not (subj["txs"] == m["txs"] and
                    subj[m["type"]] == m["mod_id"]):
                if strict:
                    raise ValueError("A modifier doesn't affect the subject.")
                else:
                    continue
            self.mods[m["type"]] = m

        # An exception will be raised if content order is compared between a
        # subject with a finalize mod and one without. This is deliberate.
        # They should not be compared.
        if TXScopeFinalize in self.mods:
            finalize = self.mods[TxScopeFinalize]
            if not finalize["is-commit"]:
                raise ValueError("Subject can't have context because it is discarded")
            self.content_order = (finalize["store_id"], subj["store_id"])
        else:
            self.content_order = subj["store_id"]

        if getattr(ut,"strand_selector",None) == StrandSelect:
            self.strand = self.mods[StrandSelect]["strd-id"]
        if ut is StrandDiscard:
            self.discard_strands = self.mods[StrandGroupSelect][None].to_range()

    def __getitem__(self, k):
        return self.subj[k]



class OcclusionTests:
    def __init__(self):
        self.tests = []
        self.types_included = set()

    def copy(self):
        obj = self.__class__()
        obj.tests = self.tests.copy()
        return obj

    def register(self, rear_type, fore_type):
        types = (rear_type, fore_type)
        def d(func):
            self.tests.append((types, func))
            for t in types:
                if t is not object:
                    self.types_included.add(t)
            return func
        return d

    def __getitem__(self, k):
        rear_type, fore_type = k
        funcs = []
        for (rt,ft),func in self.tests:
            if (issubclass(rear_type,rt) and issubclass(fore_type,ft)):
                funcs.append(func)
        def test(rear_subj, fore_subj):
            return any(f(rear_subj, fore_subj) for f in funcs)
        return test

@call
def occlusion_tests():
    tests = OcclusionTests()
    reg = tests.register

    @reg(StrandDiscard, object)
    def t(rear, fore):
        return True

    @reg(StrandCreate,         StrandDiscard)
    @reg(StrandWriteDataBlock, StrandDiscard)
    def t(rear, fore):
        return rear.strand in fore.discard_strands

    @reg(StrandWriteDataBlock, StrandWriteDataBlock)
    def t(rear, fore):
        return rear["offset"] == fore["offset"] and \
               rear.strand == fore.strand

    @reg(StrandCreate, StrandCreate)
    def t(rear, fore):
        return rear.strand == fore.strand

    @reg(StrandWriteDataBlock, StrandCreate)
    def t(rear, fore):
        return rear.strand == fore.strand and \
               rear["offset"] <= fore["strd-size-bytes"]

    return tests



class Content:
    def __init__(self, unit_infos=(), mapper:ARFMapper=None):
        try:
            self.mapper = mapper or (unit_infos.queryable.mapper if \
                hasattr(unit_infos, "queryable") else \
                next(iter(unit_infos)).mapper)
        except StopIteration:
            raise TypeError("Can't determine mapper for Content")
        self._make_indexes()
        for ui in unit_infos:
            self._add_unit(ui)

        # Test for occlusions, but in addition, this has the side-effect of
        # validating all subjects, by calling _context_for() for each.
        for _ in self.calc_occlusions(self):
            raise ValueError("Can't create Content with conflicting subjects")

    def _make_indexes(self):
        K = ARFMapperIndex.KeyDef
        # For Content's primary uses, units will be coming in out of
        # chronological order, as transactions are only applied when actually
        # committed. Therefore every ARFMapperIndex should be keyed with "txs"
        # so that each subcontainer remains "well-sorted", which improves
        # efficiency of querying results.
        self.subjects = ARFMapperIndex([K("txs"), K("type")], self.mapper)
        self.modifiers = ARFMapperIndex([K(("txs","type","mod_id"))],
                                        mapper=self.mapper, unique=True)
        self.indexes = {'SUBJECT': self.subjects, 'MODIFIER': self.modifiers}

    def _add_unit(self, unit_info):
        ut = unit_info["type"]
        try:
            assert issubclass(ut, TXUnit)
            index = self.indexes[ut.grammar]
        except AssertionError, KeyError as e:
            raise TypeError("Only transaction units are allowed as content", e)
        index.maybe_add_unit(ui)

    def _context_for(self, subj:ARFMapper.UnitInfo):
        mod_types = self.mapper.ut_listing.txmods
        mod_index_ks = frozenset((subj[txs], mt, subj[mt]) for mt in mod_types)
        q = Query(self.modifiers, {("txs","type","mod_id"):mod_index_ks})
        return SubjectWithContext(subj, q)

    def __iter__(self):
        """Returns all subjects as SubjectWithContexts, in content order."""
        def txs_gen(txs):
            for ui in Query(self.subjects, {"txs": txs}):
                yield self._context_for(ui)
        its = (txs_gen(txs) for txs in self.subjects.unique_keys_on("txs"))
        return heapq.merge(*its, lambda subj: subj.content_order)

    def iter_stream_order(self):
        return (self._context_for(s) for s in Query(self.subjects))

    def calc_occlusions(self, fore):
        """Compare `fore` against each subject in this Content, returning the
        store_id of all subjects which are occluded by fore. `fore` can be
        either a SubjectWithContext, or a Context containing any number of
        subjects. When a fore-subject is contained in this Context as well, this
        function will not test against subjects that occur after the
        fore-subject in the content order. ie.: for a given fore-subject, this
        function only tests subjects that are actually considered to be "behind"
        the fore-subject)."""
        assert set(self.subjects.unique_keys_on("type")) \
               <= occlusion_tests.types_included \
               < self.mapper.ut_listing.unit_types

        if isinstance(fore, SubjectWithContext):
            yield from self._calc_occlusions_single(fore)
        else:
            results = DenseIntegerSet()
            for fore_subj in fore:
                for occ in self._calc_occlusions_single(fore_subj, results):
                    if occ not in results:
                        yield occ
                        results.add(occ)

    def _calc_occlusions_single(self, fore_subj:SubjectWithContext):
        tests = {t:occlusion_tests[t, fore_subj["type"]] for t in
                 occlusion_tests.types_included}

        for rear_subj in iter(self):
            if rear_subj["store_id"] == fore_subj["store_id"]:
                return
            if tests[rear_subj["type"]](rear_subj, fore_subj):
                yield rear_subj["store_id"]

    def calc_unused_mods(self):
        # Super low-effort implementation, could be made more efficient if needed
        unused_mods = DenseIntegerSet(self.modifiers)
        for subj in iter(self):
            for mod in subj.mods.values():
                unused_mods.remove(mod["store_id"])
        return unused_mods

    def merge_in(self, other):
        if other.mapper != self.mapper:
            raise ValueError("Can only merge Contents that share a mapper/storage.")
        # Remove occluded, obsolete units
        subjects_to_remove = DenseIntegerSet(self.calc_occlusions(other))
        if subjects_to_remove:
            discard = self.mapper.storage.discard
            for id_ in subjects_to_remove:
                discard(id_)
            for id_ in calc_unused_mods():
                discard(id_)
        # Now add new
        it = itertools.chain(Query(other.subjects), Query(other.modifiers))
        for ui in it:
            self._add_unit(ui)



class OpenTXsIndex(ARFMapperIndex):
    def __init__(self, mapper, export_commit):
        super().__init__([K("txs"), K("type")], mapper)
        self.export_commit = export_commit
        self.active_scopes = collections.defaultdict(StrandCompositeSelection)
 
    def maybe_add_unit(self, unit_info):
        if not issubclass(unit_info["type"], TXUnit):
            return

        self._add_unit(unit_info)

        txs = unit_info["txs"]
        strand_selections = self.active_scopes[txs]

        if ut in (StrandSelect, StrandGroupSelect):
            strand_selections.add(unit_info[None])
        elif ut is TxScopeFinalize:
            del self.active_scopes[txs]
            tx_uis = list(Query(self, {"txs":txs}))
            for ui in tx_uis:
                self.discard_unit(ui)
            if unit_info["is_commit"]:
                self.export_commit(Content(tx_uis))



class ARFIndexer:
    def __init__(self, unit_type_listing, storage):
        self.mapper = mapper = ARFMapper(unit_type_listing, storage)

        self.globals = ARFMapperIndex([ARFMapperIndex.KeyDef("type")], mapper,
                        lambda ui: not issubclass(ui["type"], TXUnit))
        self.committed = Content(mapper=mapper)
        self.open_transactions = OpenTXsIndex(mapper, self.committed.merge_in)

        self.feed = mapper.getfeed(recv_extend=self._recv_extend)
        self.feed.notify_extend()

    def _recv_extend(self, iterator):
        for ui in iterator:
            self._add_unit(ui)

    def _add_unit(self, ui:ARFMapper.UnitInfo):
        self.globals.maybe_add_unit(ui)
        self.open_transactions.maybe_add_unit(ui)



class TransactionComposer:
    def __init__(self, indexer:ARFIndexer, new_units, txs:int=None):
        self.indexer = indexer

        self._made_txses = set()
        if txs is None:
            txs = self.make_txs()
        TxScopeID.validate(txs)
        self.txs = txs

        def iowrap(s): return ARFIOWrapper(s, indexer.mapper.ut_listing)
        storage = selfdelimitedblob.MemoryOnlyStorage(f"tx{txs}", iowrap)
        mapper = ARFMapper(indexer.mapper.ut_listing, storage)
        storage.append(indexer.mapper.ut_listing.new(TXScopeMarker, 0, 0))
        for unit in new_units:
            storage.append(unit)
        self.tx = Content(Query(mapper))

        self.occlusions = DenseIntegerSet(indexer.committed.calc_occlusions(self.tx))

    def make_txs(self):
        txscopes = self.indexer.open_transactions.active_scopes
        try:
            TxScopeID.validate((len(txscopes) + len(self._made_txses)) << 1)
        except TypeError:
            raise RuntimeError("Out of assignable transaction scope ids.")
        while True:
            txs = random.getrandbits(TxScopeID.bit_length)
            if txs not in txscopes and txs not in self._made_txses:
                self._made_txses.add(txs)
                return txs

    def all_committed(self):
        return self.indexer.committed.iter_stream_order()

    def unoccluded_committed(self):
        return filter(lambda x: x not in self.occlusions, self.all_committed())



# Default Transit Handler

class TransitHandler:
    pass

class LoopbackTransitHandler(TransitHandler):
    def __init__(self, storage):
        pass

    def send(self, units):
        pass



# ARF?

class ARFStrand(io.BytesIO):
    pass

class ARFManager:
    def __init__(self, cache=None,
                       transit=None,
                       unit_type_listing=base_spec):
        pass

    def e(self) -> io.BytesIO:
        pass

class ARFObject:
    pass

print("hi")
