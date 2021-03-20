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



# ARF

class ARFSpec():
    def __init__(self, inherit=None):
        if inherit is None:
            self._listing = {}
            self.txs_mods = []
            self.glob_mods = []
        else:
            self._listing = inherit._listing.copy()
            self.txs_mods = inherit.txs_mods.copy()
            self.glob_mods = inherit.glob_mods.copy()
        self._all_mods = []

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

    @property
    def all_mods(self):
        if len(self._all_mods) != (len(self.txs_mods) + len(self.glob_mods)):
            self._all_mods = self.txs_mods + self.glob_mods
        return self._all_mods

    def applicable_mod_types_for(self, subject_ut):
        if not subject_ut in self.unit_types():
            raise LookupError()
        if subject_ut.grammar != 'SUBJECT':
            return [] # only SUBJECTs can have mods applied
        if subject_ut.scope == 'TX':
            return self.all_mods
        assert subject_ut.scope == 'GLOBAL'
        return self.glob_mods

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

            if ut.grammar == 'MODIFIER':
                if ut.scope == 'TX':
                    self.txs_mods.append(ut)
                elif ut.scope == 'GLOBAL':
                    self.glob_mods.append(ut)

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
    grammar = 'SCOPE-CONTROLLER'
    scope = 'GLOBAL'
    persistence = 'ELAPSING'

@base_spec.register(3)
class TxScopeFinalize(Unit):
    additional_data_defs = {'is-commit': Bool}
    cached = 'is-commit'
    grammar = 'MODIFIER'
    scope = 'TX'
    persistence = 'ELAPSING'

@base_spec.register(4)
class StrandSelect(Unit):
    additional_data_defs = {'strd-id': StrandID}
    cached = 'strd-id'
    grammar = 'MODIFIER'
    scope = 'TX'
    persistence = 'REFRESHING'

@base_spec.register(5)
class StrandGroupSelect(Unit):
    additional_data_defs = {'strd-group': StrandID,
                            'strd-group-mag': StrandGroupMagnitude}
    cached = ('strd-group','strd-group-mag')
    grammar = 'MODIFIER'
    scope = 'TX'
    persistence = 'REFRESHING'

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
class StrandWriteDataBlock(Unit):
    additional_data_defs = {'offset': StrandSize, 'data': StrandData}
    cached = 'offset'
    grammar = 'SUBJECT'
    scope = 'TX'
    persistence = 'REFRESHING'
    differentiators = ('offset', StrandSelect)

@base_spec.register(7)
class StrandCreate(Unit):
    additional_data_defs = {'strd-size-bytes': StrandSize}
    cached = 'strd-size-bytes'
    grammar = 'SUBJECT'
    scope = 'TX'
    persistence = 'REFRESHING'
    differentiators = (StrandSelect,)

@base_spec.register(8)
class StrandDiscard(Unit):
    additional_data_defs = {}
    grammar ='SUBJECT'
    scope ='TX'
    persistence = 'ELAPSING'



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
            <Applicable mod class> : For a unit with the 'SUBJECT' grammar role, this
                        returns the modifier id (assigned by the mapper) for the
                        modifier of the applicable type. This can also return valid ids
                        for future modifiers that don't exist yet.
            "mod_id" :  If the unit is a modifier, return its modifier id.
            None :      Do a full read from storage to return the Unit itself.
            <Piece index or name> : The unit piece's value. Reads from storage if the
                        value is not cached.
            <tuple of other non-None `k` values> : a tuple containing results
                        corresponding to each key.
            """
            if k is None:
                return self.mapper.storage.read(self.store_id, select=[])
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
            if issubclass(k, Unit) and \
                    k in self.mapper.ut_listing.applicable_mod_types_for(ut):
                return self.mod_assoc[self.mapper.ut_listing.all_mods.index(k)]
            if ut.grammar == 'MODIFIER' and k == 'mod_id':
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

        def __init__(self, store_id, unit_type, cur_txscope, glob_mod_ids,
                     txs_mod_ids):
            ut = unit_type
            ut_listing = self.mapper.ut_listing

            self.store_id = store_id
            self.txs = cur_txscope if (ut.scope == 'TX') else None
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
            if ut.grammar == 'MODIFIER':
                ids_for_scope, mod_types_for_scope = {
                    'GLOBAL': (glob_mod_ids, ut_listing.glob_mods),
                    'TX': (txs_mod_ids, ut_listing.txs_mods)
                }[ut.scope]
                self.mod_assoc = ids_for_scope[mod_types_for_scope.index(ut)]
            elif ut.grammar == 'SUBJECT':
                self.mod_assoc = list(glob_mod_ids) #copy
                if ut.scope == 'TX':
                    self.mod_assoc += txs_mod_ids
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
            lambda: [0] * len(unit_type_listing.txs_mods))
        self.mod_next_ids_per_txs[None] = [0] * len(unit_type_listing.glob_mods)

        self._sync_gen = _sync_gen_func()

    def _unit_valid_test(self, store_id):
        return store_id in self.storage

    def _map_unit(self, store_id, ut):
        assert ut.scope == 'GLOBAL' or self.cur_txscope is not None
        ui = self.UnitInfo(store_id, ut, self.cur_txscope,
                           self.mod_next_ids_per_txs[None],
                           self.mod_next_ids_per_txs[self.cur_txscope])
        self.units[store_id] = ui

        mod_nexts:list = self.mod_next_ids_per_txs[ui['txs']]
        if ut is TxScopeMarker:
            prev_txs, next_txs = self.storage.read(
                store_id, select=['prev-txs','next-txs'])
            assert prev_txs == self.cur_txscope
            self.cur_txscope = next_txs
        elif ut is TxScopeFinalize:
            for mod_i in range(len(mod_nexts)):
                mod_nexts[mod_i] += 1
        elif ut.grammar == 'MODIFIER':
            mod_i = self.ut_listing.txs_mods.index(ut) if ut.scope == 'TX' \
                    else self.ut_listing.glob_mods.index(ut)
            mod_nexts[mod_i] += 1

    def _sync_gen_func(self):
        read_it = lambda st: self.storage.multi_read_iter(st, select=['typeid'])

        # sync global units, until any tx unit comes up
        cont_glob = True
        while cont_glob:
            for store_id, (typeid,) in read_it(self.units.last_sync_id + 1):
                ut = self.ut_listing[typeid]
                if ut.scope != 'GLOBAL' or ut.grammar == 'SCOPE-CONTROLLER':
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
        name: str
        sliceable: bool = False

    class UniquesMapMixin:
        def test_valid(self, k):
            v = super(PerishablesMapInterfaceMixin, self).__getitem__(k)
            return self.__dict__['test_valid'](v)
    class UniquesMap(UniquesMapMixin, PerishablesMap): pass
    class UniquesSearchTreeMap(UniquesMapMixin, PerishablesSearchTreeMap): pass

    def __init__(self, *keydefs, unique=False, selector=None, mapper:ARFMapper):
        self.keydefs = keydefs
        self.unique = unique
        self.selector = selector or (lambda x: True)
        self.mapper = mapper

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

        if self.unique:
            assert k not in map_
            map_[k] = unit_info['store_id']
        else:
            map_[k].add(unit_info['store_id'])

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
            return heapq.merge(*results)

    def __iter__(self):
        return self.iter_with_constraints({})



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

    def _filter_impl(self, iterator, f):
        return filter(f, iterator)

    def _join_impl(self, iterator, other_query):
        end = object()
        iters = (iterator, iter(other_query))
        values = [next(iters[0]), next(iters[1])]
        try:
            while end not in values:
                if values[0] == values[1]:
                    yield values[0]
                    values = [next(it, end) for it in iters]
                else:
                    adv_i = int(iters[1] < iters[0])
                    values[adv_i] = next(iters[adv_i])
            yield [x for x in values if x is not end]
        except StopIteration:
            pass
        yield from iters[0]
        yield from iters[1]

    def filter(self, f):
        return self._plus_op((self._filter_impl, f))

    def join(self, other_query):
        return self._plus_op((self._join_impl, other_query))

    def _keys_iter(self):
        it = self.queryable.iter_with_constraints(self.constraints)
        for func, *args in self.ops:
            it = func(it, *args)
        return it

    def one(self):
        r = list(islice(self._keys_iter(), 2))
        if len(r) != 1:
            raise ValueError("Result set is not exactly one element.")
        return self.queryable.mapper[r[0]]

    def __iter__(self):
        return (self.queryable.mapper[k] for k in self._keys_iter())

    def exists(self):
        return bool(list(islice(self._keys_iter(), 1)))

    def count(self):
        return sum(1 for _ in self._keys_iter())



class OpenTXsIndex(ARFMapperIndex):
    def __init__(self, mapper, export_commit):
        super().__init__(K("txs"), K("type"), mapper=mapper)
        self.export_commit = export_commit
        self.active_scopes = collections.defaultdict(StrandCompositeSelection)

    def maybe_add_unit(self, unit_info):
        if not (unit_info["type"].scope == "TX" and
                unit_info["type"].grammar in ('SUBJECT','MODIFIER')):
            return

        self._add_unit(unit_info)

        txs = unit_info["txs"]
        strand_selections = self.active_scopes[txs]

        if unit_info["type"] in (StrandSelect, StrandGroupSelect):
            strand_selections.add(unit_info[None])
        elif unit_info["type"] is TxScopeFinalize:
            del self.active_scopes[txs]
            tx = collections.deque(Query(self, {"txs":txs}))
            for ui in tx:
                self.discard_unit(ui)
            if unit_info["is_commit"]:
                self.export_commit(tx)



class ARFIndexer:
    def __init__(self, unit_type_listing, storage):
        self.mapper = mapper = ARFMapper(unit_type_listing, storage)

        self.open_transactions = OpenTXsIndex(mapper, self._process_commit)
        K = ARFMapperIndex.KeyDef
        self.committed_subjects = ARFMapperIndex(K("type"), mapper=mapper)
        self.committed_modifiers = ARFMapperIndex(
            K(("txs","type","mod_id")),
            unique=True, mapper=mapper)

        self.feed = mapper.getfeed(recv_extend=self._recv_extend)
        self.feed.notify_extend()

    def _recv_extend(self, iterator):
        for ui in iterator:
            self.open_transactions.maybe_add_unit(ui)

    def _process_commit(self, tx):
        subjects = []
        while tx:
            ui = tx.popleft()
            if ui["type"].grammar == 'SUBJECT':
                subjects.append(ui)
            elif ui["type"].grammar == 'MODIFIER':
                self.committed_modifiers.maybe_add_unit(ui)
            else:
                assert False
        del tx

        raise NotImplementedError()



class TransactionWriter:
    def __init__(self, reader:ARFReader, new_units, txs:int=None):
        self.reader = reader
        self.new_units = self.sort_units_by_mod_group_size(new_units)

        if txs is None:
            try:
                TxScopeID.validate(len(self.reader.mapper.active_txscopes) << 1)
            except TypeError:
                raise RuntimeError("Out of assignable transaction scope ids.")
            while True:
                txs = random.getrandbits(TxScopeID.bit_length)
                if txs not in self.reader.mapper.active_txscopes:
                    break
        TxScopeID.validate(txs)
        self.txs = txs

    @staticmethod
    def sort_units_by_mod_group_size(assoc_unit_groups):
        for subj, *mods in assoc_unit_groups:
            if not (u.grammar == 'SUBJECT' and \
                    all(m.grammar == 'MODIFIER' for m in mods)):
                raise TypeError()

        r = []
        while assoc_unit_groups:
            mod_counts = collections.Counter()
            for subj, *mods in assoc_unit_groups:
                mod_counts.update(mods)
            most_common = mod_counts.most_common(1)[0][0]

            defered = []
            for units in assoc_unit_groups:
                l = r if (most_common in units) else defered
                l.append(most_common)
            assoc_unit_groups = defered
        return r

    def subjects_to_refresh(self):
        # - subjects
        # - "REFRESHING" type
        # - committed
        # - not occluded
        # - exists (not expired), obviously
        pass



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
