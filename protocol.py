#! Python3.7

# Copyright: Jason Forbes

VERSION = "2r1"

# Require Python 3.7 or later
import sys
assert (sys.version_info.major, sys.version_info.minor) >= (3, 7)

import base64, re



#data types

class Data:
    @staticmethod
    def validate(v):
        raise NotImplemented()

class UInt(Data):
    bits = 32
    @classmethod
    def validate(cls, v):
        return type(v) is int and v in range(2 ** cls.bits)

class SafeULong(UInt):
    """SafeULong is a 53-bit unsigned integer.
    
    Apparently, according to the spec, JSON only officially supports integers
    up to 2^53-1. It has to do with the way numbers are represented in data
    in Javascript; all real numbers are actually floats."""
    bits = 53

class Bool(Data):
    @staticmethod
    def validate(v):
        return type(v) is bool

class B64(Data):
    encoded_length = None
    @classmethod
    def validate(cls, v):
        try:
            base64.b64decode(v, validate=True)
        except:
            return False
        if cls.encoded_length is not None and \
           len(v) != cls.encoded_length:
            return False
        return True

class B64KeySized(B64):
    encoded_length = 24

class Word(Data):
    minmax_lengths = (None, None)
    @classmethod
    def validate(cls, v):
        exp = r'^\w{{{},{}}}$'.format(*cls.minmax_lengths)
        return type(v) is str and re.match(exp, v) is not None

class VersionString(Word):
    minmax_lengths = (1, 10)

class ServInfo(Data):
    @staticmethod
    def validate(v):
        if type(v) is not dict:
            return False
        k_format = re.compile(r"^[a-z](_?[a-z0-9]+)*$")
        for k,vv in v.items():
            if not k_format.match(k) or \
               (not SafeULong.validate(vv) and type(vv) is not str):
                return False
        return True

class Record(Data):
    @classmethod
    def validate(cls, v):
        if type(v) is not list or len(v) is not len(cls.fields):
            return False
        # Python documentation says: "Changed in version 3.7: Dictionary
        # order is guaranteed to be insertion order", therefore
        # iterating over this in an order-dependent way is safe.
        return all(t.validate(x) for x,t in zip(v, cls.fields.values()))
    @classmethod
    def to_dict(cls, v):
        if not cls.validate(v):
            raise TypeError("not a valid {} object".format(cls.__name__))
        return dict(zip(cls.fields, v))
    @classmethod
    def from_dict(cls, d, *, strict=False):
        r = []
        for k,t in cls.fields.items():
            try:
                v = d[k]
                if not t.validate(v):
                    raise Exception()
            except:
                txt = f"Cannot create valid {cls.__name__} from object."
                raise TypeError(txt)
            r.append(v)
        if strict and (type(d) is not dict or d != cls.to_dict(r)):
            raise TypeError("Object is not valid in strict mode.")
        return r

class SpoolMetadata(Record):
    fields = dict(
        block_size = UInt,
        block_count = SafeULong,
        head_offset = SafeULong,
        tail_offset = SafeULong,
        annotation = SpoolAnnotation
    )

class SpoolAccessInfo(Record):
    fields = dict(
        spool_id = UInt,
        can_read = Bool,
        can_append = Bool,
        can_truncate = Bool
    )

class SpoolAnnotation(Data):
    @staticmethod
    def validate(v):
        exp = r"^[ \w\-+=/,.!#:;]{0,32}$"
        return type(v) is str and re.match(exp, v) is not None



#requests

#default request
class Request:
    method = "GET"
    params = {}
    result = None
    file = False
    owner_only = False
    owner_params = { 'owner_key': B64KeySized }
    @staticmethod
    def handler():
        raise NotImplemented()
    def client_params(self):
        if not owner_only:
            return self.params
        params = self.params.copy()
        for k in (set(params) & set(self.owner_params)):
            assert params[k] == self.owner_params[k]
        params.update(owner_params)
        return params

class SERV_PROTOCOL_VERSION(Request):
    result = VersionString
    @staticmethod
    def handler():
        return VERSION

class SERV_INFO(Request):
    result = ServInfo

class SPOOL_LIST(Request):
    owner_only = True
    result = [ UInt ]

class SPOOL_NEW(Request):
    method = "POST"
    owner_only = True
    params = dict(
        block_size = UInt,
        block_count = SafeULong,
        annotation = SpoolAnnotation
    )
    result = UInt

class SPOOL_DELETE(Request):
    method = "POST"
    owner_only = True
    params = { 'id': UInt }

class ACCESS_LIST(Request):
    owner_only = True
    params = { 'spool_id': UInt }
    result = [ B64KeySized ]

class ACCESS_INFO(Request):
    owner_only = True
    params = { 'k': B64KeySized }
    result = SpoolAccessInfo

class ACCESS_INFO_UPDATE(Request):
    method = "POST"
    owner_only = True
    params = { 'k': B64KeySized, 'info': SpoolAccessInfo }

class ACCESS_REVOKE(Request):
    method = "POST"
    owner_only = True
    params = { 'k': B64KeySized }

class METADATA_FETCH(Request):
    params = { 'k': B64KeySized }
    result = SpoolMetadata

class DATA_READ(Request):
    params = { 'k': B64KeySized,
        'start_offset': SafeULong,
        'block_count': UInt }
    result = B64

class DATA_APPEND(Request):
    method = "POST"
    file = True
    params = { 'k': B64KeySized, 'head_offset': SafeULong }

class DATA_TRUNCATE(Request):
    method = "POST"
    params = { 'k': B64KeySized, 'tail_offset': SafeULong }



#utility functions

def extract_args(param_definition, data):
    assert type(param_definition) is dict
    def gen():
        for name,datatype in param_definition.items():
            try:
                v = data[name]
            except KeyError:
                raise TypeError(name)
            if not datatype.validate(v):
                raise ValueError(name)
            yield name,v
    return dict(gen())

def result_validator(definition, data):
    if type(definition) is type and issubclass(definition, Data):
        return definition.validate(data)
    elif type(definition) == type(data):
        if type(definition) is list:
            assert len(definition) == 1
            sub_def = definition[0]
            return all(result_validator(sub_def, x) for x in data)
        elif type(definition) is dict:
            if set(definition) != set(data):
                return False
            return all(result_validator(sub_def, data[k]) \
                       for k,sub_def in definition.items())
        elif definition is None:
            return True
    return False



#set up export

request_name_format = re.compile(r"^[A-Z]+(_[A-Z]+)*$")
request_types = dict(
    (n, o()) for n,o in globals().copy().items()
    if type(o) is type and
    issubclass(o, Request) and
    request_name_format.match(n)
    )
