VERSION = "1r1"

import base64, re



#data types

class Data: pass

class Uint(Data):
    @staticmethod
    def validate(v):
        return type(v) is int and v in range(0x7fffffff)

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

class FileMetaBlock(B64):
    encoded_length = 64

class Word(Data):
    minmax_lengths = (None, None)
    @classmethod
    def validate(cls, v):
        exp = r'^\w{{{},{}}}$'.format(*cls.minmax_lengths)
        return type(v) is str and re.match(exp, v) is not None

class Nickname(Word):
    minmax_lengths = (3, 24)

class VersionString(Word):
    minmax_lengths = (1, 10)

class SeqRange(Data):
    @staticmethod
    def validate(v):
        if type(v) is not list or len(v) != 2:
            return False
        has_None=False
        for w in v:
            if w is None: has_None=True
            elif type(w) is not int or w < 0: return False
        return has_None or v[0] <= v[1]



#requests

#default request
class Request:
    method = "GET"
    params = {}
    result = None
    file = False
    auth = False
    @staticmethod
    def handler():
        raise NotImplemented()

class SERV_PROTOCOL_VERSION(Request):
    result = VersionString
    @staticmethod
    def handler():
        return VERSION

class GET_USER(Request):
    params = { 'key_hash': B64KeySized }
    result = { 'id': Uint, 'name': Nickname }

class GET_USER_NAMES(Request):
    result = [ Nickname ]

class NEW_USER(Request):
    method = "POST"
    params = { 'name': Nickname, 'key_hash': B64KeySized }
    result = Uint

class GET_FILE_ENTRY(Request):
    auth = True
    params = { 'user_id': Uint, 'file_id': Uint }
    result = dict(
        file_id = Uint,
        enc_file_key = B64KeySized,
        hidden_fn = B64KeySized,
        can_modify = Bool
        )

class GET_FILE_LIST(Request):
    auth = True
    params = { 'user_id': Uint }
    result = [ GET_FILE_ENTRY.result ]

class GET_UNACCEPTED_FILE_LIST(Request):
    auth = True
    params = { 'user_id': Uint }
    result = [ Uint ]

class GET_FILE_META(Request):
    auth = True
    params = { 'user_id': Uint, 'file_id': Uint }
    result = FileMetaBlock

class GET_FILE_DATA(Request):
    auth = True
    params = { 'user_id': Uint, 'file_id': Uint, 'start_end': SeqRange }
    result = B64

class GET_FILE_USERS(Request):
    auth = True
    params = { 'user_id': Uint, 'file_id': Uint }
    result = [ Nickname ]

class NEW_FILE_ACCESS(Request):
    method = "POST"
    auth = True
    params = dict(
        user_id = Uint,
        file_id = Uint,
        target_username = Nickname,
        can_modify = Bool
        )

class MODIFY_FILE_ACCESS_KEYS(Request):
    method = "POST"
    auth = True
    params = dict(
        user_id = Uint,
        file_id = Uint,
        enc_file_key = B64KeySized,
        hidden_fn = B64KeySized
        )

class NEW_FILE(Request):
    method = "POST"
    auth = True
    file = True
    params = dict(
        user_id = Uint,
        nonce = B64KeySized,
        enc_file_meta = FileMetaBlock,
        enc_file_key = B64KeySized,
        hidden_fn = B64KeySized
        )
    result = Uint

class UPDATE_FILE(Request):
    method = "POST"
    auth = True
    file = True
    params = dict(
        user_id = Uint,
        file_id = Uint,
        nonce = B64KeySized,
        enc_file_meta = FileMetaBlock,
        )
    result = Uint

class UNLINK_FILE(Request):
    method = "POST"
    auth = True
    params = { 'user_id': Uint, 'file_id': Uint }

class GET_REVISION(Request):
    result = Uint

class GET_NEW_REVISIONS(Request):
    auth = True
    params = { 'user_id': Uint, 'client_rev': Uint }
    result = { 'server_rev': Uint, 'rev_list': [ Uint ] }



#set up export

request_name_format = re.compile(r"^[A-Z]+(_[A-Z]+)*$")
request_types = dict(
    (n, o()) for n,o in globals().copy().items()
    if type(o) is type and
    issubclass(o, Request) and
    request_name_format.match(n)
    )
