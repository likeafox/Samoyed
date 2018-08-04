#imports: standard library
from base64 import *
from pathlib import Path, PurePath
from contextlib import contextmanager
from types import SimpleNamespace
import zlib, uuid, time, itertools, hashlib,\
       os, os.path, struct, json

#imports: modules
from requests import request

#imports: "local"
from aes import AES, AESModeOfOperationCTR

#global constants
CONSTS = SimpleNamespace(
    allowable_max_norm_fn_len = range(12,150),
    allowable_max_file_size = range(32*1024*1024),
    file_data_ctr_offset = 1024*1024,
    min_header_length = 16 )





#CRYPTO

class SomeRandomness:
    '''SomeRandomness is based in small part on Fortuna CSPRNG
    with a notable assumption that all entropy added is of equal
    quality, and only uses one pool. This isn't seen as
    a big deal because all the prng-ing of this class is all
    ultimately for the purpose of supplementing the underlying
    OS's crypto, which is available via python 3.6's built-in
    secrets module.  If the secrets module is not available,
    attempt to fall back to PyCryptodome, which also seems good.'''
    #for future reference:
    #https://www.schneier.com/academic/paperfiles/fortuna.pdf

    def __init__(self):
        try:
            from secrets import token_bytes as f
        except ImportError:
            try:
                from Crypto.Random import get_random_bytes as f
            except ImportError as e:
                msg = "Neither 'secrets' module nor 'Crypto.Random' module "+\
                      "were found"
                raise e.__class__(msg)
        self.system_random_bytes = f

        #try to not have a dry pool at the start
        self.pool = hashlib.sha256(uuid.uuid1(node=0).bytes)
        self.pool.update(self.system_random_bytes(16))

    def timeseeder(self, f):
        def d(*args,**kwargs):
            st = time.perf_counter()
            r = f(*args, **kwargs)
            timedata = struct.pack("dd", time.perf_counter(), st)
            self.pool.update(timedata)
            return r
        return d

    def reap(self, length):
        aes = AESModeOfOperationCTR(self.pool.digest())
        random1 = aes.encrypt(bytes(length))
        random2 = self.system_random_bytes(length)
        self.pool = hashlib.sha256(aes.encrypt(bytes(32)))
        return bytes(a^b for a,b in zip(random1, random2))

some_randomness = SomeRandomness()

class AESCTROTP:
    def __init__(self, aes, nonce):
        self.aes = aes if isinstance(aes, AES) else AES(aes)
        self.nonce = int.from_bytes(nonce, byteorder='little')
        self.slice_ = slice(0,None)

    def _new_slice(self, slice_):
        if slice_.step not in (None, 1):
            raise ValueError("Do not specify a slice step")

        #cons means constraint
        def new_cons(prev_cons, cons, clamp):
            if cons is None:
                n = prev_cons
            elif type(cons) is int:
                if cons >= 0:
                    n = self.slice_.start + cons
                elif self.slice_.stop is not None:
                    n = self.slice_.stop + cons
                else:
                    raise ValueError("The "+self.__class__.__name__+" is endless!")
                if prev_cons is not None:
                    n = clamp(n, prev_cons)
            else:
                raise TypeError("Slice with integers only.")
            return n

        r = self.__class__(self.aes, self.nonce.to_bytes(16, byteorder='little'))
        r.slice_ = slice(new_cons(self.slice_.start, slice_.start, max),
                         new_cons(self.slice_.stop, slice_.stop, min))
        return r

    def __getitem__(self, k):
        if isinstance(k, slice):
            return self._new_slice(k)
        elif type(k) is int:
            #a very inefficient and untested line of code:
            #return next(iter(self[k:k+1 if k != -1 else None]))
            raise NotImplemented()
        raise TypeError(self.__class__.__name__+" indices must be integers")

    def __iter__(self):
        def get_block(i):
            return self.aes.encrypt((self.nonce ^ i).to_bytes(16, byteorder='little'))

        start_block = self.slice_.start >> 4
        start_offset = self.slice_.start & 0xF
        if self.slice_.stop is None:
            full_block_indices = itertools.count(start_block)
        else:
            last_block = self.slice_.stop >> 4
            end_offset = self.slice_.stop & 0xF
            if start_block == last_block:
                yield from get_block(start_block)[start_offset:end_offset]
                return
            full_block_indices = iter(range(start_block, last_block))

        yield from get_block(next(full_block_indices))[start_offset:]
        for block_i in full_block_indices:
            yield from get_block(block_i)
        yield from get_block(last_block)[:end_offset]

    def length(self):
        if self.slice_.stop is None:
            return float("inf")
        else:
            return max((self.slice_.stop - self.slice_.start), 0)

    def crypt(self, data:bytes):
        if not len(data) <= self.length():
            raise ValueError("data is too long for this OTP")
        return bytes(a^b for a,b in zip(data, self))



#Other utility

def sequence_diff(*sequences, safe=True):
    if type(safe) is not bool:
        raise TypeError("safe must be a bool")
    def safe_iter(seq):
        it = iter(seq)
        prev_x = next(it)
        yield prev_x
        for x in it:
            if x <= prev_x:
                raise ValueError(seq.__repr__() + " is not pre-sorted.")
            yield x
            prev_x = x

    end = object()
    iters = [(iter,safe_iter)[int(safe)](s) for s in sequences]
    indices = range(len(iters))
    values = [next(it, end) for it in iters]
    while True:
        try:
            cur = min(x for x in values if x is not end)
        except ValueError:
            return
        r = tuple(x == cur for x in values)
        for i, it, advance in zip(indices, iters, r):
            if advance:
                values[i] = next(it, end)
        if not all(r):
            yield (cur, r)

@contextmanager
def cd(path):
    "Context manager to temporarily change working directory"
    prev = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(prev)





#INTERFACE

class ClientInterfaceException(Exception): pass
class RepoIntegrityError(ClientInterfaceException): pass
class RequestError(ClientInterfaceException):
    def __init__(self, msg, c=None):
        assert type(msg) is str
        self.msg = msg
        self.c = c
class FileConflict(ClientInterfaceException): pass

class Repo:
    def __init__(self, url, access_key):
        self.url = url
        self.access_key = access_key
        self.server_req_types = dict(self.req("HELP"))
        self.server_config = self.req("GET_REPO_INFO")

    @some_randomness.timeseeder
    def req(self, req_type, params={}, **kwargs):
        print(req_type)
        method = "get" if req_type == "HELP" else self.server_req_types[req_type]
        p = {'access_key':self.access_key,
             'req_type':req_type,
             'params':json.dumps(params)}
        r = request(method, self.url, data=p, **kwargs) #r: response
        try:
            ro = json.loads(r.text) #ro: response object
            assert type(ro) is dict
            response = ro['response']
            if response == "OK":
                return ro['result']
            elif response == "ERR":
                raise RequestError("Server responded with error: " +
                                   ro['error'], ro['error_class'])
            else:
                raise RequestError("Unknown request response type.")
        except RequestError:
            raise
        except:
            raise RequestError("The server sent back a malformed response.")

    def login(self, nickname, user_key_b64, root_dir, allow_new=True):
        user_key = b64decode(user_key_b64)
        key_hash = b64encode(hashlib.sha256(user_key).digest()[:16]).decode()
        try:
            r = self.req("GET_USER", {'key_hash':key_hash})
            user_id = r['id']
            assert nickname == r['name']
        except RequestError as e:
            if e.c == "NoResult":
                user_id = self.req("NEW_USER", {'name':nickname, 'key_hash':key_hash})
            else: raise
        return    User(self, user_id, nickname, user_key, key_hash, root_dir)

class User:
    def __init__(self, repo, user_id, nickname, user_key, key_hash, root_dir):
        self.repo = repo
        self.user_id = user_id
        self.nickname = nickname
        self.user_key = user_key
        self.aes = AES(user_key)
        self.key_hash = key_hash
        self.root_dir = os.path.realpath(root_dir)
        if not os.path.isdir(self.root_dir):
            raise NotADirectoryError(root_dir.__repr__() +" is not a directory.")
        self.rev = repo.req("GET_REVISION")
        self.local_file_listing = list(self.local_file_listing_gen())
        self.local_files_by_norm = {}
        self.local_files_by_hidden = {}
        for path in (os.path.join(*dirs,fn) for dirs,fn in self.local_file_listing):
            try:
                lf = LocalFile(self, path)
                lf.refresh_meta()
                self.local_files_by_norm[lf.norm_fn] = lf
                self.local_files_by_hidden[lf.hidden_fn] = lf
            except RepoIntegrityError as e:
                print(e)
        rfs = (RemoteFile.load(self, *x) for x in self.req("GET_FILE_LIST"))
        self.remote_files = dict((rf.file_id, rf) for rf in rfs)

    def req(self, req_type, params={}, **kwargs):
        p = {'user_id':self.user_id, 'key_hash':self.key_hash}
        p.update(params)
        return self.repo.req(req_type, p, **kwargs)

    def normalize_filename(self, path:str):
        with cd(self.root_dir):
            # Use realpath so we don't get fooled by symlinks or ../ trickery
            realpath = os.path.realpath(path)
            # Is it in our root dir?
            if os.path.commonpath((self.root_dir, realpath)) != self.root_dir:
                raise RepoIntegrityError("Path is not in the repo root directory")
            # Make it into a relative path so it can be translated between systems
            relpath = os.path.relpath(realpath)
        # Just say no to certain characters
        #https://en.wikipedia.org/wiki/Filename#Reserved_characters_and_words
        if set('~?%*:|"<>') & set(relpath):
            raise RepoIntegrityError("Path contains invalid characters.")
        # Return it in a common form
        return PurePath(relpath).as_posix()

    def make_hidden_filename(self, normal_filename):
        salt = b64decode(self.repo.server_config['hidden_fn_salt'])
        hash_ = hashlib.sha256(normal_filename.encode() + self.user_key + salt).digest()
        return b64encode(hash_[:16]).decode()

    def local_file_listing_gen(self):
        '''full file listing in the user's root dir,
        but split into like ((dirparts), fn) so it can be sorted'''
        for dir_, dirs, fns in os.walk(self.root_dir):
            dirs.sort()
            dir_parts = Path(dir_).parts
            yield from ((dir_parts, fn) for fn in sorted(fns))

    def get_file_listing_changes(self):
        new = list(self.local_file_listing_gen())
        diff = sequence_diff(self.local_file_listing, new)
        self.local_file_listing = new
        cats = [['deletions',[]],['creations',[]]]#categories of listing changes
        for (dir_parts, fn), (_, state) in diff:
            cats[int(state)][1].append(os.path.join(*dir_parts,fn))
        return dict(cats)

    def handle_filesystem_changes(self, remotedelete=True):
        changes = self.get_file_listing_changes()
        for path in changes['deletions']:
            norm_fn = self.normalize_filename(path)
            try:
                lf = self.local_files_by_norm[norm_fn]
            except KeyError:
                continue
            if lf.remote is not None:
                if remotedelete:
                    lf.remote.unlink()
                else:
                    lf.remote.local = None
            del self.local_files_by_norm[norm_fn]
            del self.local_files_by_hidden[lf.hidden_fn]
        for path in changes['creations']:
            try:
                lf = LocalFile(self, path)
            except RepoIntegrityError:
                continue
            lf.refresh_meta()
            self.local_files_by_norm[lf.norm_fn] = lf
            self.local_files_by_hidden[lf.hidden_fn] = lf
            for rf in self.remote_files.values():
                if lf.hidden_fn == rf.hidden_fn:
                    lf.remote = rf
                    rf.local = lf
                    break
        return changes

    def get_remote_file_listing_updates(self):
        r = self.req("GET_NEW_REVISIONS",{'client_rev':self.rev})
        self.rev = r['server_rev']
        rev_list = r['rev_list']
        for file_id,rev in rev_list:
            if file_id in self.remote_files:
                self.remote_files[file_id].refresh_meta()
            else:
                r = self.req("GET_FILE_ENTRY", {'file_id':file_id})
                rf = RemoteFile.load(self, *r)
                self.remote_files[rf.file_id] = rf

    def auto_upload(self):
        for lf in self.local_files_by_norm.values():
            if lf.remote is None:
                lf.new_upload()

    def handle_file_changes(self):
        for rf in self.remote_files.values():
            if rf.local is not None:
                old_mtime = rf.local.mtime
                new_mtime = rf.local.refresh_mtime()
                if old_mtime != new_mtime:
                    rf.local.refresh_meta()

    def sync(self, *args):
        for rf in self.remote_files.values():
            rf.sync(*args)

class File:
    pass

class LocalFile(File):
    def __init__(self, user, filename):
        self.user = user
        self.norm_fn = user.normalize_filename(filename)
        self.hidden_fn = user.make_hidden_filename(self.norm_fn)
        self.full_fn = os.path.join(user.root_dir,self.norm_fn)
        self.ver_hashes = []
        self.remote = None

    def refresh_mtime(self):
        self.mtime = int(os.path.getmtime(self.full_fn))
        return self.mtime

    def _md5_hash(self):
        with open(self.full_fn,'rb') as f:
            return b64encode(hashlib.md5(f.read()).digest()).decode()

    def refresh_meta(self):
        self.refresh_mtime()
        self.file_size = os.path.getsize(self.full_fn)
        md5 = self._md5_hash()
        if [md5] != self.ver_hashes[-1:]:
            self.ver_hashes.append(md5)

    def new_upload(self):
        if self.remote:
            raise RepoIntegrityError("That file already exists on the server")

        #prepare upload pieces
        self.refresh_meta()
        with open(self.full_fn,'rb') as f:
            file_data = f.read()
        header = dict(
            versions = [[self.ver_hashes[-1], self.user.nickname]],
            cdate = int(os.path.getctime(self.full_fn)),
            namespace = "default",
            path = self.norm_fn )
        packed_header = zlib.compress(json.dumps(header).encode())
        header_hash = hashlib.md5(packed_header).digest()
        upload_pieces = [header_hash, packed_header, file_data]

        #create new security
        key = some_randomness.reap(16)
        nonce = uuid.uuid4().bytes
        aes = AESCTROTP(key, nonce)

        #prepare meta
        meta = SimpleNamespace(
            mtime = self.mtime,
            file_length = self.file_size,
            header_length = len(header_hash) + len(packed_header),
            header_hash = header_hash,
            aes = aes )
        meta_block = bytearray(48)
        meta_block[16:22] = meta.mtime.to_bytes(6, 'big')
        meta_block[24:30] = meta.file_length.to_bytes(6, 'big')
        meta_block[30:32] = meta.header_length.to_bytes(2, 'big')
        meta_block[32:48] = meta.header_hash
        meta_block[0:16] = hashlib.md5(meta_block[16:]).digest()

        #actual outputs
        enc_file_meta = b64encode(bytes(a^b for a,b in zip(aes, meta_block))).decode()
        enc_b64_file_key = b64encode(bytes(self.user.aes.encrypt(key))).decode()
        enc_upload = aes[CONSTS.file_data_ctr_offset:].crypt(b''.join(upload_pieces))

        #make request
        file_id = self.user.req("NEW_FILE", dict(
            nonce = b64encode(nonce).decode(),
            enc_file_meta = enc_file_meta,
            enc_file_key = enc_b64_file_key,
            hidden_fn = self.hidden_fn
            ), files={'file':enc_upload})

        #RemoteFile
        r = RemoteFile(self.user, file_id, enc_b64_file_key, self.hidden_fn, True)
        r.file_key = key
        r.local = self
        r.meta = meta
        self.remote = r
        self.user.remote_files[file_id] = r
        return r

    def _save(self, data, mtime):
        with cd(self.user.root_dir):
            with open(self.norm_fn, 'wb') as f:
                f.write(data)
            os.utime(self.norm_fn, (time.time(), mtime))
            self.refresh_meta()

class RemoteFile(File):
    def __init__(self, user, file_id, enc_b64_file_key, hidden_fn, can_modify):
        self.user = user
        self.file_id = file_id
        self.enc_b64_file_key = enc_b64_file_key
        self.hidden_fn = hidden_fn
        self.can_modify = can_modify
        self.meta = None
        self.header = None
        self.local = None

    @classmethod
    def load(cls, user, file_id, enc_b64_file_key, hidden_fn, can_modify):
        r = cls(user, file_id, enc_b64_file_key, hidden_fn, can_modify)
        r.file_key = bytes(user.aes.decrypt(b64decode(enc_b64_file_key)))
        r.try_join_local()
        r.refresh_meta()
        return r

    def try_join_local(self):
        try:
            lf = self.user.local_files_by_hidden[self.hidden_fn]
            self.local = lf
            lf.remote = self
            return True
        except KeyError:
            self.local = None
            return False

    def req(self, req_type, params={}, **kwargs):
        p = {'file_id': self.file_id}
        p.update(params)
        return self.user.req(req_type, p, **kwargs)

    def refresh_meta(self):
        response = self.req("GET_FILE_META")
        aes = AESCTROTP(self.file_key, b64decode(response['nonce']))
        meta_block = aes.crypt(b64decode(response['enc_meta']))
        hash_, meta_chunk = meta_block[:16], meta_block[16:]
        del meta_block
        if hash_ != hashlib.md5(meta_chunk).digest():
            raise ValueError("Metadata integrity check failed")
        self.meta = SimpleNamespace(
            mtime = int.from_bytes(meta_chunk[:6],'big'),
            file_length = int.from_bytes(meta_chunk[8:14],'big'),
            header_length = int.from_bytes(meta_chunk[14:16],'big'),
            header_hash = meta_chunk[16:32],
            aes = aes )

    def unlink(self):
        self.req("UNLINK_FILE")
        if self.local:
            self.local.remote = None
            self.local = None
        del self.user.remote_files[self.file_id]

    def _download(self):
        if self.meta is None:
            self.refresh_meta()
        download = self.req("GET_FILE_DATA", {'start_end':[None,None]})
        data = self.meta.aes[CONSTS.file_data_ctr_offset:].crypt(b64decode(download))
        header_hash = data[0:16]
        packed_header = data[16:self.meta.header_length]
        file_data = data[self.meta.header_length:
                         self.meta.header_length+self.meta.file_length]
        if len(file_data) != self.meta.file_length:
            raise RepoIntegrityError("Incorrectly reported file length")
        if hashlib.md5(packed_header).digest() != header_hash:
            raise RepoIntegrityError("File header integrity check failed")
        header = json.loads(zlib.decompress(packed_header).decode())
        norm_fn = header['path']
        if norm_fn != self.user.normalize_filename(norm_fn):
            raise RepoIntegrityError("path in file header is not normalized")
        file_data_hash = header['versions'][-1][0]
        if file_data_hash != b64encode(hashlib.md5(file_data).digest()).decode():
            raise RepoIntegrityError("bad file hash")
        return (header, file_data)

    def _new_download(self):
        assert self.local is None
        header, file_data = self._download()
        norm_fn = header['path']

        with cd(self.user.root_dir):
            if not os.path.exists(norm_fn):
                #create file
                lf = LocalFile(self.user, norm_fn)
                lf._save(file_data, self.meta.mtime)
                lf.refresh_meta()
                self.user.local_files_by_norm[lf.norm_fn] = lf
                self.user.local_files_by_hidden[lf.hidden_fn] = lf
                lf.remote = self
                self.local = lf
            else:
                #error
                hidden_fn = self.user.make_hidden_filename(norm_fn)
                if hidden_fn in self.user.local_files_by_hidden:
                    raise ClientInterfaceException("client is in an invalid sta"\
                        "te: local and remote files exist but not associated")
                else:
                    for other_lf in self.user.local_files_by_hidden.values():
                        if os.path.samefile(norm_fn, other_lf.norm_fn):
                            raise FileConflict(norm_fn+" "+other_lf.norm_fn)
                raise FileConflict("Unknown file conflict with "+norm_fn)

    def sync(self, *args):
        if not args:
            args = {'up','down'}
        else:
            args = set(args)

        if self.local is None:
            if 'autodown' in args:
                self._new_download()
        elif 'up' in args and self.meta.mtime < self.local.refresh_mtime():
            self.local.refresh_meta()
            #upload it!
            header = self._download()[0]
            if header['versions'][-1][0] not in self.local.ver_hashes:
                raise FileConflict("Cant upload new version because it might"\
                                   "not be descendant of the server's version")
            #prepary upload body
            header['versions'].append([self.local.ver_hashes[-1], self.user.nickname])
            packed_header = zlib.compress(json.dumps(header).encode())
            header_hash = hashlib.md5(packed_header).digest()
            with open(self.local.full_fn,'rb') as f:
                file_data = f.read()
            upload_pieces = [header_hash, packed_header, file_data]
            nonce = uuid.uuid4().bytes
            aes = AESCTROTP(self.file_key, nonce)
            enc_upload = aes[CONSTS.file_data_ctr_offset:].crypt(b''.join(upload_pieces))
            #meta
            self.meta = SimpleNamespace(
                mtime = self.local.mtime,
                file_length = self.local.file_size,
                header_length = len(header_hash) + len(packed_header),
                header_hash = header_hash,
                aes = aes )
            enc_meta = b64encode(aes.crypt(self.pack_meta_block(self.meta))).decode()
            #send
            self.req("UPDATE_FILE", {'nonce':b64encode(nonce).decode(),
                                     'enc_file_meta':enc_meta},
                                     files={'file':enc_upload})
        elif {'down','autodown'} & args and self.meta.mtime > self.local.refresh_mtime():
            # it should be dowloaded
            header, file_data = self._download()
            if self.local.norm_fn != header['path']:
                raise RepoIntegrityError("sync: paths don't match")
            if self.local._md5_hash() not in dict(header['versions']):
                raise FileConflict("The newer version might not be descendant"\
                                   " from the local file's version")
            self.local._save(file_data, self.meta.mtime)

    @staticmethod
    def pack_meta_block(meta):
        meta_block = bytearray(48)
        meta_block[16:22] = meta.mtime.to_bytes(6, 'big')
        meta_block[24:30] = meta.file_length.to_bytes(6, 'big')
        meta_block[30:32] = meta.header_length.to_bytes(2, 'big')
        meta_block[32:48] = meta.header_hash
        meta_block[0:16] = hashlib.md5(meta_block[16:]).digest()
        return meta_block
