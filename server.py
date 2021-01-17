#! python3.7

# Copyright: Jason Forbes

USAGE_STRING = """server.py --- A Samoyed server

usage: server.py [run]

Before running, things require some setup. Run the python interactive
interpreter and import this server module then perform the following
steps:

1. Create the server's configuration:
>>> config.create_default_file()

This will create a config file with all server options and with most of them
set to a sane default. The defaults shouldn't be assumed to be acceptable
though. You should review all the options and in particular it's recommended
that owner_key be set to a passphrase generated with a high amount of entropy.
Any good password generator will do, and a procedure like this is also
acceptible: https://www.eff.org/dice

2. Initialize the database:
>>> db.create_all()

3. Ensure the spool file directory (defined in the config file) exists."""

#external imports
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, BIGINT, String, \
                       Boolean, ForeignKey, Sequence
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import *
import sys, os, os.path, json, functools

app = Flask(__name__)
db = SQLAlchemy(app)

#local imports
app_root = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, app_root)
import protocol
assert protocol.VERSION == "2r3"



# Utilities

def call(f): return f()

@call
def estimate_size_on_disk():
    @functools.lru_cache(None)
    def fs_block_size(dir_):
        try:
            bs = os.statvfs(dir_).f_bsize # *nix only
        except:
            bs = 4096 # if statvfs isn't available just guess a common value
        return bs
    def f(filesize):
        """Returns the amount of disk space in bytes that a file of `filesize`
        would likely take up on the current filesystem. Current filesystem
        is determined by os.getcwd the first time this function is called,
        but can be permanently overridden by setting the "use_dir" attribute
        on this function to a different path.
        """
        f.use_dir = getattr(f, "use_dir", None) or os.getcwd()
        if filesize == 0:
            return 0
        return (filesize - 1) | (fs_block_size(f.use_dir) - 1) + 1
    return f

def calc_storage_usage():
    file_sizes = db.session.query(Spool.size_bytes).all()
    spool_count = len(file_sizes)
    usage = sum(estimate_size_on_disk(sz) for (sz,) in file_sizes)
    if app.config['SQLALCHEMY_DATABASE_URI'].startswith("sqlite:"):
        usage += os.path.getsize(config()['db_location'])
    else:
        usage += spool_count * 64
    return usage



# Config loader

@call
class config():
    default_filename = "conf.json"
    public_opt_defaults = {
        'max_block_and_data_op_size': 1024**2,
        'max_spools': 9999,
        'max_spool_size': 2**32,
        'storage_quota': 5 * (1024**3)
        }
    private_opt_defaults = {
        # because the server validates the client's owner_key as a B64KeySized
        # in protocol v2, auth will always fail unless the server's owner_key
        # is set to a string in the config file
        'owner_key': None,
        'spool_file_dir': "spools",
        'db_location': "test.db",
        'db_uri_scheme': "sqlite:///"
        }
    opt_defaults = {}
    opt_defaults.update(public_opt_defaults)
    opt_defaults.update(private_opt_defaults)

    def __init__(self):
        self.conf_filename = self.default_filename
        self.obj = None
    
    def __call__(self):
        if self.obj is None:
            with open(self.conf_filename) as f:
                self.obj = json.load(f)
        return self.obj

    def create_default_file(self, filename=None):
        filename = filename or self.conf_filename
        with open(filename, 'x') as f:
            json.dump(self.opt_defaults, f, indent="  ")
        self.conf_filename = filename

    def get_public_opts(self):
        return dict((k, self()[k]) for k in self.public_opt_defaults)



# Data Model

class Spool(db.Model):
    """Spool

    id: is also the filename on disk
    block_size: spool can only be read/written in increments of this many bytes
    size_bytes: maximum size the spool file can grow to
    head_offset: measured in bytes
    owner_annotation: client-define metadata (not used by server)
    """
    __tablename__ = "spools"
    id = Column(Integer, Sequence('spool_id_seq'), primary_key=True)
    block_size = Column(Integer, nullable=False)
    size_bytes = Column(BIGINT, nullable=False)
    head_offset = Column(BIGINT, nullable=False)
    tail_offset = Column(BIGINT, nullable=False)
    annotation = Column(String(32), nullable=False)
    accessors = relationship("SpoolAccessor", back_populates="spool")
    def __repr__(self):
        return f"<Spool {self.id}>"
    def is_aligned(self, offset):
        return offset % self.block_size == 0
    def file_path(self):
        return os.path.join(config()['spool_file_dir'], str(self.id))

class SpoolAccessor(db.Model):
    __tablename__ = "spool_accessors"
    k = Column(String(24), primary_key=True)
    spool_id = Column(Integer, ForeignKey('spools.id'))
    can_read = Column(Boolean, nullable=False)
    can_append = Column(Boolean, nullable=False)
    can_truncate = Column(Boolean, nullable=False)
    spool = relationship("Spool", back_populates="accessors")
    def __repr__(self):
        return f"<SpoolAccessor ({repr(self.k)})>"
    @classmethod
    def fetch(cls, k):
        r = db.session.query(cls).get(k)
        if r is None:
            raise NoResult("Access key didn't find anything.")
        return r



# Exceptions
class RequestException(Exception): pass
class InvalidRequest(RequestException): code = 400
class NoResult(RequestException): code = 404
class LimitReached(RequestException): code = 403
class Denied(RequestException): code = 403
class Conflict(RequestException): code = 409



# Request handlers

def SERV_INFO():
    return config.get_public_opts()

def SPOOL_LIST():
    return [x for (x,) in db.session.query(Spool.id).all()]

def SPOOL_NEW(block_size, size_bytes, annotation):
    if size_bytes % block_size != 0:
        raise InvalidRequest("Spool size wouldn't align to its block size.")
    if block_size == 0 or block_size > config()['max_block_and_data_op_size']:
        raise Denied("The server refuses to use that block size.")
    if size_bytes > config()['max_spool_size']:
        raise Denied("The server refuses to create a spool that big.")
    if db.session.query(Spool).count() >= config()['max_spools']:
        raise LimitReached("Maximum number of spools reached.")
    if (calc_storage_usage() + size_bytes + 64) > config()['storage_quota']:
        raise LimitReached("Storage quota reached.")

    spool = Spool(block_size=block_size, size_bytes=size_bytes,
                head_offset=0, tail_offset=0, annotation=annotation)
    db.session.add(spool)
    db.session.commit()
    return spool.id

def SPOOL_DELETE(id):
    db.session.query(SpoolAccessor).filter_by(spool_id=id).delete()
    count = db.session.query(Spool).filter_by(id=id).delete()
    db.session.commit()
    if count == 0:
        raise NoResult("That spool doesn't exist to delete.")
    os.remove(Spool.file_dir(id))

def ACCESS_LIST(spool_id):
    if db.session.query(Spool).filter_by(id=spool_id).count() == 0:
        raise NoResult("That spool doesn't exist.")
    q = db.session.query(SpoolAccessor.k).filter_by(spool_id=spool_id)
    return [k for (k,) in q.all()]

def ACCESS_INFO(k):
    a = SpoolAccessor.fetch(k)
    return [a.spool_id, a.can_read, a.can_append, a.can_truncate]

def ACCESS_INFO_UPDATE(k, info):
    db.session.query(SpoolAccessor).filter_by(k=k).delete()
    if db.session.query(Spool).get(info['spool_id']) is None:
        db.session.commit()
        raise Conflict("Attempted creating access for nonexistent spool.")
    a = SpoolAccessor(k=k, **info)
    db.session.add(a)
    db.session.commit()

def ACCESS_REVOKE(k):
    if db.session.query(SpoolAccessor).filter_by(k=k).delete() != 0:
        db.session.commit()
    else:
        raise NoResult("Spool not found.")

def METADATA_FETCH(k):
    s = SpoolAccessor.fetch(k).spool
    return [s.block_size, s.size_bytes, s.head_offset, s.tail_offset,
        s.annotation]

def DATA_READ(k, start_offset, length):
    a = SpoolAccessor.fetch(k)
    if not a.can_read:
        raise Denied("Access denied")
    constraints_valid = a.spool.tail_offset <= start_offset and \
                        (start_offset + length) <= a.spool.head_offset
    if not constraints_valid:
        raise Conflict("Attempting to read outside of valid range.")
    if length == 0:
        return ""
    if length > config()['max_block_and_data_op_size']:
        raise Denied("Would read more than server allows in a single request.")
    if not (a.spool.is_aligned(start_offset) and a.spool.is_aligned(length)):
        raise Conflict("Constraints are not aligned to block size.")

    # here, pass essentially refers to the number of times the file has been
    # overwritten. pos is the byte position in the file of course.
    st_pass, st_pos = divmod(start_offset, a.spool.size_bytes)
    ed_pass, ed_pos = divmod(start_offset + length - 1, a.spool.size_bytes)
    ed_pos += 1
    with open(spool_file_path(a.spool_id), 'rb') as f:
        if st_pass == end_pass:
            f.seek(st_pos)
            data = f.read(length)
            assert len(data) == length
        else:
            # the read constraints start and end on different passes; we'll
            # have to do two separate reads.
            data = bytearray(length)
            ct = f.readinto(memoryview(data)[length-ed_pos:])
            # asserts because I can't tell from the documentation whether it's
            # guaranteed to fill the buffer if we don't accidentally hit EOF
            # and it doesn't throw an exception.
            assert ct == ed_pos
            f.seek(st_pos)
            ct = f.readinto(memoryview(data)[:length-ed_pos])
            assert ct == length - ed_pos
    import base64
    return base64.b64encode(data).decode()

def DATA_APPEND(k, head_offset):
    a = SpoolAccessor.fetch(k)
    if not a.can_write:
        raise Denied("Access denied")
    if head_offset != a.spool.head_offset:
        raise Conflict("Incorrect head offset.")
    stream = request.files['file'].stream

    # this operation is so ugly...
    def receive_file():
        max_op = min(a.spool.size_bytes, config()['max_block_and_data_op_size'])
        recvd_bytes = 0
        while recvd_bytes <= max_op:
            data = stream.read(max_op - recvd_bytes + 1)
            if data is None:
                # no more data is ready yet, so wait for upload to continue
                import time
                time.sleep(0.2)
            else:
                ct = len(data)
                if ct == 0:
                    #success! upload is finished
                    break
                else:
                    #got data:
                    recvd_bytes += ct
                    yield from data
        else:
            raise Denied("Client tried to write more data than is allowed by "\
                "the server in a single request.")
        if not a.spool.is_aligned(recvd_bytes):
            raise Denied("Data is not aligned to spool's block size.")

    file_data = bytes(receive_file())
    # (the file is usable now, and proper size is assured)
    if head_offset == 0: #file doesn't exist yet
        with open(a.spool.file_path(), 'xb') as f:
            f.write(file_data)
    else:
        with open(a.spool.file_path(), 'r+b') as f:
            head_pos = head_offset % a.spool.size_bytes
            writelen_1 = min(a.spool.size_bytes - head_pos, len(file_data))
            f.seek(head_pos)
            ct = f.write(file_data[:writelen_1])
            assert ct == writelen_1
            writelen_2 = len(file_data) - writelen_1
            if writelen_2:
                f.seek(0)
                ct = f.write(file_data[writelen_1:])
                assert ct == writelen_2

    a.spool.head_offset += len(file_data)
    a.spool.tail_offset = max(a.spool.tail_offset,
        (a.spool.head_offset - a.spool.size_bytes))
    db.session.commit()

def DATA_TRUNCATE(k, tail_offset):
    a = SpoolAccessor.fetch(k)
    if not a.can_truncate:
        raise Denied("Access denied")
    if not a.spool.is_aligned(tail_offset):
        raise Conflict("Not aligned to block size.")
    if not a.spool.tail_offset <= tail_offset <= a.spool.head_offset:
        raise Conflict("Tail offset is out of range.")
    a.spool.tail_offset = tail_offset
    db.session.commit()

# set handlers
for n,o in globals().copy().items():
    try:
        reqtype = protocol.request_types[n]
    except KeyError:
        continue
    reqtype.handler = o
    assert set(reqtype.params) == \
           set(o.__code__.co_varnames[:o.__code__.co_argcount])



@app.route("/", methods=['GET', 'POST'])
def handle_request():
    try:
        #set up request type
        if set(request.form) != {'req_type','params'}:
            raise InvalidRequest("Malformed request.")
        try:
            reqtype = protocol.request_types[request.form['req_type']]
        except KeyError:
            raise InvalidRequest("Unknown request.")
        if request.method != reqtype.method:
            raise InvalidRequest("Incompatable request method.")

        #decode parameters
        try:
            raw_params = json.loads(request.form["params"])
            if type(raw_params) is not dict:
                raise TypeError()
        except (json.JSONDecodeError, TypeError):
            raise InvalidRequest("Malformed request.")

        #do owner auth
        if reqtype.owner_only:
            try:
                cred = protocol.extract_args(reqtype.owner_params, raw_params)
                if cred['owner_key'] != config()['owner_key']:
                    raise ValueError()
            except (TypeError, ValueError):
                raise Denied("Invalid credentials")

        #set up & validate parameters
        try:
            args = protocol.extract_args(reqtype.params, raw_params)
        except ValueError as e:
            raise InvalidRequest("Request is missing parameter: " + e.args[0])
        except TypeError as e:
            raise InvalidRequest("Invalid parameter: " + e.args[0])

        #handle
        result = reqtype.handler(**args)
        if not protocol.result_validator(reqtype.result, result):
            raise TypeError("Would return invalid result:\n"+str(result))
        return json.dumps({'result':result, 'response':"OK"})

    except Exception as e:
        ro = {'response':"ERR",
              'error': e.args.__repr__(),
              'error_class': e.__class__.__name__}
        if not isinstance(e, RequestException):
            import traceback
            traceback.print_exc()
        return json.dumps(ro), getattr(e, 'code', 500)



# Go!

def init():
    app.config['SQLALCHEMY_DATABASE_URI'] = \
    config()['db_uri_scheme'] + config()['db_location']
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == "run":
        init()
        app.run(debug=True)
    else:
        print(USAGE_STRING)
else:
    #os.chdir(app_root)
    #???
    init()
    application = app
