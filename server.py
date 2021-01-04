#! python3.7

# Copyright: Jason Forbes

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, BIGINT
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import *

import sys, os, os.path, json

app_root = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, app_root)

#local imports
import protocol
assert protocol.VERSION == "2r1"

# constants
def spool_file_path(id): return os.path.join("spools", str(id))
db_path = "test.db"
public_config_opts = ['max_block_and_data_op_size', 'max_spools',
    'max_spool_size', 'storage_quota_kb']
private_config_opts = ['owner_key']
config_opts = public_config_opts + private_config_opts



# Init modules
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+db_path
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)



# Config loader
conf_fn = "conf.json"
conf_obj = None
def Config():
    global conf_obj
    if conf_obj is None:
        with open(conf_fn) as f:
            conf_obj = json.load(f)
    return conf_obj



# Data Model

class Spool(db.Model):
    """Spool

    id: is also the filename on disk
    block_size: spool can only be read/written in increments of this many bytes
    block_count: size of the spool, in blocks
    head_offset: measured in blocks
    owner_annotation: client-define metadata (not used by server)
    """
    __tablename__ = "spools"
    id = Column(Integer(unsigned=True),
                Sequence('spool_id_seq'), primary_key=True)
    # starting to think representing spool dimentions in blocks in the database
    # wasn't the best choice, because there's too much room for programmer error
    # in conversion. I could convert everything to bytes except block_size in
    # the db, and still be able to enforce block alignment and implement the
    # API no problem
    block_size = Column(Integer(unsigned=True), nullable=False)
    block_count = Column(BIGINT(unsigned=True), nullable=False)
    head_offset = Column(BIGINT(unsigned=True), nullable=False)
    tail_offset = Column(BIGINT(unsigned=True), nullable=False)
    annotation = Column(String(32), nullable=False)
    accessors = relationship("SpoolAccessor", back_populates="spool")
    def __repr__(self):
        return f"<Spool {self.id}>"

class SpoolAccessor(db.Model):
    __tablename__ = "spool_accessors"
    k = Column(String(24), primary_key=True)
    spool_id = Column(Integer(unsigned=True), ForeignKey('spool.id'))
    can_read = Column(Boolean, nullable=False)
    can_append = Column(Boolean, nullable=False)
    can_truncate = Column(Boolean, nullable=False)
    spool = relationship("Spool", back_populates="accessors")
    def __repr__(self):
        return f"<SpoolAccessor ({repr(self.k)})>"



# Exceptions
class RequestException(Exception):
class InvalidRequest(RequestException): code = 400
class NoResult(RequestException): code = 404
class LimitReached(RequestException): code = 403
class Denied(RequestException): code = 403
class Conflict(RequestException): code = 409



# Utilities

def call(f): return f()

def dictify(obj, attr_names:str):
    '''Use dictify() to turn obj into a dict by
    cherry picking which attributes you want'''
    if type(attr_names) == str:
        attr_names = attr_names.split()
    return dict(((a,getattr(obj,a)) for a in attr_names))

def count_filesystem_allocation(use_cached):
    # note: this function is fs only and does not include include db usage
    this = count_filesystem_allocation
    if use_cached and hasattr(this, "cached"):
        return this.cached
    try:
        f_bsize = os.statvfs(os.getcwd()).f_bsize
    except:
        f_bsize = 4096 # if statvfs isn't available just guess a common value
    to_fs_size = lambda sz: ((sz-1) | (f_bsize-1)) + 1
    spool_sizes_q = db.session.query(Spool.block_size, Spool.block_count)
    r = this.cached = sum(to_fs_size(sz*cnt) for sz,cnt in spool_sizes_q.all())
    return r

def reqd_kb_blocks(*f_sizes): return sum((sz - 1) // 1024 + 1 for sz in sizes)

def storage_usage_kb(use_cached=False):
    if not os.path.exists(db_path):
        return 0
    usage = count_filesystem_allocation(use_cached) + os.path.getsize(db_path)
    return reqd_kb(quota_usage + spool_size)

def get_accessor(k):
    r = db.session.query(SpoolAccessor).get(k)
    if r is None:
        raise NoResult("Access key didn't find anything.")



# Request handlers

def SERV_INFO():
    return dict(((k, Config()[k]) for k in public_config_opts))

def SPOOL_LIST():
    return [x for (x,) in db.session.query(Spool.id).all()]

def SPOOL_NEW(block_size, block_count, annotation):
    if block_size > Config()['max_block_and_data_op_size']:
        raise Denied("block_size is too big.")
    spool_size = block_size * block_count
    if spool_size > Config()['max_spool_size']:
        raise Denied("Cannot create a spool that big.")
    if db.session.query(Spool).count() >= Config()['max_spools']:
        raise LimitReached("Maximum number of spools reached.")
    requested_storage = storage_usage_kb() + reqd_kb_blocks(spool_size + 64)
    if requested_storage > Config()['storage_quota_kb']
        raise LimitReached("Storage quota reached.")

    spool = Spool(block_size=block_size, block_count=block_count,
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
    os.remove(spool_file_path(id))

def ACCESS_LIST(spool_id):
    if db.session.query(Spool).filter_by(id=spool_id).count() == 0:
        raise NoResult("That spool doesn't exist.")
    q = db.session.query(SpoolAccessor.k).filter_by(spool_id=id)
    return [k for (k,) in q.all()]

def ACCESS_INFO(k):
    a = get_accessor(k)
    return [a.spool_id, a.can_read, a.can_append, a.can_truncate]

def ACCESS_INFO_UPDATE(k, info):
    db.session.query(SpoolAccessor).filter_by(k=k).delete()
    conflict = db.session.query(Spool).get(info['spool_id']) is None
    try:
        if conflict:
            raise Conflict("Attempted creating access for nonexistent spool.")
    else:
        a = SpoolAccessor(k=k, **info)
        db.session.add(a)
    finally:
        db.session.commit()

def ACCESS_REVOKE(k):
    count = db.session.query(SpoolAccessor).filter_by(k=k).delete()
    db.session.commit()
    if count == 0:
        raise NoResult("Spool not found.")

def METADATA_FETCH(k):
    s = get_accessor(k).spool
    return [s.block_size, s.block_count, s.head_offset, s.tail_offset,
        s.annotation]

def DATA_READ(k, start_offset, block_count):
    a = get_accessor(k)
    if not a.can_read:
        raise Denied("Access denied")
    constraints_valid = a.spool.tail_offset <= start_offset and \
                        (start_offset + block_count) <= a.spool.head_offset
    if not constraints_valid:
        raise Conflict("Attempting to read outside of valid range.")
    if block_count == 0:
        return ""
    read_size = block_count * a.spool.block_size
    if read_size > Config()['max_block_and_data_op_size']:
        raise Denied("Would read more than server allows in a single request.")

    # here, pass essentially refers to the number of times the file has been
    # overwritten. offset is the block offset in the file of course.
    st_pass, st_offset = divmod(start_offset, a.spool.block_count)
    ed_pass, ed_offset = divmod(start_offset + block_count - 1, a.spool.block_count)
    ed_offset += 1
    # byte positions in file:
    st_pos = st_offset * a.spool.block_size
    ed_pos = ed_offset * a.spool.block_size
    with open(spool_file_path(a.spool_id), 'rb') as f:
        if st_pass == end_pass:
            f.seek(st_pos)
            data = f.read(read_size)
            assert len(data) == read_size
        else:
            # the read constraints start and end on different passes; we'll
            # have to do two separate reads.
            data = bytearray(read_size)
            ct = f.readinto(memoryview(data)[read_size-ed_pos:])
            # asserts because I can't tell from the documentation whether it's
            # guaranteed to fill the buffer if we don't accidentally hit EOF
            # and it doesn't throw an exception.
            assert ct == ed_pos
            f.seek(st_pos)
            ct = f.readinto(memoryview(data)[:read_size-ed_pos])
            assert ct == read_size - ed_pos
    import base64
    return base64.b64encode(data).decode()

def DATA_APPEND(k, head_offset):
    a = get_accessor(k)
    if not a.can_write:
        raise Denied("Access denied")
    if head_offset != k.spool.head_offset:
        raise Conflict("Incorrect head offset.")
    stream = request.files['file'].stream

    # this operation is so ugly...
    def receive_file():
        max_op = min(a.spool.block_size * a.spool.block_count,
                    Config()['max_block_and_data_op_size'])
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
        if (recvd_bytes % a.spool.block_size) != 0:
            raise Denied("Data is not aligned to spool's block size.")

    # the file is usable now, and proper size is assured
    file_data = bytes(receive_file())
    path = spool_file_path(a.spool_id)
    if head_offset == 0: #file doesn't exist yet
        with open(path, 'xb') as f:
            f.write(file_data)
    else:
        with open(path, 'r+b') as f:
            spool_size_bytes = a.spool.block_size * a.spool.block_count
            head_bytepos = (head_offset % a.spool.block_count) * a.spool.block_size
            writelen_1 = min(spool_size_bytes - head_bytepos, len(file_data))
            f.seek(head_bytepos)
            ct = f.write(file_data[:writelen_1])
            assert ct == writelen_1
            ct = f.write(file_data[:writelen_1])
            writelen_2 = len(file_data) - writelen_1
            assert ct == writelen_2

    a.spool.head_offset += len(file_data)
    a.spool.tail_offset = max(a.spool.tail_offset,
        (a.spool.head_offset - a.spool.block_count))
    db.session.commit()

def DATA_TRUNCATE(k, tail_offset):
    a = get_accessor(k)
    if not a.can_truncate:
        raise Denied("Access denied")
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
                if cred['owner_key'] != Config()['owner_key']:
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



# Setup/Installation tool
def setup():
    print("running a lousy setup script")
    opts = {}
    for o in config_opts:
        opts[o] = input(o + "=")
    from json import dump
    with open(conf_fn,'w') as f:
        dump(opts,f)
    db.create_all()
    if not os.path.exists("files"):
        os.mkdir("files")
    print("application successfully set up... probably")



# Command line actions
if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: server.py <command>")
    else:
        cmd = sys.argv[1].upper()
        if cmd == "TEST":
            app.run(debug=True)
        elif cmd == "SETUP":
            setup()
        elif cmd == "CLEAN":
            if os.path.exists(save_path):
                import shutil
                shutil.rmtree(save_path)
            if os.path.exists(db_path):
                os.remove(db_path)
            os.mkdir("files")
            db.create_all()
            print("recreated all the thigns")
            #app.run(debug=True)
else:
    os.chdir(app_root)
    application = app
