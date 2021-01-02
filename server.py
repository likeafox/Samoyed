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
spool_dir = "spools"
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
    block_size = Column(Integer(unsigned=True), nullable=False)
    block_count = Column(BIGINT(unsigned=True), nullable=False)
    head_offset = Column(BIGINT(unsigned=True), nullable=False)
    tail_offset = Column(BIGINT(unsigned=True), nullable=False)
    annotation = Column(String(24), nullable=False)
    spool_accessors = relationship("SpoolAccess", back_populates="spool")
    def __repr__(self):
        return f"<Spool {self.id}>"

class SpoolAccess(db.Model):
    __tablename__ = "spool_accessors"
    k = Column(String(24), primary_key=True)
    spool_id = Column(Integer(unsigned=True), ForeignKey('spool.id'))
    can_read = Column(Boolean, nullable=False)
    can_append = Column(Boolean, nullable=False)
    can_truncate = Column(Boolean, nullable=False)
    spool = relationship("Spool", back_populates="spool_accessors")
    def __repr__(self):
        return f"<SpoolAccess ({self.id})>"



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
        f_bsize = os.statvfs(app_root).f_bsize
    except:
        f_bsize = 4096 # if statvfs isn't available just guess a common value
    to_fs_size = lambda sz: (sz-1 | f_bsize-1) + 1
    all_spool_sizes_q = db.session.query(Spool.block_size, Spool.block_count)
    r = this.cached = sum(to_fs_size(sz*cnt) for sz,cnt in all_spool_sizes_q.all())
    return r

def storage_usage_kb(use_cached=False):
    if not os.path.exists(db_path):
        return 0
    usage = count_filesystem_allocation(use_cached) + os.path.getsize(db_path)
    return (quota_usage + spool_size - 1) // 1024 + 1

#def spool_query(**kwargs):
#    dbks = [ getattr(FileAccess, n) for n in protocol.FileEntry.fields ]
#    return db.session.query(*dbks).filter_by(**kwargs)



# Request handlers

def SERV_INFO():
    return dict(((k, Config()[k]) for k in public_config_opts))

def SPOOL_LIST():
    return db.session.query(Spool.id).all()

def SPOOL_NEW(id, block_size, block_count, annotation):
    if block_size > Config()['max_block_and_data_op_size']:
        raise Denied("block_size is too big.")
    spool_size = block_size * block_count
    if spool_size > Config()['max_spool_size']:
        raise Denied("Cannot create a spool that big.")
    if Spool.query.filter_by(id=id).count() != 0:
        raise Conflict("A spool with that id already exists.")
    if storage_usage_kb() > Config()['storage_quota_kb']
        raise Denied("Storage quota reached.")

    raise NotImplemented()



def GET_FILE_ENTRY(user_id, file_id):
    return file_entry_query(
        user_id=user_id, file_id=file_id, accepted=True).one()

def GET_UNACCEPTED_FILE_LIST(user_id):
    q = db.session.query(FileAccess.file_id).filter_by(
            user_id=user_id, accepted=False)
    return [x for (x,) in q.all()]

def GET_FILE_META(user_id, file_id):
    fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
    return dictify(fa.file, "nonce enc_meta")

def GET_FILE_DATA(user_id, file_id, start_end):
    start, end = start_end
    if start is None: start = 0
    fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
    fn = fa.file.uuid_name
    import os.path
    with open(os.path.join("files",fn), 'rb') as fh:
        fh.seek(start)
        if end is None:
            d = fh.read()
        else:
            d = fh.read(end-start)
    import base64
    return base64.b64encode(d).decode()

def GET_FILE_USERS(user_id, file_id):
    q=db.session.query(User.name).filter(User.id==FileAccess.user_id)\
                                 .filter(FileAccess.file_id==file_id)
    r = [x for (x,) in q.all()]
    return r

def NEW_FILE_ACCESS(user_id, file_id, target_username, can_modify):
    fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
    target_user_id = User.query.filter_by(name=target_username).value(User.id)
    if fa.can_modify == False and can_modify == True:
        raise Exception("you can't do that")
    new_fa = FileAccess(file_id=file_id, user_id=target_user_id,
                        can_modify=can_modify, accepted=False)
    db.session.add(new_fa)
    db.session.commit()

def MODIFY_FILE_ACCESS_KEYS(user_id, file_id, enc_file_key, hidden_fn):
    fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
    fa.enc_file_key = enc_file_key
    fa.hidden_fn = hidden_fn
    fa.accepted = True
    db.session.commit()

def NEW_FILE(user_id, nonce, enc_file_meta, enc_file_key, hidden_fn):
    if File.query.count() >= Config()['max_files']:
        raise Exception("too many files!~")
    uuid_fn = uuid.uuid4().hex
    f_path = os.path.join(save_path,uuid_fn)
    request.files['file'].save(f_path)
    try:
        file_size = os.path.getsize(f_path)
        if file_size > Config()['max_file_size']:
            raise Exception("file's to big man")
        kb_size = ((file_size - 1) // 1024) + 1
        from sqlalchemy.sql import func
        x = db.session.query(func.sum(File.kb_size)).all()[0][0]
        db_kb_size = x or 0
        print(db_kb_size)
        #db.session.query(func.sum(File.kb_size)).all()
        if db_kb_size+kb_size > Config()['max_kb']:
            raise Exception("Repo is full")
        rev_seq = Seq.query.filter_by(id=REVISION_SEQ_ID).one()
        rev_seq.v += 1
        file = File(uuid_name=uuid_fn, nonce=nonce,
                    enc_meta=enc_file_meta, kb_size=kb_size, rev=rev_seq.v)
        db.session.add(file)
        db.session.flush()
        fa = FileAccess(user_id=user_id, file_id=file.id,
                        enc_file_key=enc_file_key, hidden_fn=hidden_fn,
                        can_modify=True, accepted=True)
        db.session.add(fa)
        db.session.commit()
    except:
        os.remove(f_path)
        raise
    return file.id

def UPDATE_FILE(user_id, file_id, nonce, enc_file_meta):
    fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
    if not fa.can_modify or not fa.accepted:
        raise Exception("You can't modify that file")
    file = File.query.filter_by(id=file_id).one()

    new_uuid_name = uuid.uuid4().hex
    new_path = os.path.join(save_path, new_uuid_name)
    request.files['file'].save(new_path)
    file_size = os.path.getsize(new_path)
    if file_size > Config()['max_file_size']:
        raise Exception("Files't toooo big")
    kb_size = ((file_size - 1) // 1024) + 1
    from sqlalchemy.sql import func
    db_kb_size = db.session.query(func.sum(File.kb_size)).scalar()
    if db_kb_size+(kb_size-file.kb_size) > Config()['max_kb']:
        raise Exception("Repo is full")
    file.nonce = nonce
    file.enc_meta = enc_file_meta
    file.kb_size = kb_size
    old_path = os.path.join(save_path, file.uuid_name)
    file.uuid_name = new_uuid_name
    rev_seq = Seq.query.filter_by(id=REVISION_SEQ_ID).one()
    file.rev = rev_seq.v = rev_seq.v+1
    os.remove(old_path)
    db.session.commit()

def UNLINK_FILE(user_id, file_id):
    fa_q = FileAccess.query.filter_by(file_id=file_id,user_id=user_id)
    fa = fa_q.one()
    file = fa.file
    if len(file.file_accessors) == 1:
        os.remove(os.path.join(save_path,file.uuid_name))
        File.query.filter_by(id=file_id).delete()
    fa_q.delete()
    db.session.commit()

def GET_REVISION():
    return Seq.query.filter_by(id=REVISION_SEQ_ID).value(Seq.v)

def GET_NEW_REVISIONS(user_id, client_rev):
    rev = Seq.query.filter_by(id=REVISION_SEQ_ID).value(Seq.v)
    if client_rev == rev:
        rl = []
    else:
        q = db.session.query(File.id, File.rev).filter(
            File.rev > client_rev, File.id==FileAccess.file_id,
            FileAccess.user_id==user_id, FileAccess.accepted==True)
        rl = [ list(x) for x in q.all() ]
    return {'server_rev':rev, 'rev_list':rl}

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
        if set(request.form) != {'access_key','req_type','params'}:
            raise InvalidRequest("Malformed request.")
        global_permissions = { Config()['get_access_key']:('GET',),
                               Config()['post_access_key']:('GET','POST'),
                               }.get(request.form['access_key'],())
        if request.method not in global_permissions:
            raise Denied("Unauthorized.")
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

        #do auth
        if reqtype.auth:
            try:
                cred = protocol.extract_args(reqtype.auth_params, raw_params)
                User.query.filter_by(id=cred['user_id'],
                                     key_hash=cred['key_hash']).one()
            except (TypeError, ValueError, NoResultFound):
                raise Denied("Invalid credentials.")

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
