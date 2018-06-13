from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, Sequence,\
                       ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import relationship

import json, uuid, os, os.path, tempfile

# constants
save_path = "files"
db_path = "test.db"
public_config_opts = '''max_users max_files max_file_size max_kb
    user_key_derivation_salt hidden_fn_salt title
    suggested_root_name'''.split()
private_config_opts = '''get_access_key post_access_key'''.split()
config_opts = public_config_opts + private_config_opts



# Init modules
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+db_path
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)



# Utilities
def call(f): return f()
def jsonize(obj, attr_names:str):
    '''Use jsonize() to turn obj into a json object by
    cherry picking which attributes you want'''
    if type(attr_names) == str:
        attr_names = attr_names.split()
    d = dict(((a,getattr(obj,a)) for a in attr_names))
    return json.dumps(d)



# Config loader
conf_fn = "conf.json"
conf_obj = None
def Config():
    global conf_obj
    if conf_obj is None:
        with open(conf_fn) as f:
            conf_obj = json.load(f)
    return conf_obj



# Data Models
class File(db.Model):
    __tablename__ = "files"
    id = Column(Integer, Sequence('file_id_seq'), primary_key=True)
    uuid_name = Column(String(32), nullable=False, unique=True)
    nonce = Column(String(24), nullable=False)
    enc_meta = Column(String(64), nullable=False)
    kb_size = Column(Integer, nullable=False)
    file_accessors = relationship("FileAccess", back_populates="file")
    def __repr__(self):
        return "<File {}>".format(self.id)

class User(db.Model):
    __tablename__ = "users"
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    name = Column(String(24), nullable=False, unique=True)
    key_hash = Column(String(24), nullable=False, unique=True)
    file_accessors = relationship("FileAccess", back_populates="user")
    def __repr__(self):
        return "<User {}>".format(self.name)

class FileAccess(db.Model):
    __tablename__ = "file_accessors"
    file_id = Column(Integer, ForeignKey('files.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    enc_file_key = Column(String(24))
    hidden_fn = Column(String(24), unique=True)
    can_modify = Column(Boolean, nullable=False)
    accepted = Column(Boolean, nullable=False)
    __table_args__ = ( PrimaryKeyConstraint('file_id','user_id'),{},)
    file = relationship("File", back_populates="file_accessors")
    user = relationship("User", back_populates="file_accessors")
    def __repr__(self):
        return "<FileAccess ({}, {})>".format(self.file_id, self.user_id)



# Request data validation
class InvalidRequestError(Exception): pass

@call
def req_validators():
    import base64, re
    def b64_validate(v, col):
        try:
            base64.b64decode(v, validate=True)
            #check length ( https://stackoverflow.com/a/1778993 )
            assert(len(v) == col.property.columns[0].type.length)
        except:
            return False
        return True

    def int_validate(v):
        return type(v) is int and v in range(0x7fffffff)

    name_re = re.compile(r'^\w{3,24}$')
    def name_validate(v):
        return type(v) is str and name_re.match(v) is not None

    def type_validate(v, t): return type(v) is t

    def start_end_validate(v):
        if type(v) is not list or len(v) != 2:
            return False
        has_None=False
        for w in v:
            if w is None: has_None=True
            elif type(w) is not int or w < 0: return False
        return has_None or v[0] <= v[1]

    return {
        'key_hash': [b64_validate, User.key_hash],
        'name': [name_validate],
        'user_id': [int_validate],
        'file_id': [int_validate],
        'start_end': [start_end_validate],
        'target_username': [name_validate],
        'can_modify': [type_validate, bool],
        'enc_file_key': [b64_validate, FileAccess.enc_file_key],
        'hidden_fn': [b64_validate, FileAccess.hidden_fn],
    }

def validate_request_params(obj):
    for k,v in obj.items():
        try:
            [f, *xargs] = req_validators[k]
        except KeyError:
            raise InvalidRequestError("Parameter does not exist.")
        if not f(v,*xargs):
            raise InvalidRequestError("Invalid value for "+k+".")



# Request handling
@call
def req_handlers():
    r = {}
    def handler(reqtype, auth=True):
        def d(f):
            #nonlocal r
            args = f.__code__.co_varnames[:f.__code__.co_argcount]
            r[f.__name__] = (reqtype, f, auth, args)
        return d

    @handler('GET', auth=False)
    def ERASE_EVERYTHING():
        '''for testing purposes only'''
        print("nope")
        print('https://stackoverflow.com/questions/4763472/sqlalchemy-clear-database-content-but-dont-drop-the-schema')
        return 0

    @handler("GET", auth=False)
    def GET_REPO_INFO():
        return dict(((k, Config()[k]) for k in public_config_opts))

    @handler("GET", auth=False)
    def GET_USER(key_hash):
        #todo check how many users there are first
        user = User.query.filter_by(key_hash=key_hash).one()
        return jsonize(user, "id name")

    @handler("GET", auth=False)
    def GET_USER_NAMES():
        return list(db.session.query(User.name).all())

    @handler("POST", auth=False)
    def NEW_USER(name, key_hash):
        #http://docs.sqlalchemy.org/en/latest/errors.html#integrityerror
        user = User(name=name, key_hash=key_hash)
        db.session.add(user)
        db.session.commit()
        return user.id

    @handler("GET")
    def GET_FILE_LIST(user_id):
        ks=(FileAccess.file_id, FileAccess.enc_file_key,
            FileAccess.hidden_fn, FileAccess.can_modify)
        return db.session.query(*ks).filter_by(user_id=user_id, accepted=True).all()

    @handler("GET")
    def GET_UNACCEPTED_FILE_LIST(user_id):
        return db.session.query(FileAccess.file_id).filter_by(
            user_id=user_id, accepted=False).all()

    @handler("GET")
    def GET_FILE_META(user_id, file_id):
        fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
        return jsonize(fa.file, "nonce enc_meta")

    @handler("GET")
    def GET_FILE_DATA(user_id, file_id, start_end):
        start, end = start_end
        if start is None: start = 0
        fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
        fn = fa.file.uuid_fn
        import os.path
        with open(os.path.join("files",fn), 'rb') as fh:
            fh.seek(start)
            if end is None:
                d = fh.read()
            else:
                d = fh.read(end-start)
        import base64
        return base64.b64encode(d)

    @handler("GET")
    def GET_FILE_USERS(user_id, file_id):
        q=db.session.query(User.name).filter(User.id==FileAccess.user_id)\
                                     .filter(FileAccess.file_id==file_id)
        r = list(q.all())
        return r

    @handler("POST")
    def NEW_FILE_ACCESS(user_id, file_id, target_username, can_modify):
        fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
        target_user_id = User.id.query(name=target_username).one()
        if fa.can_modify == False and can_modify == True:
            raise Exception("you can't do that")
        new_fa = FileAccess(file_id=file_id, user_id=target_user_id,
                            can_modify=can_modify, accepted=False)
        db.session.add(new_fa)
        db.session.commit()

    @handler("POST")
    def MODIFY_FILE_ACCESS_KEYS(user_id, file_id, enc_file_key, hidden_fn):
        fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
        fa.enc_file_key = enc_file_key
        fa.hidden_fn = hidden_fn
        fa.accepted = True
        db.session.commit()

    @handler("POST")
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
            db_kb_size = db.session.query(func.sum(File.kb_size)).all()
            if db_kb_size+kb_size > Config()['max_kb']:
                raise Exception("Repo is full")
            file = File(uuid_name=uuid_fn, nonce=nonce,
                        enc_meta=enc_file_meta, kb_size=kb_size)
            db.session.add(file)
            db.session.commit()
            fa = FileAccess(user_id=user_id, file_id=file.id,
                            enc_file_key=enc_file_key, hidden_fn=hidden_fn,
                            can_modify=True, accepted=True)
            db.session.add(fa)
            db.session.commit()
        except:
            os.remove(f_path)
            raise

    @handler("POST")
    def UPDATE_FILE(user_id, file_id, nonce, enc_file_meta):
        fa = FileAccess.query.filter_by(user_id=user_id, file_id=file_id).one()
        if not fa.can_modify or not fa.accepted:
            raise Exception("You can't modify that file")
        file = File.query.filter_by(id=file_id).one()
        with tempfile.TemporaryDirectory() as tempdir:
            temp_path = os.path.join(tempdir, file.uuid_name)
            request.files['file'].save(temp_path)
            file_size = os.path.getsize(temp_path)
            if file_size > Config()['max_file_size']:
                raise Exception("Files't toooo big")
            kb_size = ((file_size - 1) // 1024) + 1
            from sqlalchemy.sql import func
            db_kb_size = db.session.query(func.sum(File.kb_size)).all()
            if db_kb_size+(kb_size-file.kb_size) > Config()['max_kb']:
                raise Exception("Repo is full")
            file.nonce = nonce
            file.enc_meta = enc_file_meta
            file.kb_size = kb_size
            old_path = os.path.join(save_path, file.uuid_name)
            file.uuid_name = uuid.uuid4().hex
            new_path = os.path.join(save_path, file.uuid_name)
            os.rename(temp_path, new_path)
            os.remove(old_path)
            db.session.commit()

    @handler("POST")
    def UNLINK_FILE(user_id, file_id):
        q = FileAccess.query.filter_by(file_id=file_id)
        fa = q.filter_by(user_id=user_id).one()
        if q.count() == 1:
            file = File.query(file_id=file_id).one()
            os.remove(os.path.join("files",file.uuid_fn))
            file.query.delete()
        fa.query.delete()
        db.session.commit()

    @handler("GET", auth=False)
    def HELP():
        return list(req_handlers.keys())

    return r

@app.route("/", methods=['GET', 'POST'])
def handle_request():
    def error(msg, code=400): return json.dumps({'error':msg, 'response':"ERR"}), code
    global_permissions = { Config()['get_access_key']:('GET',),
                           Config()['post_access_key']:('GET','POST'),
                           }.get(request.form['access_key'],())
    if request.method not in global_permissions:
        return error("Unauthorized",403)
    try:
        reqtypemethod, handler, require_auth, arg_names = req_handlers[request.form['req_type']]
    except KeyError:
        return error("Unknown Request Type")
    if request.method != reqtypemethod:
        return error("Wrong Request Method (GET/POST?)")
    try:
        params = json.loads(request.form["params"])
        validate_request_params(params)
    except KeyError:
        return error("No Params")
    except json.JSONDecodeError:
        return error("JSON not work")
    except InvalidRequestError as e:
        return error(e.args.__repr__())
    if require_auth:
        cred = { 'id':params['user_id'], 'key_hash':params['key_hash'] }
        if User.query.filter_by(**cred).count() == 0:
            return error("Unauthorized2",403)
    if not set(arg_names) <= params.keys():
        return error("missing parameters")
    args = (params[p] for p in arg_names)
    try:
        return json.dumps({'result':handler(*args), 'response':"OK", 'error':""})
    except Exception as e:
        print(type(e), e)
        return error(e.args.__repr__())



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
        print("Usage: api.py <command>")
    else:
        cmd = sys.argv[1].upper()
        if cmd == "TEST":
            app.run(debug=True)
        elif cmd == "SETUP":
            setup()
