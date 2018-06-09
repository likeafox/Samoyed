from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, Sequence,\
                       ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import relationship

import json

# Init modules
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
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
    file_accessor = relationship("FileAccess", back_populates="file")
    def __repr__(self):
        return "<File {}>".format(self.id)  

class User(db.Model):
    __tablename__ = "users"
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    name = Column(String(24), nullable=False, unique=True)
    key_hash = Column(String(24), nullable=False, unique=True)
    file_accessor = relationship("FileAccess", back_populates="user")
    def __repr__(self):
        return "<User {}>".format(self.name)

class FileAccess(db.Model):
    __tablename__ = "file_accessors"
    file_id = Column(Integer, ForeignKey('files.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    enc_file_key = Column(String(24))
    hidden_fn = Column(String(24))
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

    def bool_validate(v): return type(v) is bool

    return {
        'key_hash': [b64_validate, User.key_hash],
        'name': [name_validate],
        'user_id': [int_validate],
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
    def handler(reqtype, auth=False):
        def d(f):
            #nonlocal r
            r[f.__name__] = (reqtype, f, auth)
        return d

    @handler('GET')
    def GET_REPO_INFO():
        pass

    @handler("GET")
    def GET_USER(key_hash):
        #todo check how many users there are first
        user = User.query.filter_by(key_hash=key_hash).one()
        return jsonize(user, "id name")

    @handler("GET")
    def GET_USER_NAMES():
        return list(db.session.query(User.name).all())

    @handler("POST")
    def CREATE_USER(name, key_hash):
        user = User(name=name, key_hash=key_hash)
        db.session.add(user)
        db.session.commit()
        return user.id

    @handler("GET", auth=True)
    def GET_FILE_LIST(user_id):
        ks=(FileAccess.file_id, FileAccess.enc_file_key,
            FileAccess.hidden_fn, FileAccess.can_modify)
        return db.session.query(*ks).filter_by(user_id=user_id, accepted=True).all()

    @handler("GET", auth=True)
    def GET_UNACCEPTED_FILE_LIST(user_id):
        return db.session.query(FileAccess.file_id).filter_by(
            user_id=user_id, accepted=True).all()

    return r

@app.route("/")
def handle_request():
    def error(msg, code=400): return json.dumps({'error':msg, 'response':"ERR"}), code
    global_permissions = { Config()['get_access_key']:('GET',),
                           Config()['post_access_key']:('GET','POST'),
                           }.get(request.form['access_key'],())
    if request.method not in global_permissions:
        return error("Unauthorized",403)
    try:
        reqtypemethod, handler, require_auth = req_handlers[request.form['req_type']]
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
        return error(e.msg)
    if require_auth:
        cred = { 'id':params['user_id'], 'key_hash':params['key_hash'] }
        if User.query.filter_by(**cred).count() == 0:
            return error("Unauthorized2",403)
    args = (params[p] for p in handler.__code__.co_varnames)
    return json.dumps({'result':handler(*args), 'response':"OK", 'error':""})



# Setup/Installation tool
opt_names='''max_users max_files max_file_size get_access_key
            post_access_key user_key_derivation_salt
            hidden_fn_salt'''.split()
def setup():
    print("running a lousy setup script")
    opts = {}
    for o in opt_names:
        opts[o] = input(o + "=")
    from json import dump
    with open(conf_fn,'w') as f:
        dump(opts,f)
    db.create_all()
    import os
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
