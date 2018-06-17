import requests
from base64 import *
import json
from time import sleep
import uuid
from types import SimpleNamespace

def akey():
    k = uuid.uuid4().bytes
    k64 = b64encode(k).decode()
    return SimpleNamespace(key=k, key64=k64)

bad_access_key = 'snth'
get_access_key = "7ldRkoIdQpWlEO9FW2p/fw=="
post_access_key = "g/BqyJs5QiOzd1lJBtkPCg=="
#user_key = b'\xfe\xae,\xf9@7H\xbc\x8f7&p\xcbBm\xd0'
#key_hash = b64encode(user_key).decode()

req= lambda o,m='get', **kwargs: getattr(requests,m)("http://127.0.0.1:5000/", data=o, **kwargs)
def go(req_type,params={},m='get',**kwargs):
    r = req({'access_key':post_access_key,'req_type':req_type,'params':json.dumps(params)},m, **kwargs)
    #print(r.request.headers)
    #print(r.request.body)
    #print(r.text)
    sleep(0.2)
    return json.loads(r.text)
def gop(req_type,params={},m='get',**kwargs):
    r = req({'access_key':post_access_key,'req_type':req_type,'params':json.dumps(params)},m, **kwargs)
    print(req_type)
    o = json.loads(r.text)
    sleep(0.2)
    if o['response'] == "OK":
        print(o['result'], type(o['result']))
    else:
        print(o['error'])
    return o
print("")


#
my_users={'jason':[akey()],
          'eli':[akey()],
          'BlUsHiE':[akey()],}
fns = ['pic1.png','pic2.jpg','pic3.jpg']
fids = []

def uid(n): return my_users[n][1]
def kh(n): return my_users[n][0].key64

gop("GET_REPO_INFO")
for k,v in my_users.items():
    r = gop("NEW_USER", {'name':k, 'key_hash':v[0].key64, },'post')
    v.append(r['result'])
gop("GET_USER_NAMES")
print('jason' == gop("GET_USER",{'key_hash':my_users['jason'][0].key64})['result']['name'])
gop("GET_FILE_LIST",{'user_id':uid('jason'), 'key_hash':kh('jason')})
stupid_meta = b64encode( b''.join(uuid.uuid4().bytes for _ in range(3)) ).decode()
for fn, user in zip(fns,('jason','jason','eli')):
    with open(fn,'rb') as f:
        r = gop("NEW_FILE",{'user_id':uid(user), 'key_hash':kh(user),
                    'nonce':akey().key64, 'enc_file_meta':stupid_meta,
                    'enc_file_key':akey().key64, 'hidden_fn':akey().key64},'post',
                    files={'file':f})
    fids.append(r['result'])
flist = gop("GET_FILE_LIST",{'user_id':uid('jason'), 'key_hash':kh('jason')})
gop("GET_FILE_META",{'user_id':uid('jason'), 'key_hash':kh('jason'),
                     'file_id':fids[0]})
gop("NEW_FILE_ACCESS",{'user_id':uid('jason'), 'key_hash':kh('jason'),
                       'file_id': fids[0],
                       'target_username':'eli', 'can_modify':False},'post')
gop("GET_FILE_USERS",{'user_id':uid('jason'), 'key_hash':kh('jason'),
                       'file_id': fids[0]})
gop("GET_UNACCEPTED_FILE_LIST",{'user_id':uid('eli'), 'key_hash':kh('eli')})
gop("GET_FILE_DATA", {'user_id':uid('jason'), 'key_hash':kh('jason'),
                       'file_id': fids[0], 'start_end': [None,2000]})
gop("GET_FILE_DATA", {'user_id':uid('jason'), 'key_hash':kh('jason'),
                       'file_id': fids[1], 'start_end': [2000,2012]})
gop("MODIFY_FILE_ACCESS_KEYS", {'user_id':uid('eli'), 'key_hash':kh('eli'),
                       'file_id': fids[0],
                        'enc_file_key':akey().key64,'hidden_fn':akey().key64},'post')
gop("GET_FILE_LIST",{'user_id':uid('eli'), 'key_hash':kh('eli')})
gop("GET_FILE_META",{'user_id':uid('eli'), 'key_hash':kh('eli'),
                     'file_id':fids[0]})
gop("UNLINK_FILE", {'user_id':uid('jason'), 'key_hash':kh('jason'),
                       'file_id': fids[1]}, 'post')
gop("GET_FILE_LIST",{'user_id':uid('jason'), 'key_hash':kh('jason')})
gop("HELP")

#r = go("GET_USER",{'key_hash':key_hash})
#r = json.loads(r)["result"]
#r = json.loads(r)
#id_ = r["id"]
#go("ERASE")
#go("CREATE_USER",{'name':"boof",'key_hash':key_hash},"post")
#go("GET_USER_NAMES")
#go("GET_FILE_LIST", {'user_id':id_,'key_hash':key_hash})
