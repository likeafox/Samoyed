bad_access_key = 'snth'
get_access_key = '7ldRkoIdQpWlEO9FW2p/fw=='
user_key = b'\xfe\xae,\xf9@7H\xbc\x8f7&p\xcbBm\xd0'
post_access_key = "g/BqyJs5QiOzd1lJBtkPCg=="
import json
import requests
from base64 import *
#import urllib.request
#import urllib.parse
from time import sleep

req=lambda o,m='get': getattr(requests,m)("http://127.0.0.1:5000/", data=o)
def go(req_type,params={},m='get'):
    r = req({'access_key':post_access_key,'req_type':req_type,'params':json.dumps(params)},m)
    #print(r.request.headers)
    #print(r.request.body)
    print(r.text)
    sleep(0.2)
    return(r.text)
print("")


key_hash = b64encode(user_key).decode()

r = go("GET_USER",{'key_hash':key_hash})
r = json.loads(r)["result"]
r = json.loads(r)
id_ = r["id"]
#go("ERASE")
#go("CREATE_USER",{'name':"boof",'key_hash':key_hash},"post")
go("GET_USER_NAMES")
go("GET_FILE_LIST", {'user_id':id_,'key_hash':key_hash})
go("lynxy_go")
