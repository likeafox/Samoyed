bad_access_key = 'snth'
get_access_key = '7ldRkoIdQpWlEO9FW2p/fw=='
user_key = b'\xfe\xae,\xf9@7H\xbc\x8f7&p\xcbBm\xd0'
import json
import requests
from base64 import *
#import urllib.request
#import urllib.parse

req=lambda o,m='get': getattr(requests,m)("http://127.0.0.1:5000/", data=o)
def go(req_type,params={}):
    r = req({'access_key':get_access_key,'req_type':req_type,'params':json.dumps(params)})
    print(r.request.headers)
    print(r.request.body)
    print(r.text)


go("GET_USER",{'key_hash':b64encode(user_key).decode()})

class Session():
    def __init__(self, url):
        self.url = url
