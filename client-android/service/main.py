import json, jnius

service = jnius.autoclass('org.kivy.android.PythonService').mService

shared_pref = service.getSharedPreferences("0.1.0",0)

def save_shared(v):
    editor = shared_pref.edit()
    editor.putString('comm', json.dumps(v))
    editor.commit()
COMM_PORT_BASE = 20517
if shared_pref.contains('comm'):
    COMM_PORT, COMM_CODE = json.loads(shared_pref.getString('comm',None))
else:
    COMM_PORT = COMM_PORT_BASE
    import Crypto.Random
    COMM_CODE = Crypto.Random.get_random_bytes(16).hex()
    save_shared([COMM_PORT, COMM_CODE])

import socket, sys, os, os.path
from time import sleep

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for i in range(5):
    try:
        sock.bind(('127.0.0.1', COMM_PORT))
    except:
        sleep(1)
        COMM_PORT = (((COMM_PORT - COMM_PORT_BASE)+1) % 10) + COMM_PORT_BASE
        save_shared([COMM_PORT, COMM_CODE])
    else:
        break
else:
    raise Exception("Exhausted UDP service ports to try")
sock.setblocking(False)

Environment = jnius.autoclass('android.os.Environment')
x_store_state = Environment.getExternalStorageState()
print("External storage: "+ x_store_state.__repr__())
app_root_dir = service.getExternalFilesDir(None).toString()
repo_root_dir = os.path.realpath(os.path.join(app_root_dir, "repo"))
if not os.path.exists(repo_root_dir):
    print("Repo dir does not exist. creating "+repo_root_dir)
    os.mkdir(repo_root_dir)
from clientapi import *
print("Loaded clientapi.py")
settings_json = shared_pref.getString('settings','{}')
settings = json.loads(settings_json)
print("Loaded settings")

def wait_send_exc(e):
    import traceback
    e_info = traceback.format_exc()
    info = (e.__repr__() + "\n" + e_info)[:70]
    for i in range(21):
        if i>0:
            sleep(1)
        try:
            _, addr = sock.recvfrom(128)
        except BlockingIOError:
            pass
        else:
            response = {'code':COMM_CODE, 'value':['info',info]}
            send_data = json.dumps(response).encode()
            sock.sendto(send_data, addr)
            return

try:
    repo = Repo(settings['server_url'], settings['access_key'])
    user = repo.login(settings['nickname'], settings['user_key'], repo_root_dir)
except Exception as e:
    wait_send_exc(e)
    raise

print("created repo objects.  entering main loop")
last_proc = 0
proc_interval = 3

while True:
    #get network requests
    while True:
        try:
            packed_data, addr = sock.recvfrom(128)
        except BlockingIOError:
            break
        try:
            data = json.loads(packed_data.decode())
            if data['code'] != COMM_CODE or \
               data['value'] is None:
                continue
            msg = data['value']
        except Exception as e:
            print(e)
            continue
        #handle incoming data
        if msg == 'ping':
            response = {'code':COMM_CODE, 'value':['pong',0]}
            send_data = json.dumps(response).encode()
            sock.sendto(send_data, addr)
        elif msg == 'exit':
            sys.exit(0) #this is apparently considered a crash by android

    t = time.time()
    if t > last_proc + proc_interval:
        last_proc = t
        try:
            user.handle_filesystem_changes()
            user.get_remote_file_listing_updates()
            user.handle_file_changes()
            user.auto_upload()
            user.sync('up','autodown')
        except Exception as e:
            wait_send_exc(e)
            raise

    sleep(0.5)
