#!python3.6
import sys
sys.path.append(r"F:\Dropbox\cloudfiles\source\samoyed")

def keygen(password):
        import base64, hashlib, time
        st_t = time.time()
        x = hashlib.pbkdf2_hmac('sha256', password.encode(), b'testsalt', 1000000)
        print("Generated key in {:.3f} seconds.".format(time.time() - st_t))
        return base64.b64encode(x[:16]).decode()

args = sys.argv[1:]
if args == []:
    from clientapi import *
    from time import sleep

    #load config
    import json
    with open("samoyed.json") as f:
        config = json.load(f)
    conf_fields = '''enc_key server access_key root_dir nickname'''.split()
    if set(conf_fields) != set(config.keys()):
        raise Exception("config file doesn't have the right fields defined")

    repo = Repo(config['server'], config['access_key'])
    user = repo.login(config['nickname'], config['enc_key'], config['root_dir'])
    while True:
        sleep(2)
        print("boop")
        #print(user.get_file_listing_changes().__repr__())
        user.handle_filesystem_changes()
        user.get_remote_file_listing_updates()
        user.handle_file_changes()
        user.auto_upload()
        user.sync('up','autodown')
elif args == ['keygen']:
    print("Enter new password: ",end='')
    k = keygen(input())
    print("Key: "+k)
