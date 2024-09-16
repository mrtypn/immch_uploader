#import warnings
#warnings.filterwarnings("ignore", module="urllib3")
#This version works on immich version 115 , i might not work with previous version since the api has changed
import queue
import threading
import time

from os import walk
import requests #pip3 install requests
import os,sys
from datetime import datetime
import yaml  #pip3 install pyyaml
from sys import exit
import argparse
import hashlib
import base64

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

API_KEY=""
BASE_URL=""
CONFIG_FILE = "immic.config"
TEST_RUN=False
RECURSIVE=False
CONFIG_DATA={}

def upload(file):
    stats = os.stat(file)

    headers = {
        'Accept': 'application/json',
        'x-api-key': API_KEY
    }

    data = {
        'deviceAssetId': f'{file}-{stats.st_mtime}',
        'deviceId': 'python',
        'fileCreatedAt': datetime.fromtimestamp(stats.st_mtime),
        'fileModifiedAt': datetime.fromtimestamp(stats.st_mtime),
        'isFavorite': 'false',
    }

    files = {
        'assetData': open(file, 'rb')
    }
    try:
        #response = requests.post(    f'{BASE_URL}/asset/upload', headers=headers, data=data, files=files)

        session = requests.Session()

        request = requests.Request('POST',  f'{BASE_URL}/assets', data=data,headers=headers,files=files)
        prepped = request.prepare()
        del prepped.headers['Content-Length']

        response = session.send(prepped, verify=False)

        print(file,response.json())
    except Exception as e:
        print("Failed to upload %s " % file)
        print(e)
    
def get_all_photos_data():

    headers = {
        'Accept': 'application/json',
        'x-api-key': API_KEY
    }
    print("Pulling all assets to avoid re-upload")
    page=1
    photo_list=[]
    while(True):
        try:
            data = {
                'page': page,
                "withExif": "true",
                'size':1000
            }
            response = requests.post(
                f'{BASE_URL}/search/metadata', headers=headers,data=data)
            search_result=response.json()
            if "error" in search_result:
                raise Exception(search_result["message"])
           #print(search_result["assets"])
            photo_list=photo_list + search_result["assets"]["items"]
            #print(photo_list)
            #exit(0)
            page=search_result["assets"]["nextPage"]
            #print(page)
            #break
            if (page==None or page==0):
                break
        except Exception as e:
            print("There is a communication error with your endpoint: %s"% BASE_URL)   
            print("The error is: ")
            print(e)
            print("If you change your endpoint or token - you can reconfig by running: %s config"%os.path.basename(sys.argv[0]))
            
            exit(1)

    #print(photo_list)
    #exit(0)
    photo_data={}
    #we use dictionary as we want to easily check if a file exists based on filename-filezie
    for item in photo_list:
        try:
            file_name_without_ext=os.path.splitext(item["originalFileName"])[0]        
            photo_data["%s-%s"%(file_name_without_ext.lower(),item["exifInfo"]["fileSizeInByte"])] =item    
        except Exception as e:        
            continue
            print(e)
            print(item)
	        #exit(1)
    #print(photo_data)
    #exit(0)
    print("Total photos found: %s"%(len(photo_data)))
    return photo_data 

def ping_server():

    headers = {
        'Accept': 'application/json',
        'x-api-key': API_KEY
    }
    try:
        response = requests.get(
        f'{BASE_URL}/server-info/ping', headers=headers)
    except Exception as e:
        print("There is a communication error with your endpoint: %s"% BASE_URL)   
        print("The error is: ")
        print(e)
        print("If you change your endpoint or token - you can reconfig by running: %s config"%os.path.basename(sys.argv[0]))
        exit(1)

def get_all_album_data():
    headers = {
        'Accept': 'application/json',
        'x-api-key': API_KEY
    }
    album_data={}
    response = requests.get(
        f'{BASE_URL}/albums', headers=headers)
    album_list=response.json()
    for item in album_list:
        album_data[item["albumName"].lower()]=item #item["albumName"]    
    return album_data      

def create_album(album_name):
    headers = {
        'Accept': 'application/json',
        'x-api-key': API_KEY
    }
    data = {
        'albumName': album_name,
        'description':'Created by Python Script'
    }
    response = requests.post(
        f'{BASE_URL}/albums', headers=headers, data=data)
    return response.json()

def delete_all_album():
    headers = {
        'Accept': 'application/json',
        'x-api-key': API_KEY
    }

    album_list=get_all_album_data()
    for album_name,album in album_list.items():
        if not "Python" in album["description"]:
            continue
        print("Deleting album: %s with id %s"% (album["albumName"],album["id"]))
        response = requests.delete(
        f'{BASE_URL}/albums/%s'%album["id"], headers=headers)
        

    
def sync_folders_album(dir_path):

    print("Syncing folders with album name by mapping photos in folder to the same album name, if album does not exist, it will create")
    headers = {
        'Accept': 'application/json',
        'x-api-key': API_KEY
    }

    photo_list=get_all_photos_data()
    album_list=get_all_album_data()
    files_list=get_directory_files(dir_path)
    album_photos_mapping={}
    for file in files_list:
        stats = os.stat(file)
        file_name = os.path.basename(file ).lower() 
        head, dir_name = os.path.split(os.path.dirname(file ) )
        #file_name=os.path.splitext(file_name)[0]        
        photo_key=("%s-%s") %(file_name,stats.st_size)
        if not dir_name.lower() in album_list:
            print("Creating album %s" % dir_name)
            result=create_album(dir_name)
            album_id=result["id"]
            album_list[dir_name.lower()]=result
        else:
            album_id=album_list[dir_name.lower()]["id"]
        if not album_id in album_photos_mapping:
            album_photos_mapping[album_id]=[]

        if photo_key in photo_list:
            album_photos_mapping[album_id].append(photo_list[photo_key]["id"])    
        else: #old api use filename without ext
            photo_key=("%s-%s") %(os.path.splitext(file_name)[0].lower()   ,stats.st_size)
            if photo_key in photo_list:
                album_photos_mapping[album_id].append(photo_list[photo_key]["id"])  
    for album_id,photo_ids in album_photos_mapping.items():
        photo_ids_trunk= split_list(photo_ids,100) #we need to split into 100 items as if there are too many photos ids, we'll got error - too many parameters
        for photo_ids in photo_ids_trunk:
            data = {
            'ids': photo_ids
            }
            if len(photo_ids)>0 and photo_ids!="":
                response = requests.put(
                f'{BASE_URL}/albums/%s/assets'%album_id, headers=headers, data=data)  
                try:      
                    r =  response.json()
                    if 'statusCode' in r and  r['statusCode']!=200:
                        print(response.text)
                        print("Unable to update albums: photo_ids:",photo_ids," albumid:",album_id)
                except Exception as e:
                        print(e)
                        print("photo_ids:",photo_ids)

def get_directory_files(dir_path):
    files_list=[]
    support_file_ext=CONFIG_DATA['fileExt'].replace(" ","").replace(".","").lower().split(",")
    for root, dirs, files in os.walk(dir_path):
        for name in files:            
            file_name, file_extension = os.path.splitext(name)   
            file_extension=file_extension.replace(".","")         
            if file_extension.lower() in support_file_ext :
                files_list.append(os.path.join(root, name))
        if not RECURSIVE:
            break
    return files_list



# Worker, handles each task
def worker():
    while True:
        item = q.get()
        if item is None:
            break
        print("Uploading", item)
        upload(item)
        q.task_done()


def start_workers(worker_pool=10):
    threads = []
    for i in range(worker_pool):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    return threads


def stop_workers(threads):
    # stop workers
    for i in threads:
        q.put(None)
    for t in threads:
        t.join()


def create_queue(task_items):
    for item in task_items:

        q.put(item)

def get_sha1_base64encode(file):
    sha1 = hashlib.sha1()
    with open(file, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break        
            sha1.update(data)
    return base64.b64encode(sha1.digest()).decode('ascii')

def split_list(the_list, chunk_size):
    result_list = []
    while the_list:
        result_list.append(the_list[:chunk_size])
        the_list = the_list[chunk_size:]
    return result_list
def config():
    global API_KEY,BASE_URL,CONFIG_DATA
    load_config()

    if not 'fileExt' in CONFIG_DATA:
        CONFIG_DATA['fileExt']="png,jpg,hec"
    if not 'instanceUrl' in CONFIG_DATA:
        CONFIG_DATA['instanceUrl']=""
    if not 'apiKey' in CONFIG_DATA:
        CONFIG_DATA['apiKey']=""
    config_text=("instanceUrl: %s \napiKey: %s \nfileExt: %s\n")%(CONFIG_DATA['instanceUrl'],CONFIG_DATA['apiKey'],CONFIG_DATA['fileExt'])
    print("This is your current configuration-if you don't want to change, just leave it blank")
    print(config_text)

    new_base_url=input("Your new API endpoint( http://server:2238/api)  - must include /api):")
    new_api_key=input("Token Key:")
    new_file_ext=input("File Ext(png,jpg):")
    
    new_base_url=new_base_url.strip()
    new_api_key=new_api_key.strip()
    new_file_ext=new_file_ext.strip()
    if new_base_url!="":
        CONFIG_DATA['instanceUrl']=new_base_url
    if new_api_key!="":
        CONFIG_DATA['apiKey']=new_api_key
    if new_file_ext!="":
        CONFIG_DATA['fileExt']=new_file_ext
    if not 'fileExt' in CONFIG_DATA:
        CONFIG_DATA['fileExt']="png,jpg,hec"
 
    API_KEY=CONFIG_DATA['apiKey']
    BASE_URL=CONFIG_DATA['instanceUrl']

    config_text=("instanceUrl: %s \napiKey: %s \nfileExt: %s\n")%(CONFIG_DATA['instanceUrl'],CONFIG_DATA['apiKey'],CONFIG_DATA['fileExt'])
    file = open(CONFIG_FILE, 'w') 
    file.write(config_text) 
    file.close() 
    print("Authentication File saved to: %s"%CONFIG_FILE)

def load_config():
    global API_KEY,BASE_URL,CONFIG_DATA
    try:
        with open(CONFIG_FILE, 'r') as file:
            CONFIG_DATA = yaml.safe_load(file)
        API_KEY=CONFIG_DATA["apiKey"]
        BASE_URL=CONFIG_DATA["instanceUrl"]
    except Exception as e:
        return

def upload_folder(dir_path):
    # The queue for tasks
    global q
    q = queue.Queue()
    # list to store files name
    files_list=get_directory_files(dir_path)
    #print(files_list)
    photo_list=get_all_photos_data()
    #print(photo_list)
   # exit(0)
    files_to_upload=[]
    for file in files_list:
        stats = os.stat(file)
        file_name = os.path.basename(file ).lower() 
        head, dir_name = os.path.split(os.path.dirname(file ) )

        file_name_without_ext=os.path.splitext(file_name)[0]        
        photo_key=("%s-%s") %(file_name,stats.st_size)
        photo_key_without_ext=("%s-%s") %(file_name_without_ext,stats.st_size)
        if (not photo_key  in photo_list) and  (not photo_key_without_ext in photo_list):
            if args.sha1:                
                found_duplicate=False
                checksum=(get_sha1_base64encode(file))
                for key,photo in photo_list.items():                                    
                    if photo["checksum"]==checksum : 
                        found_duplicate=True
                        break
                if found_duplicate:
                    print("Photo %s (%s) exists - checksum"%(file,checksum))
                    continue
            if TEST_RUN:
                print("File is supposed to be uploaded: %s"%file)
                continue
            else:
                #print(photo_key,file_name_without_ext,file)
                files_to_upload.append(file)  
                #exit(0)
        else:                
            print("Photo %s (%s) exists"%(file,photo_key))


    # Start up your workers
    workers = start_workers(worker_pool=max_worker)
    create_queue(files_to_upload)
    # Blocks until all tasks are complete
    q.join()
    stop_workers(workers)

#delete files that is being marked as deleted(trash) from online 
#it support test run - it just show what to be deleted
def delete_local_files(dir_path):
    global TEST_RUN
    print("Deleting local files in folder ",dir_path)
    all_photos=get_all_photos_data()
    files_list=get_directory_files(dir_path)
    trash_photos={}
    for photo_key in all_photos:
            if all_photos[photo_key]["isTrashed"]:
                trash_photos[photo_key]=all_photos[photo_key]
    for file in files_list:
        checksum=get_sha1_base64encode(file)
        for photo_key in trash_photos:
            if trash_photos[photo_key]["isTrashed"] and trash_photos[photo_key]["checksum"] ==checksum:
                if(TEST_RUN):
                     print("Files to be deleted: ",file)
                else:
                    print("Deleting : ",file)
                    os.remove(file)
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-a','--album',  action='store_true',help='Creating ablums based on directory name.')
    parser.add_argument('-r','--recursive' ,action='store_true',help='Including sub directory.')
    parser.add_argument('--deletealbum' ,action='store_true',help='Delete album created by this tool - it does not delete your assets.')
    parser.add_argument('-t','--test' ,action='store_true',help='See what file to upload - it will not upload')
    parser.add_argument('-m','--max_worker' ,type=int , required=False,help='Max worker to upload - must be a number')
    parser.add_argument('-s','--sha1' ,action='store_true',help='Use sha1 to check duplicate')
    parser.add_argument('--deletelocal' ,action='store_true',help='remove files from local if it\'s deleted from online. It\'s based on folder name')
    #parser.add_argument('-h','--help' ,type=str,help='Print Help',required=False)
    parser.add_argument('command', nargs=argparse.REMAINDER,help='Command (config or  folder1 folder2 folder3)')
    args = parser.parse_args()
    
    max_worker=(args.max_worker)
    if max_worker==None:
        max_worker=20
    if args.test:
        TEST_RUN=True
    


    if len(args.command)<1:
        print("Pls specify the upload dir to upload: immic_uploader folder name")
        exit(1)

    if os.path.basename(sys.argv[1])=="config":
        config()
        exit(0)
    if args.command[0]=="config":
        config()
        exit()
    load_config()
    ping_server()
    if args.deletealbum:
        print("Deleting all albums created by this tool")
        delete_all_album()
        exit()
    if args.recursive:
        RECURSIVE=True
    print("Your endpoint is: %s"%BASE_URL)
    print("If you want to change your endpoint or token, run this: %s config " % os.path.basename(sys.argv[0]))

    dir_path=args.command[0]

    if API_KEY=="" or BASE_URL=="":
        config()
    
    if args.deletelocal:
        delete_local_files(dir_path)
        exit(0)
    print("File ext to upload:%s"%CONFIG_DATA['fileExt'])
    for dir_path in args.command: 
        print("Uploading folder:%s" % dir_path)
        upload_folder(dir_path)
        #do we wwant to map photos to album using folder name ?
        if args.album:
            sync_folders_album(dir_path)
            
   
    exit()
