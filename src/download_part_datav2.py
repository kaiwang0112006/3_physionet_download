# -*- coding: utf-8 -*-
import urllib
import requests
import wget
import os
import shutil
BASEPATH = r"C:\work\data\physionet.org\2.0\training"
import time
from concurrent.futures import ThreadPoolExecutor

def download_dest(url, dest, retry=1000000000, time_sleep=160):
    count = 0
    print("\ndownloading %s to %s" % (url, dest))
    while count<retry:
        try:
            if not os.path.exists(dest):
                #wget.download(url, dest)
                proxies = {'https': '127.0.0.1:7890','http': '127.0.0.1:7890'}
                r = requests.get(url,  proxies=proxies)
                with open(dest, 'wb') as f:
                    f.write(r.content)
            return True
        except:
            time.sleep(time_sleep)
        count +=1
    return False


def main():
    idlist = []
    with open("record.txt") as f:
        for eachline in f:
            line = eachline.strip()
            id = line.split("/")[1]
            idlist.append(id)

    idlist = sorted(idlist)
    filelist = []
    for id in idlist:
        dpath = os.path.join(BASEPATH,id)
        if not os.path.exists(dpath):
            os.makedirs(dpath)

        # Clinical Data
        dfile = "%s.txt" % id
        url = "https://physionet.org/files/i-care/2.0/training/%s/%s.txt" % (id, id)
        dest = os.path.join(dpath, dfile)
        if not os.path.exists(dest):
            print("\ndownloading %s to %s" % (url, dest))
            #download_file(url, dest)
            download_dest(url, dest)

        # record
        dfile = "RECORDS"
        url = "https://physionet.org/files/i-care/2.0/training/%s/RECORDS" % (id)
        dest = os.path.join(dpath, dfile)
        if not os.path.exists(dest):
            print("\ndownloading %s to %s" % (url, dest))
            #download_file(url, dest)
            download_dest(url, dest)

        with open(dest) as fr:
            for eachline in fr:
                line = eachline.strip()
                if "EGG" in line or 'ECG' in line:
                    h = int(line.split("_")[2])
                    if h<=72:
                        for suffix in ["hea","mat"]:
                            dfile = line+"."+suffix
                            url = "https://physionet.org/files/i-care/2.0/training/%s/%s" % (id, dfile)
                            dest = os.path.join(dpath, dfile)
                            filelist.append((url, dest))
    # when run first time save the list to see the size
    #with open("job_url.txt",'w') as f:
    #    for items in filelist:
    #        f.write("%s\t%s\n" % (items[0],items[1]))
    print("job size is %s" % str(len(filelist)))
    with ThreadPoolExecutor(max_workers=16) as pool:
        for url, dest in filelist:
            pool.submit(download_dest, url, dest)

if __name__ == "__main__":
    main()