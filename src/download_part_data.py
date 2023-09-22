# -*- coding: utf-8 -*-
import time
import urllib
import requests
import wget
import os
import shutil
import time
BASEPATH = r"C:\work\data\physionet.org\2.0\training"

def download_dest(url, dest, retry=1000000000, time_sleep=160):
    count = 0
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


def download_file(url, file):
    query_parameters = {"downloadformat": "txt"}
    r = requests.get(url, params=query_parameters)
    with open(file, 'wb') as f:
        f.write(r.content)

def main():
    idlist = []
    with open("record.txt") as f:
        for eachline in f:
            line = eachline.strip()
            id = line.split("/")[1]
            idlist.append(id)

    idlist = sorted(idlist)
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

        filelist = []
        with open(dest) as fr:
            for eachline in fr:
                line = eachline.strip()
                if "EGG" in line or 'ECG' in line:
                    h = int(line.split("_")[2])
                    if h<=72:
                        for suffix in ["hea"]:
                            dfile = line+"."+suffix
                            url = "https://physionet.org/files/i-care/2.0/training/%s/%s" % (id, dfile)
                            dest = os.path.join(dpath, dfile)
                            if not os.path.exists(dest):
                                print("\ndownloading %s to %s" % (url, dest))
                                download_dest(url, dest)

if __name__ == "__main__":
    main()