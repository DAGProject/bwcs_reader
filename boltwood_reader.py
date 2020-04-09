# -*- coding: utf-8 -*-
"""
@author: Recep Balbay
"""


import mysql.connector as mariadb
from datetime import datetime
from time import sleep
from os import mkdir, path, remove, replace

start = 0
basePath = path.dirname(path.realpath(__file__)) + "\\"
dataFolderPath = basePath + "BWCSDATA\\"
bwcsTempFile = basePath + 'bwcsTemp.txt'

bwcsSql = "INSERT INTO `2020` (`date`, `H`, `D`, `E`, `C`, `W`, `R`, `l`, `CC`, `SKY`, `AMB`, `WIND`," \
          "`WW`, `RR`, `HUM`, `DEW`, `CAS`, `HEA`, `BLKT`, `HH`, `PWR`, `WNDTD`, `WDROP`, `WAVG`, `WDRY`, `RHT`," \
          "`AHT`, `ASKY`, `ACSE`, `APSV`, `ABLK`, `AWND`, `AWNE`, `DKMPH`, `VNE`, `RWOSC`, `DD`, `ADAY`, `PH`, `CN`," \
          "`T`, `S`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
          "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"


def utcNow():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def dataFolder():
    year = datetime.utcnow().strftime('%Y') + "\\"

    if path.isdir(dataFolderPath):
        pass
    else:
        try:
            print(utcNow() + " | BWCSDATA Folder can not find. Creating...")
            mkdir(dataFolderPath)
            sleep(1)
            print(utcNow() + " | BWCSDATA Folder created: " + dataFolderPath)
        except WindowsError as w:
                print(utcNow() + " | " + w.args[0])

    if path.isdir(dataFolderPath+year):
        pass
    else:
        try:
            print(utcNow() + " | Year Folder can not find. Creating...")
            mkdir(dataFolderPath+year)
            sleep(1)
            print(utcNow() + " | Year Folder created: " + dataFolderPath+year)
        except WindowsError as w:
            print(utcNow() + " | " + w.args[0])

    sleep(1)


def dayFile():
    year = datetime.utcnow().strftime('%Y') + "\\"
    bwcsDayFile = dataFolderPath + year + datetime.utcnow().strftime('%Y%m%d') + ".txt"

    if path.exists(bwcsDayFile):
        pass
    else:
        print(utcNow() + " | BWCS DAY file can not find. Creating...")
        with open(bwcsDayFile, 'w') as day:
            day.write("YYYY-mm-DD HH:MM:SS.SS,h,D,E,C,W,R,1,c,SKY,AMB,WIND,w,r,HUM,DEW,CASE,HEA,BLKT,H,PWR,WNDTD,WDROP,"
                      "WAVG,WDRY,RHT,AHT,ASKY,ACSE,APSV,ABLK,AWND,AVNE,DKMPH,VNE,RWOSC,D,ADAY,PH,CN,T,S\n")
            sleep(1)
            day.close()
        print(utcNow() + " | BWCS DAY file created: " + bwcsDayFile)

    sleep(1)


def rename():
    bwcsFile = basePath + datetime.utcnow().strftime('%Y-%m-%d') + ".txt"

    if path.exists(bwcsFile):
        try:
            print(utcNow() + " | Renaming file: " + bwcsFile)
            replace(bwcsFile, bwcsTempFile)
            sleep(1)
            print(utcNow() + " | File renamed: bwcsTemp.txt")
        except WindowsError as w:
            if w.args[0] == 13:
                print(utcNow() + " | Permission denied. Rolling back actions.")
                sleep(1)
                replace(bwcsTempFile, bwcsFile)
            else:
                print(utcNow() + w.args[0])
    else:
        print(utcNow() + " | Waiting for new data file.")

    sleep(1)


def bwcsDo():
    year = datetime.utcnow().strftime('%Y') + "\\"
    bwcsDayFile = dataFolderPath + year + datetime.utcnow().strftime('%Y%m%d') + ".txt"

    tempList1 = []
    tempList2 = []
    bwcsProper = []

    if path.exists(bwcsTempFile):
        with open(bwcsTempFile, 'r', encoding='iso-8859-15') as f:
            data = f.readlines()

        for i in data:
            tempList1.append(i.strip().split())

        for i in tempList1:
            if (len(i) == 43) and (i[2] == "M"):
                tempList2.append(i)

        bwcsProper.append(tempList2[0])
        tempList2.pop(0)

        for i in tempList2:
            if (bwcsProper[-1][1].split(':')[0] == i[1].split(':')[0]) and (bwcsProper[-1][1].split(':')[1] == i[1].split(':')[1]):
                pass
            else:
                bwcsProper.append(i)

        for i in bwcsProper:
            lineDate = i.pop(0)
            i[0] = lineDate + " " + i[0]

        try:
            AtaDb = mariadb.connect(
                host="",    # Server IP address
                user="",    # Username
                passwd="",  # Password
                database='boltwood')
            AtaDbCursor = AtaDb.cursor()
            AtaDbCursor.execute("SELECT * FROM `2020` ORDER BY `date` DESC LIMIT 1")
            bwcsLastLine = AtaDbCursor.fetchone()

            if (str(bwcsLastLine[0]).split(' ')[1].split(':')[0]) == (bwcsProper[0][0].split(' ')[1].split(':')[0]) and (str(bwcsLastLine[0]).split(' ')[1].split(':')[1]) == (bwcsProper[0][0].split(' ')[1].split(':')[1]):
                print(utcNow() + " | Duplicate timed data. Passing.")
                bwcsProper.pop(0)

            for i in bwcsProper:
                AtaDbCursor.execute(bwcsSql, i)
                AtaDb.commit()
                sleep(0.05)

        except Exception as sqlError:
            print(utcNow() + " | " + getattr(sqlError, 'message', repr(sqlError)))

        if path.exists(bwcsDayFile):
            try:
                print(utcNow() + " | Processing file: " + bwcsDayFile)
                for i in bwcsProper:
                    with open(bwcsDayFile, 'a') as day:
                        day.write(str(",").join(i) + '\n')
                        day.close()
                print(utcNow() + " | Process finished.")
                sleep(1)

                try:
                    remove(bwcsTempFile)
                    print(utcNow() + " | bwcsTempFile.txt removed.")
                    sleep(1)

                except Exception as err:
                    print(utcNow() + " | " + getattr(err, 'message', repr(err)))

                print(utcNow() + " | Next cycle will start in 60 seconds.")

            except Exception as err:
                print(utcNow() + " | " + getattr(err, 'message', repr(err)))
    sleep(60)


while True:
    try:
        if start == 0:
            sleep(0.1)
            print(utcNow() + " | ------------------------ ATASAM BOLTWOOD Client v.1.2 ------------------------")
            print(utcNow() + " |   Client, first check and/or make folders. Then parsing starts. Please wait.  ")
            print(utcNow() + " | ------------------------------------------------------------------------------")
            start = 1
            sleep(1)
        else:
            dataFolder()
            dayFile()
            rename()
            bwcsDo()
    except Exception as e:
        print(utcNow() + " | " + getattr(e, 'message', repr(e)))
