import time
import random
import copy
import threading
import Queue
from datetime import datetime
import pytz
import numpy

# Different types of data 
# point in time
# - When archiving you could take an average


# events
# - Each event is discrete
# - When archiving you want to 

DEBUG = True

class Record:

    def __init__(self):
        self.data = []

        # Set the archive ranges. This can be configurable...

        total_record = 0


    def add(self,value,record_time=None):

        if record_time:
            # [time,value,min,max,num_records]
            self.data.append([record_time,value,value,value,1])
        else:
            self.data.append([time.time(),value,value,value,1])

    def get(self,t_0=None,t_1=None,start=None):
        if not t_0 or not t_1:
            return copy.deepcopy(self.data)
        else:
            dataSet = []
            index = 0
            if start:
                index = start

            #data_copy = copy.deepcopy(self.data) 

            for r in self.data[start:]:
                if r[0] > t_0 and r[0] < t_1:
                    dataSet.append(r)

                if r[0] > t_1:
                    break

                index += 1

            return copy.deepcopy(dataSet) , index

    def calc_average(self,data):
        data_sum = 0
 
        data_min = data[0][2]
        data_max = data[0][3]

        total_records = 0

        for d in data:
            total_records += d[4]

            data_sum += d[1]*d[4]
            if d[2] < data_min:
                data_min = d[2]
            if d[3] > data_max:
                data_max = d[3]

        if total_records == 0:
            return 0 , 0 , 0 , 0
        else:
            return data_sum / float(total_records) , data_min , data_max , total_records

    def archive(self, archive_times):
        now = time.time()

        # Get all the records for a time period.
        new_data = []
        found_all = False

        # We can archive the different periods at different frequencies.
        # I.e. there is no need to archive data every 5 minutes between the 5year and 30 year period
        for time_set in archive_times:

            # Calculate the oldest and newest times
            oldest = now-time_set[1]
            newest = now-time_set[1]+time_set[2]
            t = 0
            index = 0
            while newest < now-time_set[0]:
                oldest += time_set[2]
                newest += time_set[2]
                t += 1

                series , index = self.get(oldest,newest,index)

                if len(series) > 0:
                    avg , new_min , new_max , num_pts = self.calc_average(series)
                    #print avg , new_min , new_max , num_pts
                    new_data.append([(oldest+newest)/2,avg,new_min,new_max,num_pts])

                if index >= len(self.data):
                    found_all = True
                    break

            if found_all:
                break

        then = time.time()
        self.data = copy.deepcopy(new_data)

class RecordManager:

    def __init__(self,archive_times=None):
        self.records = {}
        self.archive_times = []
        if archive_times:
            self.archive_times = copy.deepcopy(archive_times)
        else:
            self.archive_times.append([5*365*86400,30*365*86400,28*86400,"30year"]) # Keep records once a month for 30 years
            self.archive_times.append([180*86400,5*365*86400,7*86400,"5year"]) # Keep records once a week for 5 years
            self.archive_times.append([28*86400,180*86400,86400,"6month"]) # Keep records once a day for 6 months
            self.archive_times.append([7*86400,28*86400,21600,"month"]) # Keep records once 6 hours for a month
            self.archive_times.append([86400,86400*7,3600,"week"]) # Keep records once an hour for a week
            self.archive_times.append([3600,86400,900,"day"]) # Keep records once 15 minute for a day
            self.archive_times.append([900,3600,60,"hour"]) # Keep records once a minute for an hour
            self.archive_times.append([300,900,15,"15min"]) # Keep records once 15 seconds for 15 minutes (360)
            self.archive_times.append([0,300,1,"5min"]) # Keep records once a second for 5 minutes (360)

        threading.Thread(target=self.archive).start()

    def archive(self):

        last_archive = time.time()
        archive_period = 1 # Archive every 5 minutes

        run = True
        times_run = 0
        while run:
            
            if time.time() > last_archive + archive_period:
                print "Archiving"
                last_archive = time.time()

                for r in self.records:
                    self.records[r].archive(self.archive_times)

                now = time.time()

                print now - last_archive

                times_run += 1

                if times_run == 5:
                    run = False

            time.sleep(1)
            

    def createRecord(self,record_name):

        if record_name in self.records:
            return 201
        else:   
            self.records[record_name] = Record()

    def addValue(self,record_name,value,time=None):

        if record_name not in self.records:
            raise
        else:   
            self.records[record_name].add(value,time)

    def getRecord(self,record_name,t_0,t_1):
        data , index = self.records[record_name].get(t_0,t_1)
        return copy.deepcopy(data)

    def deleteRecord(self,record_name):

        keys = []

        for key in self.records:
            keys.append(key)

        for key in keys:
            if key.find(record_name) == 0:
                del self.records[key]
            else:
                raise

    def listRecords(self,pattern=None):
        keys = []

        for key in self.records:
            if pattern:
                if not key.find(record_name) == 0:
                    continue
            keys.append(key)

        return copy.deepcopy(keys)

if __name__ == '__main__':

    # Create a Record Manager
    manager = RecordManager()

    record_name = "record1"
    t_0 = time.time()
    t = t_0 - 360000

    count = 0
    while t < t_0:
        t += random.randint(0,2)

        rv = manager.addValue(record_name,random.randint(0,100),time=t)
        if rv == 404:
            manager.createRecord(record_name)
            rv = manager.addValue(record_name,random.randint(0,100),time=t)

        count +=1

    time.sleep(5)

    print manager.listRecords()

    new_data =  manager.getRecord(record_name,time.time()-360000,time.time())

    now = time.time()
    for d in new_data:
        print d[0]-now,",",d[1],",",d[2],",",d[3],",",d[4]



