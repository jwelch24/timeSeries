import time
import random
import copy
import threading
import Queue

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
        self.archive_times = []
        self.archive_times.append([0,300,1]) # Keep records once a second for 5 minutes
        self.archive_times.append([300,3600,60]) # Keep records once a minute for an hour
        self.archive_times.append([3600,86400,900]) # Keep records once 15 minute for a day
        self.archive_times.append([86400,86400*7,3600]) # Keep records once an hour for a week 
        self.archive_times.append([7*86400,28*86400,21600]) # Keep records once 6 hours for a month
        self.archive_times.append([28*86400,180*86400,86400]) # Keep records once a day for 6 months
        self.archive_times.append([180*86400,5*365*86400,7*86400]) # Keep records once a week for 5 years

    def add(self,value,record_time=None):

        if record_time:
            self.data.append([record_time,value])
        else:
            self.data.append([time.time(),value])

    def get(self,t_0=None,t_1=None):
        if not t_0 or not t_1:
            return copy.deepcopy(self.data)
        else:
            dataSet = []
            for r in self.data:
                if r[0] > t_0 and r[0] < t_1:
                    dataSet.append(r)



            return copy.deepcopy(dataSet)

    def archive(self):

        now = time.time() + 3600
        for time_set in self.archive_times:

            print now,now-time_set[1],now-time_set[0],len(self.get(now-time_set[1],now-time_set[0]))


class RecordManager:

    def __init__(self):
        self.records = {}

        threading.Thread(target=self.archive).start()

    def archive(self):

        last_archive = time.time()
        archive_period = 5 # Archive every 5 minutes
        while True:

            if time.time() > last_archive + archive_period:
                print "Archiving"
                last_archive = time.time()

                for r in self.records:
                    self.records[r].archive()

            time.sleep(1)

    def createRecord(self,record_name):

        if record_name in self.records:
            return 201
        else:   
            self.records[record_name] = Record()

    def addValue(self,record_name,value,time=None):

        if record_name not in self.records:
            return 404
        else:   
            self.records[record_name].add(value,time)

    def getRecord(self,record_name,t_0,t_1):
        return self.records[record_name].get(t_0,t_1)

if __name__ == '__main__':

    # Create a Record Manager
    manager = RecordManager()

    # Create a new Record
    t_0 = time.time()
    t = t_0
    for i in range(0,500):
        t += 1
        rv = manager.addValue("voltages",random.randint(220,260),time=t)
        if rv == 404:
            manager.createRecord("voltages")
            rv = manager.addValue("voltages",random.randint(220,260),time=t)






