'''
Created on 28 Mar 2019

@author: richard
'''
import sys
import random
import time
import pprint
import datetime
from collections import Counter
from random import randint

from pymongo import MongoClient


class MakeBrokerData(object):

    def __init__(self):

        self.clearDownDB_YN = True
        self.coll_setup_1()
        self.localNow = datetime.datetime.strptime("04/04/2019 13:00", "%m/%d/%Y %H:%M")
        self.objTs = Timestamps()

        self.objTs.windback = 24
        self.set_recordcount(20)

        self.partners = [
            "p1",
            "p2",
            "p3",
            "p4",
            "p5",
            "p6"
        ]

        self.campaigns = [
            "cmp1",
            "cmp2",
            "cmp3",
            "cmp4",
            "cmp5",
            "cmp6"
        ]

        self.clients = [
            "cli1",
            "cli2",
            "cli3",
            "cli4",
            "cli5",
            "cli6"
        ]

        self.records = []

    def mgr(self):
        """
        NOTES ABOUT THE USE OF THIS CLASS:

        This class invokes Timestamps.
        1.    It sets the instance of Timestamps to use THE SAME LOCALNOW as THIS CLASS.
        2.    It sets the instance of Timestamps to have windback time of its own!!

        WHAT's WINDBACK TIME??
        The number of hours, counting backwards, from localNow, which is used as the earliest 
        bound of the random-date-time

        clearDownDB_YN determines whether or not the database will be emptied before
        new records are generated.

        MAXRECORDS.
        At the moment, these are set in the __main__.
        Change this.
        """

        if(self.clearDownDB_YN == True):
            self.collection.delete_many({})

        for x in range(0, self.maxrecords):
            ts = self.objTs.randomTimeStamp()
            # print(ts)
            dataline = {'id': x,
                        'client': self.rs_client(),
                        'campaign': self.rs_campaign(),
                        'partner': self.rs_partner(),  # Pass TRUE to make 'random random' data set
                        'status': "new",
                        'timestamp': ts
                        }
            # self.records.append(dataline)
            """
            INSERT INTO MONGODB HERE.
            """
            self.collection.insert_one(dataline)

    def set_recordcount(self, recordcount):

        self.maxrecords = recordcount

    def run(self):

        self.objTs.localNow = self.localNow
        date1 = self.localNow
        strdate1 = date1.strftime("%m/%d/%Y %H:%M")
        #print("LOCAL DEF'N OF NOW:", strdate1)
        self.mgr()

    def weighted_random(self, pairs):
        total = sum(pair[0] for pair in pairs)
        r = randint(1, total)
        for (weight, value) in pairs:
            r -= weight
            if r <= 0:
                return value

    def rs_client(self):
        posn = random.randint(0, len(self.clients) - 1)
        return self.clients[posn]

    def rs_campaign(self):
        posn = random.randint(0, len(self.campaigns) - 1)
        return self.campaigns[posn]

    def rs_partner(self, weighYN=False):
        if(weighYN == True):
            upperLimit = random.randint(0, len(self.partners) - 1)
            posn = random.randint(0, upperLimit)
        else:
            posn = random.randint(0, len(self.partners) - 1)
        return self.partners[posn]

    def coll_setup_1(self):
        client = MongoClient('localhost', 27017)
        self.db = client["test_broker"]
        self.collection = self.db.brokerdata


class Timestamps():

    def __init__(self):

        #self.windback = 48
        #self.windback = 24
        self.windback = 24
        self.localNow = datetime.datetime.now()

    def strTimeProp(self, start, end, format, prop):
        """Get a time at a proportion of a range of two formatted times.

        start and end should be strings specifying times formated in the
        given format (strftime-style), giving an interval [start, end].
        prop specifies how a proportion of the interval to be taken after
        start.  The returned time will be in the specified format.
        """

        stime = time.mktime(time.strptime(start, format))
        etime = time.mktime(time.strptime(end, format))

        ptime = stime + prop * (etime - stime)

        return time.strftime(format, time.localtime(ptime))

    def randomDate(self, start, end, prop):
        return self.strTimeProp(start, end, '%m/%d/%Y %H:%M', prop)
        #%m/%d/%Y %I:%M%p

    def randomTimeStamp(self):

        date1 = self.localNow

        date2 = date1 - datetime.timedelta(hours=self.windback)
        #date2 = date1 - datetime.timedelta(hours=13)
        #date2 = date1 - datetime.timedelta(days=0)

        strdate1 = date1.strftime("%m/%d/%Y %H:%M")
        strdate2 = date2.strftime("%m/%d/%Y %H:%M")
        return self.randomDate(strdate1, strdate2, random.random())


if __name__ == '__main__':

    main = MakeBrokerData()
    main.run()
    # main.run()
