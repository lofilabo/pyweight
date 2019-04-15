'''
Created on 9 Apr 2019

@author: richard
'''
from pyweight.alg_03_make_longterm_data import DoThingsWithDate
import random
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
#from orca.orca import die
import time


class TidyOldBuffers(object):

    def __init__(self):
        self.approvedStatus = "accepted"
        self.bufferedStatus = "buffered"
        self.newStatus = 'new'
        self.archStatus = 'archive'

        self.dtf = DoThingsWithDate()

    def coll_setup_1(self):
        client = MongoClient('localhost', 27017)
        self.db = client["test_broker"]
        self.collection = self.db.brokerdata

    def run(self, todaysDate):
        
        number_of_days_to_expire_buffer_records = 3
        
        self.todaysDate = todaysDate
        twodaysorless = 0
        morethantwodays = 0
        buffertobeexpired = 0

        self.coll_setup_1()
        self.mongoCursor = self.collection.find({"status_changed": {"$exists": True}})
        for post in self.mongoCursor:
            # print(post)
            pdate_of_record = self.dtf.get_python_daydate_from_antrix_daydate(
                post['timestamp'])
            pdate_of_today = self.dtf.get_python_daydate_from_antrix_daydate(
                self.todaysDate)
            pdate_long_ago = self.dtf.get_python_daydate_from_antrix_daydate(
                "01/01/1700 00:00")
            pdate_two_days_ago = pdate_of_today - timedelta(days=number_of_days_to_expire_buffer_records)
            outside_last_two_days_TF = self.dtf.test_is_between_start_and_end__python_datetime(
                pdate_of_record, pdate_long_ago, pdate_two_days_ago)

            inside_last_two_days_TF = self.dtf.test_is_between_start_and_end__python_datetime(
                pdate_of_record, pdate_two_days_ago, pdate_of_today)

            if(inside_last_two_days_TF == True):
                twodaysorless += 1
            else:
                morethantwodays += 1
                
                if(post['status'] == self.bufferedStatus):
                    #print(post['timestamp'], "  " , post['status_changed'])
                    buffertobeexpired += 1
                    myquery = {"_id": post['_id']}
                    newvalues = {"$set": {'status': self.archStatus}}
                    rez = self.collection.update_one(myquery, newvalues)
                    # print(rez)
        #print(twodaysorless, "," , morethantwodays)
        print(f"Day {self.todaysDate[:10] }:\tExpired {buffertobeexpired}  two-day old buffer records")


if __name__ == '__main__':

    main = TidyOldBuffers()
    main.run("04/09/2019 23:59")
