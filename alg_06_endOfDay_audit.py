'''
Created on 9 Apr 2019

@author: richard
'''
from pymongo import MongoClient
import bson
import pprint


class EndOfDay(object):
    '''
    classdocs
    '''

    def __init__(self, strLocalNowDate_dateOnly, nrc):
        self.nrc = nrc
        self.coll_setup_1()
        self.approvedStatus = "accepted"
        self.bufferedStatus = "buffered"
        self.newStatus = 'new'
        self.archStatus = 'archive'

        self.differentDates = {}

        self.dateAudit = {
            'gt': 0,
            't': 0,
            'n': 0,
            'a': 0,
            'b': 0,
            'z': 0
        }

        self.coll_setup_1()
        self.strLocalNowDate = strLocalNowDate_dateOnly

    def coll_setup_1(self):
        client = MongoClient('localhost', 27017)
        self.db = client["test_broker"]
        self.collection = self.db.brokerdata

    def run(self):
        self.process_EOD_records_Status_Changed()
        self.process_EOD_records_Timestamp()
        self.dateAudit['n'] = self.nrc
        self.printAudit()

    def process_EOD_records_Status_Changed(self):
        """
        SAMPLE MONGODB QUERIES:
        db.brokerdata.find({'timestamp':/04\/03\/2019/}, {status:1,timestamp:1})
        """
        self.coll_setup_1()
        regx = bson.regex.Regex(self.strLocalNowDate + '*')
        self.mongoCursor = self.collection.find(
            {'status_changed': regx}, {'_id': 1, 'timestamp': 1, 'status': 1})

        for post in self.mongoCursor:
            dt = self.strLocalNowDate
            ty = post['status']

            if(ty == self.newStatus):
                self.dateAudit['gt'] += 1
            elif(ty == self.approvedStatus):
                self.dateAudit['a'] += 1
                self.dateAudit['gt'] += 1
            elif(ty == self.bufferedStatus):
                self.dateAudit['b'] += 1
                self.dateAudit['gt'] += 1
            elif(ty == self.archStatus):
                self.dateAudit['z'] += 1
                self.dateAudit['gt'] += 1
            else:
                self.differentDates['t'] += 1
                self.differentDates['gt'] += 1

    def process_EOD_records_Timestamp(self):
        """
        SAMPLE MONGODB QUERIES:
        db.brokerdata.find({'timestamp':/04\/03\/2019/}, {status:1,timestamp:1})
        """
        self.coll_setup_1()
        regx = bson.regex.Regex(self.strLocalNowDate + '*')
        self.mongoCursor = self.collection.find(
            {'timestamp': regx}, {'_id': 1, 'timestamp': 1, 'status': 1})

        for post in self.mongoCursor:
            dt = self.strLocalNowDate
            ty = post['status']
            if(ty == self.approvedStatus):
                self.dateAudit['t'] += 1

    def printAudit(self):
        dt = self.strLocalNowDate

        v = self.dateAudit
        print(f"Day {dt}: "
              f"\tNew Today: {v['n']},  "
              f"\tTotal Records in: {v['gt'] }, "
              f"\tAccepted: {v['a']},"
              f" \tBuffered out: {v['b']} "
              f"(of allowed buf. {int(v['a']/10)}), "
              f"\tSurplus released : {v['z']}")


if __name__ == '__main__':
    nrd = {}
    main = EndOfDay("04/04/2019",nrd)
    main.run()
