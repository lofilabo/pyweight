'''
Created on 5 Apr 2019

@author: richard
'''

from pymongo import MongoClient
import pprint


class AuditRecords():
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
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

    def coll_setup_1(self):
        client = MongoClient('localhost', 27017)
        self.db = client["test_broker"]
        self.collection = self.db.brokerdata

    def getDifferentDates(self):
        self.mongoCursor = self.collection.find()
        for post in self.mongoCursor:
            dt = str(post['timestamp'][:10])
            if not(dt in self.differentDates):
                da = self.dateAudit.copy()
                self.differentDates.update({dt: da})
                # print(self.differentDates[dt]['n'])

    def getOneDate(self, d):

        if not(d in self.differentDates):
            da = self.dateAudit.copy()
            self.differentDates.update({d: da})
            # print(self.differentDates[dt]['n'])

        # pprint.pprint(self.differentDates)
    def auditListAll(self):
        for k, v in self.differentDates.items():
            self.auditList(k)

    def auditList(self, fullDateUnderInspection):

        dui = fullDateUnderInspection[:10]

        self.mongoCursor = self.collection.find()
        for post in self.mongoCursor:
            dt = str(post['timestamp'][:10])
            if(dt == dui):
                ty = post['status']

                if(ty == self.newStatus):
                    self.differentDates[dt]['n'] += 1
                    self.differentDates[dt]['t'] += 1
                    self.differentDates[dt]['gt'] += 1
                elif(ty == self.approvedStatus):
                    self.differentDates[dt]['a'] += 1
                    self.differentDates[dt]['t'] += 1
                    self.differentDates[dt]['gt'] += 1
                elif(ty == self.bufferedStatus):
                    self.differentDates[dt]['b'] += 1
                    self.differentDates[dt]['t'] += 1
                    self.differentDates[dt]['gt'] += 1
                elif(ty == self.archStatus):
                    self.differentDates[dt]['z'] += 1
                    self.differentDates[dt]['t'] += 1
                    self.differentDates[dt]['gt'] += 1
                else:
                    self.differentDates[dt]['t'] += 1
                    self.differentDates[dt]['gt'] += 1

    def retroactively_update_surplusAll(self):
        for k, v in self.differentDates.items():
            self.retroactively_update_surplus(k)

    def retroactively_update_surplus(self, dui):

        for k, v in self.differentDates.items():
            v['z'] = 0

        self.mongoCursor = self.collection.find()
        #self.differentDates[dui]['a'] = 0
        for post in self.mongoCursor:
            dt = str(post['timestamp'][:10])
            if(dt == dui):
                ty = post['status']

                if(ty == self.archStatus):
                    self.differentDates[dt]['z'] += 1
                # if(ty == self.approvedStatus):
                #    self.differentDates[dt]['a'] += 1

    def printResults(self):
        i = 0
        j = 0
        print("\n")
        print("FINAL ROUND-UP")

        for k, v in self.differentDates.items():
            i += 1
            #tbs = (v['t'] - (v['t'] % 10)) / 10
            tbs = int((v['t'] / 10))

            print(
                f"Day {i} ({k}): Records in: {v['gt']}, Accepted: {v['a']}, \tBuffered: {v['b']} (of allowed buf. {int(v['a']/10)}), \tSurplus after release : {v['z']}")

    def run(self):

        self.getDifferentDates()
        self.auditListAll()
        self.printResults()

    def run_withOneDate(self, dateonly):
        self.getOneDate(dateonly)
        self.auditList(dateonly)


if __name__ == '__main__':

    main = AuditRecords()
    main.run()

    # main.run_withOneDate("04/01/2019")
    # main.printResults()
