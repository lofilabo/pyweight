'''
Created on 10 Apr 2019

@author: richard
'''
from pymongo import MongoClient
import pprint


class AuditTestRecords(object):

    def __init__(self):
        i = 1
        self.lstCampaigns = []
        self.lstClients = []
        self.lstPartners = []
        self.countCampaigns = 0
        self.countClients = 0
        self.countPartners = 0

    def run(self, tdate):
        self.tdate = tdate

        self.coll_setup_1()
        self.mongoCursor = self.collection.find()
        
        print("Count:" , self.mongoCursor.count())
        i=0
        j=0
        for post in self.mongoCursor:

            #print(post['campaign'])
            if((post['campaign'] in self.lstCampaigns) == False):
                self.lstCampaigns.append(post['campaign'])
                self.countCampaigns += 1

            if((post['client'] in self.lstClients) == False):
                self.lstClients.append(post['client'])
                self.countClients += 1

            if((post['partner'] in self.lstPartners) == False):
                self.lstPartners.append(post['partner'])
                self.countPartners += 1


            if(i>=10000):
                #print(j , "records")
                i=0
            i+=1
            j+=1
         
        print("\n\nCampaigns")                  
        pprint.pprint(self.lstCampaigns)
        print("\n\nClients")  
        pprint.pprint(self.lstClients)
        print("\n\nPartners")  
        pprint.pprint(self.lstPartners)

    def coll_setup_1(self):
        client = MongoClient('localhost', 27017)
        self.db = client["surveydb-staging"]
        self.collection = self.db.brokerdata


if __name__ == '__main__':

    main = AuditTestRecords()
    main.run("04/09/2019 23:59")
