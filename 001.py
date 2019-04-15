'''
Created on 27 Mar 2019

@author: richard
'''
from pymongo import MongoClient
import pprint
import bson
from Crypto.Util import _counter

"""
BROKERDATA fields:
_id
status
status_changed
partner
sys_recieved  <--------absent in some records
timestamp
client
campaign

"""

dailycap = 1000

weighting_by_partners = {
    "dynaxon": 60,
    "vigeo": 40,
    "adminds": 2,
    "datablazers": 2,
    "firstrite": 2,
    "flex": 2,
    "cegurats": 2,
    "rhint": 2,
    "permission": 2,
    "readgroup": 2
}

cap_by_partners = {
    "dynaxon": 0,
    "vigeo": 0,
    "adminds": 0,
    "datablazers": 0,
    "firstrite": 0,
    "flex": 0,
    "cegurats": 0,
    "rhint": 0,
    "permission": 0,
    "readgroup": 0
}

availibility_by_partners = {
    "dynaxon": 0,
    "vigeo": 0,
    "adminds": 0,
    "datablazers": 0,
    "firstrite": 0,
    "flex": 0,
    "cegurats": 0,
    "rhint": 0,
    "permission": 0,
    "readgroup": 0
}


def coll_setup_1():
    client = MongoClient('localhost', 27017)
    db = client["surveydb-staging"]
    coll = db.brokerdata
    return coll


def list_posts_1(posts):
    """
    for post in posts:
        #print(post['_id'])
        if "sys_recieved" in post:
            print(post['sys_recieved'])
        if "status" in post:
            print(post['status'])
        if "status_changed" in post:
            print(post['status_changed'])
        if "partner" in post:
            print(post['partner'])
        #-->print(post['sys_recieved'])
        if "timestamp" in post:
            print(post['timestamp'])
        if "client" in post:
            print(post['client'])
        if "campaign" in post:
            print(post['campaign'])
    """


def process_weights():

    for key, value in weighting_by_partners.items():
        print(key)
        return


def process_posts_1(posts, collection):
    for key, value in weighting_by_partners.items():
    #        process_posts_from_one_partner(posts, collection, key)
        
        process_availibility_from_one_partner(posts, collection, key)
    pprint.pprint(availibility_by_partners)


def process_availibility_from_one_partner(posts, collection, partner):

    count = 0
    for post in posts:
        if "client" in post:
            if "campaign" in post:
                if (post['campaign'] == 'CHILDREN'
                    and
                        post['client'] == 'Find_An_Expert_Ltd'):

                    if(post['partner'] == partner):
                        count = count + 1

        if (count >= cap_by_partners[partner]):
            print(count, " : TARGET AVAILABLE (", partner,  ")")
            availibility_by_partners[partner]=count
            return

    print("NOT ENOUGH FOR TARGET ( " + partner + " ):", count , " AVAILABLE")
    availibility_by_partners[partner]=count


def process_posts_from_one_partner(posts, collection, partner):

    count = 0
    for post in posts:
        if "client" in post:
            if "campaign" in post:
                if (post['campaign'] == 'CHILDREN'
                    and
                        post['client'] == 'Find_An_Expert_Ltd'):

                    if(post['partner'] == partner):
                        update_brokerdata_set_status_by_id(
                            post['_id'], collection)
                        count = count + 1

        if (count >= cap_by_partners[partner]):
            print(count, "(", partner,  ")")
            return

    print("RAN OUT OF POSTS. TOTAL AVAILABLE ( " + partner + " ):", count)


def reset_all_brokerdata_to_accepted(collection):
    posts = collection.find()
    for post in posts:
        myquery = {"_id": post['_id']}
        newvalues = {"$set": {"status": "accepted"}}
        collection.update_one(myquery, newvalues)


def update_brokerdata_set_status_by_id(cid, collection):
    myquery = {"_id": cid}
    newvalues = {"$set": {"status": "accepted"}}
    collection.update_one(myquery, newvalues)


def setup_cap_by_partners():
    for key, value in cap_by_partners.items():
        curcap = int((weighting_by_partners[key] / 100) * dailycap)
        cap_by_partners[key] = curcap


def mgr():
    setup_cap_by_partners()
    pprint.pprint(cap_by_partners)
    c = coll_setup_1()
    posts = c.find()
    # list_posts_1(posts)
    process_posts_1(posts, c)


if __name__ == '__main__':
    mgr()
