'''
Created on 27 Mar 2019

@author: richard
'''
from pymongo import MongoClient
import pprint


def coll_setup_1():
    client = MongoClient('localhost', 27017)
    db = client["surveydb-staging"]
    coll = db.brokerdata
    return coll


def list_posts_1(posts):

    for post in posts:
        # print(post['_id'])
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


def mgr():

    c = coll_setup_1()
    posts = c.find()
    # list_posts_1(posts)
    #process_posts_1(posts, c)


if __name__ == '__main__':
    mgr()
