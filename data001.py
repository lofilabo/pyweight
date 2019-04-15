'''
Created on 28 Mar 2019

@author: richard
'''
from pymongo import MongoClient
import pprint


def render_line_1(post): return print(post['_id'], "\t", post['status'], "\t",  post['status_changed'],
                                      "\t", post['partner'], "\t",  post['timestamp'], "\t", post['client'], "\t",  post['campaign'])


def process_posts_1(posts, collection):
    for post in posts:
        render_line_1(post)


def coll_setup_1():
    client = MongoClient('localhost', 27017)
    db = client["surveydb-staging"]
    coll = db.brokerdata
    return coll

if __name__ == '__main__':
    c = coll_setup_1()
    posts = c.find()
    process_posts_1(posts, c)
