'''
Created on 3 Apr 2019

@author: richard
'''
import random
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
#from orca.orca import die
import time
from test.support import calcobjsize


class make_longterm_data():

    def __init__(self):
        self.curdata = {
            'i': 0,
            'n': 0,
            'a': 0,
            'b': 0,
            'z': 0,
            'f': 0
        }

        self.proc_count = 0
        self.new_lodef = 0
        self.new_hidef = 0
        self.dailyreq = 1000
        self.fulfillment = 0
        self.hdata = []
        self.mgr(900, 1160)

    def printHdata(self):
        pstri = "CT. \t"
        pstrn = "NEW:\t"
        pstra = "ACC.\t"
        pstrb = "BUF.\t"
        pstrz = "ARC.\t"
        pstrf = "FFD.\t"

        for rec in self.hdata:
            pstri += str(rec['i']) + "\t"
            pstrn += str(rec['n']) + "\t"
            pstra += str(rec['a']) + "\t"
            pstrb += str(rec['b']) + "\t"
            pstrz += str(rec['z']) + "\t"
            pstrf += str(rec['f']) + "\t"

        print(pstri)
        print(pstrn)
        print(pstra)
        print(pstrb)
        print(pstrz)
        print("")
        print(pstrf)
        print("")

    def getRandomNew(self):
        return random.randint(self.new_lodef, self.new_hidef)

    def setup(self):
        self.curdata['i'] = "S/U"
        self.curdata['n'] = 1000
        self.curdata['a'] = 0
        self.curdata['b'] = 0
        self.curdata['z'] = 0
        self.curdata['f'] = '-'

        self.hdata.append(self.curdata.copy())

    def process(self):

        old_n = self.curdata['n']  # transient preservation of overnight value.

        self.curdata['i'] = "PRC " + str(self.proc_count)

        # New records have more than enough.
        if(self.curdata['n'] >= self.dailyreq):
            # deduct the quantity of records we want for today, from the new
            # records
            self.curdata['n'] -= self.dailyreq
            # set Accepted = Daily Requirement
            self.curdata['a'] = self.dailyreq
            # make fulfilment = daily requirement
            self.fulfillment = self.dailyreq
        else:
            # not enough in New.
            balance_from_buffer = self.dailyreq - self.curdata['n']
            # if we can make up balance from buffer
            if(self.curdata['b'] >= balance_from_buffer):
                self.curdata['n'] = 0                               # empty new
                # deduct balance from buffer
                self.curdata['b'] -= balance_from_buffer
                self.curdata['a'] = self.dailyreq
                # set Accepted to daily-requirements
                self.fulfillment = self.dailyreq
            else:
                # put all New records into accepted
                self.curdata['a'] = self.curdata['n']
                # put all buffer records into accepted
                self.curdata['a'] += self.curdata['b']
                # empty New - we know we're going to use everything.
                self.curdata['n'] = 0
                # empty buffer - we know we're going to empty it and still pull
                # short
                self.curdata['b'] = 0
                # fulfilled daily requirements now = accepted records
                self.fulfillment = self.curdata['a']
        
        self.last_buffer=self.curdata['b']
        self.curdata['z'] += self.curdata['b']
        self.curdata['b'] = self.curdata['n']
        self.curdata['n'] = 0
        self.curdata['f'] = self.fulfillment
        """
        mk=""
        for x in range(0,int(self.dailyreq/10)):
            if(x<self.fulfillment/10):
                mk+="#"
            else:
                mk+="-"
        print("D" ,  self.proc_count ,"\tFULFILLING:", self.fulfillment, "\t"+mk)
        """
        self.hdata.append(self.curdata.copy())
        print(
            f"Day {self.proc_count}: Records in: {old_n}, \tAccepted: {self.curdata['a']},    \tBuffered: {self.curdata['b']} (total buffer size 110), \tSurplus released: {self.last_buffer}\t(Cumulative:{self.curdata['z']})")

    def overnight(self, onamount):
        self.proc_count += 1
        self.curdata['i'] = "O/N " + str(self.proc_count)
        self.curdata['n'] += onamount
        self.curdata['a'] -= self.fulfillment
        self.curdata['f'] = '-'
        self.hdata.append(self.curdata.copy())

    def coll_setup_1(self):
        client = MongoClient('localhost', 27017)
        self.db = client["test_data_001"]
        self.collection = self.db.td001

    def mgr(self, lodef, hidef):

        self.coll_setup_1()
        self.new_lodef = lodef
        self.new_hidef = hidef
        # self.setup()
        dataline = {
            'id': 88776655,
            "key": "mykey",
            "value": "myvalue"
        }
        self.collection.insert_one(dataline)
        for x in range(0, 9):
            self.overnight(self.getRandomNew())
            self.process()
        self.printHdata()
        # print("\n\n=========================================================================================================================================\n\n")


class DoThingsWithDate():

    def __init__(self):
        self.localNow = datetime.now()

    def get_python_daydate_from_antrix_daydate(self, antrixdaydate):
        objTimestamp = datetime.strptime(antrixdaydate, "%m/%d/%Y %H:%M")
        return objTimestamp

    def get_antrix_daydate_from_python_daydate(self, pythondate):
        strdate1 = pythondate.strftime("%m/%d/%Y %H:%M")
        strdate1 = str(strdate1)
        strdate1 = strdate1[:16]
        return strdate1

    def get_real_python_daydate_now(self):
        date1 = datetime.now()
        return date1

    def get_real_antrix_daydate_now(self):
        date1 = self.get_real_python_daydate_now()
        strdate1 = str(date1)
        strdate1 = strdate1[:16]
        return strdate1

    def test_is_between_start_and_end__antrix_datetime(self, test, start, end):
        s = self.get_python_daydate_from_antrix_daydate(start)  # start
        e = self.get_python_daydate_from_antrix_daydate(end)  # end
        t = self.get_python_daydate_from_antrix_daydate(test)  # test
        return self.test_is_between_start_and_end__python_datetime(t, s, e)

    def test_is_between_start_and_end__python_datetime(self, objTest, objStart, objEnd):

        startToEnd = objEnd - objStart
        testToEnd = objEnd - objTest

        """
        BIZARRE TIMEDELTAS:
        
        print(type(objStart))
        print(type(objEnd))
        print(type(objTest))

        print(type(startToEnd))
        print(type(testToEnd))

        print(startToEnd)
        print(testToEnd)        
                
        If the Test delta is the same day but longer
        than the End-Start delta, the TestToEnd comes out
        as something like -1 day, 23:59:00.
        The only good test seems to be to convert to a string and
        look for a - sign at the start.
        
        In any other case, it is only necessary to test that 
        test-to-end is less than start-to-end
        """
        if(str(testToEnd)[0:1] == "-"):
            return False
        else:
            if(testToEnd <= startToEnd):
                return True
            else:
                return False

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

    def get_random_time_stamp_python(self, s, e):
        strS = self.get_antrix_daydate_from_python_daydate(s)
        strE = self.get_antrix_daydate_from_python_daydate(e)
        return(self.get_random_time_stamp_antrix(strS, strE))

    def get_random_time_stamp_antrix(self, s, e):

        #date1 = datetime.datetime.now()
        #date2 = date1 - datetime.timedelta(hours=12)
        #strdate1 = date1.strftime("%m/%d/%Y %H:%M")
        #strdate2 = date2.strftime("%m/%d/%Y %H:%M")
        return self.randomDate(s, e, random.random())


if __name__ == '__main__':
    mld = make_longterm_data()
    die(0)

    twd = DoThingsWithDate()
    # print(twd.get_real_antrix_daydate_now())

    s = ("01/09/2019 10:00")  # start
    e = ("01/11/2019 10:00")  # end
    t = ("01/10/2019 10:00")  # test

    v = twd.test_is_between_start_and_end__antrix_datetime(t, s, e)
    print(v)

    print(twd.get_random_time_stamp_antrix(s, e))
    i = 0
