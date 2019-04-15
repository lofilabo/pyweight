import pprint
import sys
import datetime
from pkg1.alg_01_processBrokerData import BrokerData
from pkg1.alg_03_make_longterm_data import DoThingsWithDate
#from pkg1.alg_05_audit_records import AuditRecords
#from pkg1.alg_06_endOfDay_audit import EndOfDay
from pkg1.alg_07_tidy_all_old_buffers import TidyOldBuffers


class Entry(object):

    def run(self, clientname='', campaignname='', weighting=0, dailycap=0):

        if(weighting == 0):
            self.weighting = {
                "p1": 17,
                "p2": 17,
                "p3": 17,
                "p4": 17,
                "p5": 16,
                "p6": 16
            }

        if(dailycap == 0):
            dailycap = 10

        if(clientname == ''):
            clientname = "cli1"

        if(campaignname == ''):
            campaignname = "cmp1"

        #strLocalNowDate = "04/04/2019 23:39"
        strLocalNowDate = self.dtf.get_real_antrix_daydate_now()

        pdate = self.dtf.get_python_daydate_from_antrix_daydate(
            strLocalNowDate)
        #pdate = datetime.datetime.now()
        # datetime.datetime.strptime(strLocalNowDate, "%m/%d/%Y %H:%M")
        self.bd.localNow = pdate
        self.bd.setup()
        self.bd.run(clientname, campaignname, self.weighting, dailycap)
        self.bd.dailycap = dailycap
        self.tob.run(strLocalNowDate)

    def __init__(self, clientname='', campaignname='', weighting=0, dailycap=0):
        i = 0
        self.bd = BrokerData()
        self.tob = TidyOldBuffers()
        self.dtf = DoThingsWithDate()
        self.run(clientname, campaignname, weighting, dailycap)


if __name__ == '__main__':
    e = Entry("cli1", "cmp1", 0, 10)
