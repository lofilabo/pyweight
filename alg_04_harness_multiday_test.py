import pprint
import sys
import datetime
from pyweight.alg_02_makeBrokerData import MakeBrokerData
from pyweight.alg_02_makeBrokerData import Timestamps
from pyweight.alg_01_processBrokerData import BrokerData
from pyweight.alg_05_audit_records import AuditRecords
from pyweight.alg_06_endOfDay_audit import EndOfDay
from pyweight.alg_07_tidy_all_old_buffers import TidyOldBuffers


class Harness():

    def __init__(self):

        self.startday = 1
        self.endday = 5

        self.mbd = MakeBrokerData()
        self.bd = BrokerData()
        #self.ar = AuditRecords()
        self.tob = TidyOldBuffers()
        self.newRecordsForEachDay = {}

        #self.recordcount = 100
        self.windback = 24

        self.weighting = {
            "p1": 17,
            "p2": 17,
            "p3": 17,
            "p4": 17,
            "p5": 16,
            "p6": 16
        }

        self.recordcountlist = [
            800000,
            800000,
            1600000,
            3600,
            80000,
            80000,
            80000,
        ]

        self.dailycaplist = [
            12000,
            11000,
            30000,
            100,
            220,
            220,
            220,
        ]

        self.run()

    def run(self):

        self.mbd.clearDownDB_YN = True
        for x in range(self.startday, self.endday):
            self.dailycap = self.dailycaplist[x - 1]
            self.recordcount = self.recordcountlist[x - 1]
            #self.bd.dailycap = self.dailycap
            self.mbd.objTs.windback = self.windback
            self.mbd.set_recordcount(self.recordcount)
            day_of_month = x
            self.strLocalNowDate = "04/" + \
                str(day_of_month).zfill(2) + "/2019 23:59"

            if(self.mbd.clearDownDB_YN == False):
                self.tob.run(self.strLocalNowDate)
            self.regular_cycle(self.dailycaplist[x - 1])
            self.mbd.clearDownDB_YN = False

            ld = self.strLocalNowDate[:10]

            eod = EndOfDay(ld, self.newRecordsForEachDay[ld])
            eod.run()
        for y in range(1, 6):
            self.strLocalNowDate = "04/" + \
                str(day_of_month + y).zfill(2) + "/2019 23:59"

            self.tob.run(self.strLocalNowDate)

        # self.ar.retroactively_update_surplusAll()
        # self.ar.printResults()

    def regular_cycle(self, dailycap):

        #dailycap = 10
        clientname = ""
        campaignname = ""

        self.mbd.localNow = datetime.datetime.strptime(
            self.strLocalNowDate, "%m/%d/%Y %H:%M")

        self.bd.localNow = datetime.datetime.strptime(
            self.strLocalNowDate, "%m/%d/%Y %H:%M")

        self.mbd.run()

        # self.ar.run_withOneDate(self.strLocalNowDate[:10])
        self.bd.setup()
        #self.bd.dailycap = self.dailycap

        self.bd.buffercount = 0
        self.bd.buffer_cap_count = 0
        self.bd.buffer_cap_max_size = 0

        self.bd.run('cli1', 'cmp1', self.weighting, dailycap)

        self.newRecordsForEachDay.update(
            {self.strLocalNowDate[:10]: self.bd.start_of_process_new_record_count})

        # self.ar.run_withOneDate(self.strLocalNowDate[:10])
        self.bd.dailycap = self.dailycap


if __name__ == '__main__':
    h = Harness()
