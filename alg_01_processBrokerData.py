import pprint
import sys
#from pkg1.alg_02_makeBrokerData import MakeBrokerData
#from pkg1.alg_02_makeBrokerData import Timestamps
#from alg_02_makeBrokerData import MakeBrokerData
#from alg_02_makeBrokerData import Timestamps
from datetime import datetime
#from orca.orca import die
from pymongo import MongoClient
import bson


class BrokerData():

    def __init__(self):

        self.approvedStatus = "accepted"
        self.bufferedStatus = "buffered"
        self.newStatus = 'new'
        self.archStatus = 'archive'
        self.dataDump = False
        self.start_of_process_new_record_count = 0

    def setup(self):

        self.generated_records_by_partners = {
            "p1": 0,
            "p2": 0,
            "p3": 0,
            "p4": 0,
            "p5": 0,
            "p6": 0
        }

        self.surplus_by_partners = {
            "p1": 0,
            "p2": 0,
            "p3": 0,
            "p4": 0,
            "p5": 0,
            "p6": 0
        }

        self.shortfall_by_partners = {
            "p1": 0,
            "p2": 0,
            "p3": 0,
            "p4": 0,
            "p5": 0,
            "p6": 0
        }

        self.cap_by_partners = {
            "p1": 0,
            "p2": 0,
            "p3": 0,
            "p4": 0,
            "p5": 0,
            "p6": 0
        }

        self.obtained_by_partners = {
            "p1": 0,
            "p2": 0,
            "p3": 0,
            "p4": 0,
            "p5": 0,
            "p6": 0
        }
        self.iteration = 1
        self.newcount = 0
        #self.buffercount = 0
        self.total_new_recovered_shortfall = 0
        self.backwards_time_limit = 1  # start off with 24 hours
        self.shortfallcount = 0
        self.dailycap = 0
        self.records = []
        self.dailycap = 0
        self.shortfall = 0
        self.surplus = 0
        self.total_reassignment_count = 0
        self.initdailycap = 0

    def load_surplus(self):
        for k, v in self.weighting_by_partners.items():
            self.surplus_by_partners[k] = self.initial_records_by_partners[k]

    def calculate_requirements(self):
        for key, value in self.cap_by_partners.items():
            curcap = int(
                (self.weighting_by_partners[key] / 100) * self.dailycap)
            self.cap_by_partners[key] = curcap

    def obtain_available_records(self):
        for key, value in self.cap_by_partners.items():

            if(self.surplus_by_partners[key] >= value):
                obtained = value
            else:
                obtained = (self.surplus_by_partners[key])

            surplus = self.surplus_by_partners[key] - value
            if(surplus < 0):
                ssurplus = 0
            else:
                ssurplus = surplus

            shortfall = value - obtained

            if (self.dataDump == True):
                print('  trying to get ', value,
                      ' records from ', key,
                      ", took", obtained,
                      " out of", self.surplus_by_partners[key],
                      "available, shortfall of:", shortfall,
                      ", surplus of:", ssurplus
                      )

            self.obtained_by_partners[key] += obtained

            #print("Obtained:" , obtained)

            """
            IMPORTANT.
            This line is the only line which wires the 'numbers only' highest abstraction with the
            'records in memory' 2-nd-level abstraction.
            
            At the moment, it requires only key (the partner name) and Obtained 
            (the number of records which the high-level abstraction thinks we need.
            
            Effectively, it says "get [obtained] records where partner = key"
            
            Because there is nothing special about each individual record, we don't 
            even care about which records of the subset [partner x, status y] we get.
            """
            self.get_and_process_records_by_partner(key, obtained)

            if(surplus >= 0):
                self.surplus_by_partners[key] = surplus
                self.shortfall_by_partners[key] = 0
            else:
                self.surplus_by_partners[key] = 0
                self.shortfall_by_partners[key] = (surplus * -1)

    def test_within_today(self, dt, timeWindowSelect):
        difference = self.localNow - dt
        # pprint.pprint(difference.seconds)

        tSecs = {"24hours": 1,
                 "48hours": 2

                 }

        if difference.days < tSecs[timeWindowSelect]:
            return (True)
        else:
            return (False)

    def test_within_x_seconds(self, dt, x):
        difference = self.localNow - dt

        if difference.days < x:
            #print(x,difference.seconds, True)
            return (True)
        else:
            #print(x,difference.seconds, False)
            return (False)

    def get_and_process_records_by_partner(self, partner, recordcount):
        #print(partner, recordcount)
        localcount = 0
        for record in self.records:
            objTimestamp = datetime.strptime(
                record['timestamp'], "%m/%d/%Y %H:%M")

            if (localcount >= recordcount):
                return

            withinTimeTF = self.test_within_today(objTimestamp, "24hours")
            #print(record['timestamp'] , objTimestamp, self.test_within_today(objTimestamp) , "\n")
            """
            NOTE:
            We'll keep 2 versions of the if... statement around.
            This one is for historical purposes:
            if(record['partner'] == partner and record['status'] == 'new' and withinTimeTF == True):
            
            If the record is flagged as New, we already know it's less than 24 (or whatever) hours old.
            We only need to check STATUS, not TIME.
            """
            if(record['partner'] == partner and record['status'] == 'new'):  # and withinTimeTF == True):
                # if(record['partner'] == partner and record['status'] == self.newStatus):
                #print(record['timestamp'] , objTimestamp, self.test_within_today(objTimestamp) , "\n")
                record['status'] = self.approvedStatus
                record['status_changed'] = self.localNow.strftime(
                    "%m/%d/%Y %H:%M")
                localcount += 1
                self.total_reassignment_count += 1

    def tidy_and_get_outstanding_records(self):
        """
        At the end of the process, we might be 3 or 4 records short of the amount we need...
        """
        end_total = 0
        for record in self.records:
            if(record['status'] == self.approvedStatus):
                end_total += 1

        calc_shortfall = self.initdailycap - end_total
        if (self.dataDump == True):
            print("| Shortfall to be reassigned: \t  ", calc_shortfall, "\t\t |")

        localcount = 0
        for record in self.records:
            if (localcount >= calc_shortfall):
                return

            """
            We can use any number of New records from anywhere, but only buffer records from the 
            available window.
            This window requires some tinkering.  Possibly.
            """

            if(record['status'] == self.newStatus):
                record['status'] = self.approvedStatus
                localcount += 1
                self.total_reassignment_count += 1
                self.obtained_by_partners[record['partner']] += 1

        for record in self.records:
            if (localcount >= calc_shortfall):
                return

            objTimestamp = datetime.strptime(
                record['timestamp'], "%m/%d/%Y %H:%M")

            """
            A BIG CHANGE on MON 8th, 1700.
            DRAW THE RECORDS TO MAKE UP THE calc_shortfall
            FROM today's archive, NOT today's Buffer.
            The buffer has already been set, and its size should not change.
            But the 'overflow-from-the-archive', marked as Today, is available.
            """
            withinTimeTF = self.test_within_today(objTimestamp, "24hours")
            if(record['status'] == self.archStatus and withinTimeTF == True):
                record['status'] = self.approvedStatus

                """
                D A N G E R O U S ! !
                Do we change the timestamp on this record from Yesterday........?????????
                """
                # print("LOCALNOW___________________________________________________________________>>>>>>>>>>>>>>>>>>",self.localNow)
                # Right now...maybe not
                #record['timestamp'] = self.localNow.strftime("%m/%d/%Y %H:%M")
                record['status_changed'] = self.localNow.strftime(
                    "%m/%d/%Y %H:%M")
                localcount += 1
                self.total_reassignment_count += 1
                self.obtained_by_partners[record['partner']] += 1

        for record in self.records:
            if (localcount >= calc_shortfall):
                return

            objTimestamp = datetime.strptime(
                record['timestamp'], "%m/%d/%Y %H:%M")

            """
            And now, and only now, are we allowed to plunder the Buffer!!
            This step will only be arrived at if:
            i. There is a Rounding shortfall.
            ii. It cannot be made up by ransacking the Archive for today.
            """
            withinTimeTF = self.test_within_today(objTimestamp, "48hours")
            if(record['status'] == self.bufferedStatus and withinTimeTF == True):
                record['status'] = self.approvedStatus
                record['status_changed'] = self.localNow.strftime(
                    "%m/%d/%Y %H:%M")
                localcount += 1
                self.total_reassignment_count += 1
                self.obtained_by_partners[record['partner']] += 1

    def calculate_total_shortfall(self):
        sf = 0
        for key, value in self.shortfall_by_partners.items():
            sf = sf + value
        self.shortfall = sf

    def calculate_total_surplus(self):
        sp = 0
        for key, value in self.surplus_by_partners.items():
            sp = sp + value
        self.surplus = sp

    def recalculate_weighting(self):

        # use the Surplus dict as mask to set each item in
        # weighting to either 0 or a modified value.
        # Put another way, an item which is already exhausted cannot meaningfully
        # be assigned a Weight,

        totalwbp = 0
        for key, value in self.surplus_by_partners.items():
            # print(value)
            if(value == 0):
                self.weighting_by_partners[key] = 0

            totalwbp = totalwbp + self.weighting_by_partners[key]

        if(totalwbp <= 0):
            modification_index = 100
        else:
            modification_index = 100 / totalwbp
        for key, value in self.weighting_by_partners.items():
            self.weighting_by_partners[key] = (self.weighting_by_partners[key] *
                                               modification_index)
        if (self.dataDump == True):
            print('New Weight- Partners:', self.weighting_by_partners)

        self.mgr()

    def tidy_final_surplus(self):

        if (self.dataDump == True):
            print("\n/------------{FINAL SUMMARY}---------------------\\")
            print("|BUFFER ITEMS:                             \t |")

        if (self.dataDump == True):
            for key, value in self.surplus_by_partners.items():
                print("|\t", key, "has ", value,
                      " \titems to be buffered \t |")

        for record in self.records:
            if(record['status'] == self.newStatus):
                if(self.bufferSizeExceeded() == False and self.alreadyUsingBuffer == False):
                    record['status'] = self.bufferedStatus
                    self.buffercount += 1
                    self.buffer_cap_count += 1
                else:
                    record['status'] = self.archStatus
                record['status_changed'] = self.localNow.strftime(
                    "%m/%d/%Y %H:%M")

    def print_final_obtained(self):

        if (self.dataDump == True):
            print("\\ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  /")
            print("/                                            \t \\")
            print("|OBTAINED ITEMS:                             \t |")

            for key, value in self.obtained_by_partners.items():
                print("|\t", key, "donated ", value, " \titems       \t |")

            for record in self.records:
                if(record['status'] == self.newStatus):
                    if(self.bufferSizeExceeded() == False and self.alreadyUsingBuffer == False):
                        print("Making Buffer Item: ", record['timestamp'])
                        record['status'] = self.bufferedStatus
                        self.buffercount += 1
                        self.buffer_cap_count += 1
                    else:
                        record['status'] = self.archStatus
                    record['status_changed'] = self.localNow.strftime(
                        "%m/%d/%Y %H:%M")

    def calculate_total_available(self):
        total = 0
        for k, v in self.initial_records_by_partners.items():
            total = total + v

        if (self.dataDump == True):
            print("Total Records Available", total)

        if(total >= self.dailycap):
            return True
        else:
            return False

    def countRecordsByPartner(self):
        for record in self.records:
            for k, v in self.initial_records_by_partners.items():
                #print(k, record['partner'])
                if(k == record['partner']):
                    # print(record['partner'])
                    self.generated_records_by_partners[k] = self.generated_records_by_partners[k] + 1

    def print_records(self):
        if (self.dataDump == True):
            for record in self.records:
                print(record)

    def affirmStatus(self):
        """
        loop through the records, if a certain record is older than (whatever),
        assign its status to 'buffer' ; else assign to new.
        """
        localcount = 0
        for record in self.records:
            objTimestamp = datetime.strptime(
                record['timestamp'], "%m/%d/%Y %H:%M")

            #withinTimeTF = self.test_within_today(objTimestamp)
            withinTimeTF = self.test_within_x_seconds(
                objTimestamp, self.backwards_time_limit)
            #print(record['timestamp'] , objTimestamp, self.test_within_today(objTimestamp) , "\n")
            if(withinTimeTF == True):
                #print(record['timestamp'] , objTimestamp, self.test_within_today(objTimestamp) , "\n")
                record['status'] = "new"
                self.newcount += 1
                i = 1
            else:
                if(self.bufferSizeExceeded() == False and self.alreadyUsingBuffer == False and record['status'] == self.newStatus):
                    #print("Making Buffer Item(2): ", record['timestamp'] )
                    record['status'] = self.bufferedStatus
                    record['status_changed'] = self.localNow.strftime(
                        "%m/%d/%Y %H:%M")
                    self.buffercount += 1
                    self.buffer_cap_count += 1
        # self.print_records()

    def bufferSizeExceeded(self):
        # return False
        if(self.buffer_cap_count >= (self.buffer_cap_max_size)):
            #print ("BUFFER EXCEEDED")
            return True
        else:
            return False

    def handle_reassignment_to_make_up_numbers(self):
        """
        Called if we don't have enough New records.
        Pick some buffered records, reassign their status to New
        """

        if(self.newcount < self.initdailycap):
            """
            We need a flag to say we're eating the buffer already.
            """
            self.alreadyUsingBuffer = True

            self.total_new_recovered_shortfall = self.initdailycap - self.newcount

            for record in self.records:
                objTimestamp = datetime.strptime(
                    record['timestamp'], "%m/%d/%Y %H:%M")

                withinTimeTF = self.test_within_x_seconds(
                    objTimestamp, self.backwards_time_limit)
                # print(record['timestamp'], objTimestamp,
                #      self.test_within_today(objTimestamp), "\n")
                if(record['status'] == self.bufferedStatus):
                    if(self.shortfallcount < self.total_new_recovered_shortfall):
                        # print(self.shortfallcount)
                        record['status'] = self.approvedStatus
                        record['status_changed'] = self.localNow.strftime(
                            "%m/%d/%Y %H:%M")

                        self.shortfallcount += 1

                        # print(    "*******A SUITABLE RECORD WAS FOUND*********",
                        # self.shortfallcount,
                        # self.total_new_recovered_shortfall, objTimestamp,
                        # withinTimeTF)

                        if(self.shortfallcount >= self.total_new_recovered_shortfall):
                            if (self.dataDump == True):
                                print(
                                    "\n*****************RETURNING!!*****************")
                                print("******After Recovering Buffer Of:",
                                      self.shortfallcount, "*******")
                                print(
                                    "*********************************************\n")
                            return

            #print("RECALLING:: localcount-reassigned records:", self.shortfallcount)
            #self.backwards_time_limit += (1)
            # self.handle_reassignment_to_make_up_numbers()

        else:
            if (self.dataDump == True):
                print(
                    "\n*************************************************************************")
                print("**** SUFFICIENT NEW RECORDS(", self.newcount,
                      "records ): NO REASSIGNMENT REQUIRED ****")
                print(
                    "*************************************************************************\n")

    def mgr(self):
        if (self.dataDump == True):
            print("\n--------------------------------------------------------")
            print("|                   ITERATION No.",
                  self.iteration, "                   |")
            print("--------------------------------------------------------")

        self.iteration += 1

        if (self.dataDump == True):
            print('Daily Cap:              ', self.dailycap)
            print('Cap By Partners:        ', self.cap_by_partners)
            print('Weight By Partners:     ', self.weighting_by_partners)

        self.calculate_requirements()

        if (self.dataDump == True):
            print('Available By Partners:    ', self.surplus_by_partners)

        self.obtain_available_records()

        if (self.dataDump == True):
            print('Surplus By Partners:    ', self.surplus_by_partners)
            print('Shortfall By Partners:  ', self.shortfall_by_partners)

        self.calculate_total_shortfall()
        self.calculate_total_surplus()

        if (self.dataDump == True):
            print("shortfall:              ", self.shortfall)
            print("surplus:                ", self.surplus)
            print('Weight By Partners:  ', self.weighting_by_partners)

        if(self.shortfall > 0):
            self.dailycap = self.shortfall
            self.recalculate_weighting()
        else:
            self.tidy_final_surplus()
            return

    def tidy_buffer(self):
        """
        This is now taken care of by alg_07.
        But keep it around for a while....
        """

        return
        """
        All buffer records older than 48 hours need to be changed to 
        ARCHIVE status.
        """
        archCount = 0
        for record in self.records:
            objTimestamp = datetime.strptime(
                record['timestamp'], "%m/%d/%Y %H:%M")
            withinTimeTF = self.test_within_today(objTimestamp, "48hours")

            if(record['status'] == self.bufferedStatus):
                if(withinTimeTF == False):
                    #record['status'] = self.archStatus
                    #record['status_changed'] = self.localNow.strftime("%m/%d/%Y %H:%M")
                    #archCount += 1
                    j = 1

        if(archCount > 0):
            if (self.dataDump == True):
                print("/                                                \\")
                print("|-------------ARCHIVED:", archCount,
                      "RECORDS----------------|")
                print("\\________________________________________________/")

    def update_database_with_internal_records(self):
        #rez = self.collection.update_many({'indice':0, 'thread_id': {'$in': self.records}}, {'$set': {'updated':'yes'}})
        # pprint.pprint(rez)
        for record in self.records:
            # print(record)
            myquery = {"_id": record['_id']}
            newvalues = {"$set": record}
            self.collection.update_one(myquery, newvalues)

    def coll_setup_1(self):
        client = MongoClient('localhost', 27017)
        self.db = client["test_broker"]
        self.collection = self.db.brokerdata

    def run(self, clientname, campaignname, weightings, dailycap):
        self.alreadyUsingBuffer = False
        self.buffercount = 0
        self.buffer_cap_count = 0
        self.buffer_cap_max_size = 0

        self.initial_records_by_partners = self.generated_records_by_partners
        self.clientname = clientname
        self.campaignname = campaignname
        self.weighting_by_partners = weightings.copy()
        self.dailycap = dailycap

        #print("DAILYCAP:" , dailycap, "BUFFER:",int(dailycap/10))
        self.buffer_cap_max_size = int(dailycap / 10)
        # print(self.buffer_cap_max_size)
        """
        print("This is the name of the script: ", sys.argv[0])
        print("Number of arguments: ", len(sys.argv))
        print("The arguments are: ", str(sys.argv))
        """
        self.coll_setup_1()

        if (len(sys.argv) > 1):
            """
            Change on 04/04.  Do Not harness record-generation to this 
            set of operations.  In future, drive them all from a manager.
            """
            #self.bd = MakeBrokerData()
            self.dailycap = int(sys.argv[1])
            # self.records =
            # self.print_records()
        else:
            """NO CL ARGUMENTS"""
            #self.bd = MakeBrokerData(3000)
            # if(self.dailycap ==0):
            #    self.dailycap = 100
        """
        Print out all the records.
        """
        # self.print_records()

        self.dailycap = dailycap
        self.records = []

        strdate1 = self.localNow.strftime("%m/%d/%Y %H:%M")
        regx = bson.regex.Regex(strdate1[:10] + '*')

        self.mongoCursorNew = self.collection.find(
            {'client': self.clientname, 'campaign': self.campaignname, 'status': 'new'})
        start_of_process_new_record_count = self.mongoCursorNew.count()
        self.start_of_process_new_record_count = start_of_process_new_record_count

        """
        WHY 2 VERSIONS?
        v1 considers ALL the records, and 'tops up' the buffer when the older parts are expired out.
        v2. considers only Today, and re-makes the buffer each day.
        
        Note that v1 looks at a LOT more data.
        
        BUT!! v2 DOES NOT CONTAIN THE BUFFER RECORDS!!
        So, use v1 for now...
        """
        self.mongoCursor = self.collection.find({'client': self.clientname,
                                                 'campaign': self.campaignname,
                                                 'status': {'$ne': self.archStatus},
                                                 'status': {'$ne': self.approvedStatus}})
        #self.mongoCursor = self.collection.find({'timestamp': regx})

        for post in self.mongoCursor:
            # print(post)

            post['status_changed'] = post['timestamp']

            if(post['status'] != self.approvedStatus and post['status'] != self.archStatus):
                self.records.append(post)
        lsr = len(self.records)
        if(lsr < self.dailycap):
            #print("NOT ENOUGH RECORDS.  DO SOMETHING")
            self.dailycap = len(self.records)
            print("ALERT.  NOT ENOUGH RECORDS AVAILABLE. DAILYCAP SET TO:", self.dailycap)

            if(self.dailycap == 0):
                die(0)

        self.initdailycap = self.dailycap
        self.affirmStatus()
        self.handle_reassignment_to_make_up_numbers()
        # self.print_records()
        # return
        """
        Print out all the records.
        """
        # self.print_records()

        # pprint.pprint(bd.records)
        # print(len(self.records))

        #print('Hardcoded Records by Partner: ',self.initial_records_by_partners)

        # WHAT'S HAPENNING HERE?
        # we are copying the 'generated records' distribution matrix on top of the pretend one we set up.
        # to go over the worked example, COMMENT THIS LINE ONLY.

        # print(self.initial_records_by_partners)

        self.countRecordsByPartner()

        if (self.dataDump == True):
            print('Generated records by Partner: ',
                  self.generated_records_by_partners)

        if(self.calculate_total_available()):
            self.load_surplus()
            self.mgr()
        else:
            if (self.dataDump == True):
                print("not enough records.")

        if (self.dataDump == True):
            if (self.dataDump == True):
                print("| Reassignment Count (before tidy)",
                      self.total_reassignment_count, "\t\t |")

        """
        ~NOTE- after redesigning the 'calculate total shortfall' method, this
        tidy-up function should NOT be necessary....
        But we'll leave it in for a bit, until we can verfiy that 
            Reassignment Count (before tidy)
            Reassignment Count (after tidy)
        are the same thing.
        ~NOTE 2.  The tidy-up function turns out to be very necessary.  Leave it alone
        and don't try to be clever.
        """
        self.tidy_and_get_outstanding_records()
        if (self.dataDump == True):
            print("| Reassignment Count (after tidy) ",
                  self.total_reassignment_count, "\t\t |")

        self.print_final_obtained()

        if (self.dataDump == True):
            print("\\________________________________________________/")

        # pprint.pprint(self.obtained_by_partners)

        """
        Print out all the records.
        """
        # self.print_records()
        self.tidy_buffer()
        self.update_database_with_internal_records()


if __name__ == '__main__':

    weighting = {
        "p1": 20,
        "p2": 30,
        "p3": 0,
        "p4": 0,
        "p5": 0,
        "p6": 20
    }

    dailycap = 10
    clientname = ""
    campaignname = ""

    main = BrokerData()
    #main.dailycap = 1000
    main.localNow = datetime.strptime("04/04/2019 13:00", "%m/%d/%Y %H:%M")
    main.setup()
    main.run(clientname, campaignname, weighting, dailycap)
