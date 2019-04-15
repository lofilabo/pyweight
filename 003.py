import pprint
from pygments.lexers.esoteric import CAmkESLexer

dailycap = 1000
shortfall = 0
surplus = 0


weighting_by_partners = {
    "p1": 20,
    "p2": 30,
    "p3": 10,
    "p4": 10,
    "p5": 10,
    "p6": 20
}

initial_records_by_partners = {
    "p1": 100,
    "p2": 200,
    "p3": 300,
    "p4": 400,
    "p5": 500,
    "p6": 600
}

surplus_by_partners = {
    "p1": 0,
    "p2": 0,
    "p3": 0,
    "p4": 0,
    "p5": 0,
    "p6": 0
}

shortfall_by_partners = {
    "p1": 0,
    "p2": 0,
    "p3": 0,
    "p4": 0,
    "p5": 0,
    "p6": 0
}

cap_by_partners = {
    "p1": 0,
    "p2": 0,
    "p3": 0,
    "p4": 0,
    "p5": 0,
    "p6": 0
}


def load_surplus():
    for k, v in weighting_by_partners.items():
        surplus_by_partners[k] = initial_records_by_partners[k]


def calculate_requirements():
    for key, value in cap_by_partners.items():
        curcap = int((weighting_by_partners[key] / 100) * dailycap)
        cap_by_partners[key] = curcap


def obtain_available_records():
    for key, value in cap_by_partners.items():

        if(surplus_by_partners[key] >= value):
            obtained = value
        else:
            obtained = (surplus_by_partners[key])

        surplus = surplus_by_partners[key] - value
        print('trying to get ', value, ' records from ', key,
              ", got ", obtained, " out of ", surplus_by_partners[key])

        # print(surplus)

        if(surplus >= 0):
            surplus_by_partners[key] = surplus
            shortfall_by_partners[key] = 0
        else:
            surplus_by_partners[key] = 0
            shortfall_by_partners[key] = (surplus * -1)


def calculate_total_shortfall():
    global shortfall
    sf = 0
    for key, value in shortfall_by_partners.items():
        sf = sf + value
    shortfall = sf


def calculate_total_surplus():
    global surplus
    sp = 0
    for key, value in surplus_by_partners.items():
        sp = sp + value
    surplus = sp


def recalculate_weighting():

    # use the Surplus dict as mask to set each item in
    # weighting to either 0 or a modified value.

    totalwbp = 0
    for key, value in surplus_by_partners.items():
        # print(value)
        if(value == 0):
            weighting_by_partners[key] = 0

        totalwbp = totalwbp + weighting_by_partners[key]

    modification_index = 100 / totalwbp
    for key, value in weighting_by_partners.items():
        weighting_by_partners[key] = weighting_by_partners[key] * \
            modification_index
    print('New Weight- Partners:', weighting_by_partners)
    mgr()


def tidy_final_surplus():
    for key, value in surplus_by_partners.items():
        print(key, "has ", value, " items to be buffered")


def calculate_total_available():
    total = 0
    for k, v in initial_records_by_partners.items():
        total = total + v

    print("Total Records Available", total)
    if(total >= dailycap):
        return True
    else:
        return False


def mgr():
    global dailycap

    print('Daily Cap:              ', dailycap)
    print('Cap By Partners:        ', cap_by_partners)
    calculate_requirements()

    print('Available By Partners:    ', surplus_by_partners)
    obtain_available_records()
    print('Surplus By Partners:    ', surplus_by_partners)
    print('Shortfall By Partners:  ', shortfall_by_partners)
    calculate_total_shortfall()
    calculate_total_surplus()
    print("shortfall:              ", shortfall)
    print("surplus:                ", surplus)
    print('Weight By Partners:  ', weighting_by_partners)

    if(shortfall > 0):
        dailycap = shortfall
        recalculate_weighting()
    else:
        tidy_final_surplus()
        return


if __name__ == '__main__':

    if(calculate_total_available()):
        load_surplus()
        mgr()
    else:
        print("not enough records.")
