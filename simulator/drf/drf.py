import sys
import numpy as np
import matplotlib.pyplot as plt
import time

from random import random
from copy import deepcopy

FIGS_DIR = "./figs/"

R = [] # Total resource capabilities
C = [] # Consumed resources, initially 0
s = [] # Each user's dominant resource shares, initially 0
U = [] # Resources given to user i, initially 0
D = [] # Demand vector of of user i

""" DRF algorithm """
def drf():
    global R, C, s, U, D

    min_dom_rsrc = sys.maxint
    min_i = 0
    is_full = False # Cluster's condition

    for i in range(len(s)):
        if s[i] < min_dom_rsrc:
            min_dom_rsrc = s[i]
            min_i = i

            if s[i] == 0:
                break

    i = 0
    for d_rsrc, c_rsrc, r_rsrc in zip(D[min_i], C, R):
        i += 1
        if d_rsrc + c_rsrc > r_rsrc:
            is_full = True
            break

    if not is_full:
        for i in range(len(R)):
            C[i] += D[min_i][i]
            U[min_i][i] += D[min_i][i]

            rsrc = float(U[min_i][i])/R[i]
            if rsrc > s[min_i]:
                s[min_i] = rsrc

    return not is_full # There is a change in the cluster

"""
Reset cluster allocation for some users.
@percent: Percentage of current cluster consumption to be rescheduled
"""
def reset(percent):
    global R, C, s, U, D

    _U = []
    users = range(len(s))

    for _ in range(len(R)):
        _U.append(0)

    for i in range(int(percent * len(s))):
        user_id = int(random() * (len(users) - 1))

        for i in range(len(R)):
            C[i] -= U[user_id][i]
        U[user_id] = deepcopy(_U)
        s[user_id] = 0

        del users[user_id]

def init(rsrc, num_users):
    global R, C, s, U, D

    R = rsrc
    C = []
    s = []
    U = []
    D = []
    _U = []
    _D = []

    for _ in range(len(rsrc)):
        _U.append(0)
        C.append(0)

    for i in range(num_users):
        _D = []
        for r in range(len(rsrc)):
            _D += [int(9 * random()) + 1]

        U.append(deepcopy(_U))
        D.append(_D)
        s.append(0)

def plot(name, num_users, times):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    plt.errorbar(num_users, [np.average(arr) for arr in times],
        yerr=[np.std(arr) for arr in times])

    # ax.set_title(name + " w/ Varied Routing")
    ax.legend()
    ax.set_xlabel("Number of users")
    ax.set_ylabel("Time (ms)")

    plt.savefig(FIGS_DIR + name + ".png", format="png")
    plt.clf()

def add_users(num_users):
    global R, s, U, D

    _U = []
    _D = []

    for r in range(len(R)):
        _U.append(0)

    for i in range(num_users):
        _D = []
        for r in range(len(R)):
            _D += [int(9 * random()) + 1]

        U.append(deepcopy(_U))
        D.append(_D)
        s.append(0)

def main():
    global R, C, s, U, D

    # Static case
    # num_users = range(100, 1001, 100)
    # times = []

    # for num_user in num_users:
    #     _times = []
    #     for i in range(0, 10):
    #         change = True
    #         init([100000, 100000], num_user)

    #         start = time.clock()
    #         while change:
    #             change = drf()
    #         diff = (time.clock() - start) * 1000 # Milliseconds

    #         _times.append(diff)
    #     times.append(_times)

    # plot("static", num_users, times)

    # Dynamic case
    num_users = range(100, 1001, 100)

    C_list = []
    s_list = []
    U_list = []
    inc_C_list = []
    inc_s_list = []
    inc_U_list = []

    for num_user in num_users:
        for i in range(1, 5):
            change = True
            init([10000, 10000], num_user)

            while change:
                change = drf()

            num_new_user = 10

            for _ in range(1, 10):
                reset(float(i)/10)
                add_users(num_new_user)

                change = True
                while change:
                    change = drf()

                inc_C = deepcopy(C)
                inc_s = deepcopy(s)
                inc_U = deepcopy(U)

                inc_C_list.append(inc_C)
                inc_s_list.append(inc_s)
                inc_U_list.append(inc_U)

                reset(1)
                change = True
                while change:
                    change = drf()

                C_list.append(C)
                s_list.append(s)
                U_list.append(U)

                print "inc_C: ", inc_C
                print "C: ", C

                C = deepcopy(inc_C)
                s = deepcopy(inc_s)
                U = deepcopy(inc_U)

if __name__ == '__main__':
    main()
