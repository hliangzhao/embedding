"""
Print the scheduling result for the appointed DAG.

    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""


import pprint
from embedding.scenario import para


def print_scheduling_results(T_optimal_all, DAGs_deploy, process_sequence_all, DAG_num):
    """
    Print the scheduling results of the given DAG.
    """
    DAG_deploy = DAGs_deploy[DAG_num]
    T_optimal = T_optimal_all[DAG_num]
    process_sequence = process_sequence_all[DAG_num]

    schedules = [[] for n in range(para.get_server_num())]
    for func in process_sequence:
        chosen_server = int(DAG_deploy[func - 1])
        pair = {'f' + str(func): T_optimal[func - 1][chosen_server]}
        schedules[chosen_server].append(pair)
    schedules_dict = {}
    for n in range(para.get_server_num()):
        schedules_dict['server ' + str(n + 1)] = schedules[n]
    print('\nThe finish time of each function on the chosen server for DAG #%d:' % DAG_num)
    pprint.pprint(schedules_dict)


def print_Gantt():
    pass
