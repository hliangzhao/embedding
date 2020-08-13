"""
Implement the HEFT algorithm.

    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""

import pprint
import numpy as np
import pandas as pd
import random
from itertools import chain
from collections import namedtuple
from functools import partial
from embedding.parameters import *
# from embedding.scenario import bar, para
from embedding.scenario import *
from embedding.utils import reverse_dict, find_func_event


Event = namedtuple('Event', 'function start end')


class HEFT:
    def __init__(self, G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream):
        # get the generated edge computing scenario
        self.G, self.bw, self.pp = G, bw, pp
        self.simple_paths, self.reciprocals_list, self.proportions_list = simple_paths, reciprocals_list, proportions_list
        # get the generated functions' requirements
        self.pp_required, self.data_stream = pp_required, data_stream

    @staticmethod
    def get_funcs_num(DAG, idx, DAG_len):
        """
        Get each function's number sequentially for the given DAG.
        """
        funcs_num = []
        for i in range(DAG_len):
            name_str_list = DAG.loc[i + idx, 'task_name'].strip().split('_')
            func_str_len = len(name_str_list[0])
            func_num = int(name_str_list[0][1:func_str_len])
            funcs_num.append(func_num)
        return funcs_num

    @staticmethod
    def parse_DAG_structure(DAG, idx, DAG_len):
        succ_funcs = [[] for n in range(DAG_len)]
        for j in range(DAG_len):
            name_str_list = DAG.loc[j + idx, 'task_name'].strip().split('_')
            name_str_list_len = len(name_str_list)
            func_str_len = len(name_str_list[0])
            func_num = int(name_str_list[0][1:func_str_len])
            if name_str_list_len == 1:
                pass
            else:
                for i in range(name_str_list_len - 1):
                    if name_str_list[i + 1] == '':
                        continue
                    dependent_func_num = int(name_str_list[i + 1])
                    succ_funcs[dependent_func_num - 1].append(func_num)
        succ_funcs_dict = dict()
        for i in range(DAG_len):
            succ_funcs_dict[i + 1] = tuple(succ_funcs[i])
        del succ_funcs
        return succ_funcs_dict

    def get_comp_cost(self, funcs_num, DAG_pp_required):
        """
        Get computation cost of each function on each server for a given DAG.
        """
        comp_cost = np.zeros((len(funcs_num), para.get_server_num()))
        for i in range(len(funcs_num)):
            func_num = funcs_num[i]
            comp_cost[i] = DAG_pp_required[func_num - 1] / self.pp
        return comp_cost

    def get_aver_comp_cost(self, funcs_num, DAG_pp_required):
        """
        Get average computation cost of each function for a given DAG.
        """
        aver_comp_cost = np.zeros(len(funcs_num))
        for i in range(len(funcs_num)):
            aver_comp_cost[i] = np.mean(self.get_comp_cost(funcs_num, DAG_pp_required)[i])
        return aver_comp_cost

    def get_comm_cost(self, funcs_num, DAG_data_stream):
        """
        Get the data transmission cost between any two servers for a given DAG.
        """
        # comm_cost = []
        # fix_path_reciprocals = np.zeros((para.get_server_num(), para.get_server_num()))
        # for n1 in range(para.get_server_num()):
        #     for n2 in range(para.get_server_num()):
        #         if n1 != n2:
        #             paths_num = len(self.reciprocals_list[n1][n2])
        #             chosen_path = random.randint(0, paths_num - 1)
        #             fix_path_reciprocals[n1][n2] = self.reciprocals_list[n1][n2][chosen_path]
        #
        # for i in range(len(funcs_num)):
        #     comm_cost_i = np.zeros((para.get_server_num(), para.get_server_num()))
        #     func_num = funcs_num[i]
        #     for n1 in range(para.get_server_num()):
        #         for n2 in range(para.get_server_num()):
        #             comm_cost_i[n1][n2] = DAG_data_stream[func_num - 1] * fix_path_reciprocals[n1][n2]
        #     comm_cost.append(comm_cost_i)
        #
        # del fix_path_reciprocals
        # return comm_cost
        pass

    def get_aver_comm_cost(self, funcs_num, DAG_data_stream):
        """
        Get average transmission cost of each function for a given DAG.
        """
        # aver_comm_cost = np.zeros(len(funcs_num))
        # comm_cost = self.get_comm_cost(funcs_num, DAG_data_stream)
        # for i in range(len(funcs_num)):
        #     aver_comm_cost[i] = sum(comm_cost[i][n1][n2]
        #                             for n1 in range(para.get_server_num())
        #                             for n2 in range(para.get_server_num())) / para.get_n_pairs()
        # return aver_comm_cost
        pass


if __name__ == '__main__':
    G, bw, pp = generate_scenario()
    simple_paths = get_simple_paths(G)
    reciprocals_list, proportions_list = get_ratio(simple_paths, bw)
    pp_required, data_stream = set_funcs()

    heft = HEFT(G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream)

    df = pd.read_csv(TEST_DAG_PATH)
    df_len = df.shape[0]
    idx = 0

    while idx < df_len:
        DAG_name = df.loc[idx, 'job_name']
        DAG_len = 0
        while (idx + DAG_len < df_len) and (df.loc[idx + DAG_len, 'job_name'] == DAG_name):
            DAG_len = DAG_len + 1
        DAG = df.loc[idx: idx + DAG_len]
        DAG_pp_required = heft.pp_required[:DAG_len]
        DAG_data_stream = heft.data_stream[:DAG_len]

        funcs_num = HEFT.get_funcs_num(DAG, idx, DAG_len)
        print(heft.get_comp_cost(funcs_num, DAG_pp_required))
        print(heft.get_aver_comp_cost(funcs_num, DAG_pp_required))
        pprint.pprint(heft.get_comm_cost(funcs_num, DAG_data_stream))
        print(heft.get_aver_comm_cost(funcs_num, DAG_data_stream))

        idx += DAG_len
