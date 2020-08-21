"""
Implement the DPE algorithm.

    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""
import numpy as np
import pandas as pd
from embedding.parameters import *
from embedding.scenario import bar, para


class DPE:
    def __init__(self, G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream):
        # get the generated edge computing scenario
        self.G, self.bw, self.pp = G, bw, pp
        self.simple_paths, self.reciprocals_list, self.proportions_list = simple_paths, reciprocals_list, proportions_list
        # get the generated functions' requirements
        self.pp_required, self.data_stream = pp_required, data_stream

    def get_response_time(self, sorted_DAG_path=SORTED_DAG_PATH):
        """
        Calculate the overall finish time of all DAGs achieved by DPE algorithm.
        """
        if not os.path.exists(sorted_DAG_path):
            print('DAGs\' topological order has not been obtained! Please get topological order firstly.')
            return

        df = pd.read_csv(sorted_DAG_path)
        df_len = df.shape[0]
        idx = 0

        makespan_of_all_DAGs = 0
        DAGs_deploy = []
        T_optimal_all = []
        start_time_all = []
        process_sequence_all = []

        required_num = REQUIRED_NUM
        all_DAG_num = sum(required_num)
        calculated_num = 0
        print('\nGetting makespan for %d DAGs by DPE algorithm...' % all_DAG_num)
        while idx < df_len:
            # get a DAG
            DAG_name = df.loc[idx, 'job_name']
            DAG_len = 0
            while (idx + DAG_len < df_len) and (df.loc[idx + DAG_len, 'job_name'] == DAG_name):
                DAG_len = DAG_len + 1
            DAG = df.loc[idx: idx + DAG_len]
            DAG_pp_required = self.pp_required[:DAG_len]
            DAG_data_stream = self.data_stream[:DAG_len]

            # T_optimal stores the earliest finish time of each function on each server
            T_optimal = np.zeros((DAG_len, para.get_server_num()))
            start_time = np.zeros(DAG_len)
            funcs_deploy = -1 * np.ones(DAG_len)
            process_sequence = []
            # server_runtime records the moment when the newest func on each server is finished
            server_runtime = np.zeros(para.get_server_num())

            makespan = 0
            for j in range(DAG_len + 1):
                if j == DAG_len:
                    # this is the dummy tail function, update all the exit functions' deployment and return the makespan
                    # makespan is the slowest 'exit function's earliest finish time'
                    for e in range(DAG_len):
                        if funcs_deploy[e] == -1.:
                            funcs_deploy[e] = int(np.argmin(T_optimal[e]))
                            process_sequence.append(e + 1)
                            if min(T_optimal[e]) > makespan:
                                makespan = min(T_optimal[e])
                    break

                # get the number of this function and stores in func
                name_str_list = DAG.loc[j + idx, 'task_name'].strip().split('_')
                name_str_list_len = len(name_str_list)
                func_str_len = len(name_str_list[0])
                func_num = int(name_str_list[0][1:func_str_len])

                if name_str_list_len == 1:
                    # func is an entry function
                    pass
                else:
                    # func is not an entry function, func has dependencies
                    # enumerate the deployment of func
                    for n in range(para.get_server_num()):
                        # get t(p(f_j)) where p(f_j) is n
                        process_cost = DAG_pp_required[func_num - 1] / self.pp[n]
                        all_min_phi = []
                        for i in range(name_str_list_len - 1):
                            if name_str_list[i + 1] == '':
                                continue
                            dependent_func_num = int(name_str_list[i + 1])

                            if funcs_deploy[dependent_func_num - 1] != -1.:
                                # dependent_func_num has been deployed beforehand, get min_phi directly
                                # ==== DIR_PATH is where we can improved (maybe in the next paper) ====
                                # For example, for DAG 'M2, R4_2 and R5_2', M2's placement is decided by R4 if we
                                # process (M2, R4) firstly. R5 will not affect the placement of M2. However, we don't
                                # know that if we process (M2, R5) firstly, whether the makespan can be decreased further.
                                # =================================================================
                                where_deployed = int(funcs_deploy[dependent_func_num - 1])
                                if n == funcs_deploy[dependent_func_num - 1]:
                                    trans_cost = 0
                                else:
                                    trans_cost = self.proportions_list[where_deployed][n] * \
                                                 DAG_data_stream[dependent_func_num - 1] * \
                                                 self.reciprocals_list[where_deployed][n][0]
                                min_phi = T_optimal[dependent_func_num - 1][where_deployed] + trans_cost + process_cost
                                all_min_phi.append(min_phi)
                                continue

                            for h in range(DAG_len):
                                name_str_list_inner = DAG.loc[h + idx, 'task_name'].strip().split('_')
                                func_str_inner_len = len(name_str_list_inner[0])
                                if int(name_str_list_inner[0][1:func_str_inner_len]) != dependent_func_num:
                                    continue
                                else:
                                    # dependent_func_num is found
                                    name_str_list_inner_len = len(name_str_list_inner)
                                    if name_str_list_inner_len == 1:
                                        # dependent_func_num is an entry function. Set its T_optimal
                                        T_optimal[dependent_func_num - 1] = \
                                            DAG_pp_required[dependent_func_num - 1] / self.pp + server_runtime
                                    else:
                                        # although T_optimal of dependent_func_num has been set, but it has to be
                                        # updated because server_runtime may changed!!!
                                        process_begin_time = np.zeros(para.get_server_num())
                                        for k in range(para.get_server_num()):
                                            min_process_begin_time = 0
                                            # dependent_func_num is deployed on k
                                            for h_inner in range(name_str_list_inner_len - 1):
                                                # dependent_func_num's one predecessor and its deployment
                                                if name_str_list_inner[h_inner + 1] == '':
                                                    continue
                                                dependent_func_num_predecessor = int(name_str_list_inner[h_inner + 1])
                                                where_deployed_predecessor = int(funcs_deploy[dependent_func_num_predecessor - 1])
                                                if where_deployed_predecessor == -1.:
                                                    print('Sth. wrong! It\'s impossible!')
                                                if k == where_deployed_predecessor:
                                                    trans_cost = 0
                                                else:
                                                    trans_cost = self.proportions_list[where_deployed_predecessor][k] * \
                                                                 DAG_data_stream[dependent_func_num - 1] * \
                                                                 self.reciprocals_list[where_deployed_predecessor][k][0]
                                                tmp = T_optimal[dependent_func_num_predecessor - 1][where_deployed_predecessor] + trans_cost
                                                # the process of dependent_func_num can be started if and only if the slowest predecessor of it has finished data transfer
                                                if tmp > min_process_begin_time:
                                                    min_process_begin_time = tmp
                                            if min_process_begin_time > server_runtime[k]:
                                                process_begin_time[k] = min_process_begin_time
                                            else:
                                                process_begin_time[k] = server_runtime[k]

                                        T_optimal[dependent_func_num - 1] = \
                                            DAG_pp_required[dependent_func_num - 1] / self.pp + process_begin_time
                                    break

                            # decide the optimal deployment for dependent_func_num
                            min_phi = MAX_VALUE
                            selected_server = -1
                            for m in range(para.get_server_num()):
                                if n == m:
                                    trans_cost = 0
                                else:
                                    trans_cost = self.proportions_list[m][n] * \
                                                 DAG_data_stream[dependent_func_num - 1] * \
                                                 self.reciprocals_list[m][n][0]
                                phi = T_optimal[dependent_func_num - 1][m] + trans_cost + process_cost
                                if phi < min_phi:
                                    min_phi = phi
                                    selected_server = m

                            # this is where a function really be deployed
                            funcs_deploy[dependent_func_num - 1] = selected_server
                            process_sequence.append(dependent_func_num)
                            server_runtime[selected_server] = T_optimal[dependent_func_num - 1][selected_server]
                            start_time[dependent_func_num - 1] = server_runtime[selected_server] - DAG_pp_required[
                                dependent_func_num - 1] / self.pp[selected_server]
                            all_min_phi.append(min_phi)

                        # now, all the predecessors of func has been deployed, use their T_optimal to update T_optimal of func
                        T_optimal[func_num - 1][n] = max(all_min_phi)

            makespan_of_all_DAGs += makespan
            DAGs_deploy.append(funcs_deploy)
            process_sequence_all.append(process_sequence)
            T_optimal_all.append(T_optimal)
            start_time_all.append(start_time)

            calculated_num += 1
            percent = calculated_num / float(all_DAG_num) * 100
            # for overflow
            if percent > 100:
                percent = 100
            bar.update(percent)
            idx += DAG_len
        print('The overall makespan achieved by DPE: %f second' % makespan_of_all_DAGs)
        print('The average makespan: %f second' % (makespan_of_all_DAGs / sum(REQUIRED_NUM)))
        return T_optimal_all, DAGs_deploy, process_sequence_all, start_time_all
