"""
Implement the HEFT algorithm.
This script refers the code from https://github.com/mrocklin/heft/.

(function ---> job, edge server ---> agent)

    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""
import pandas as pd
import numpy as np
import random
from collections import namedtuple
from functools import partial
from itertools import chain
from embedding.scenario import bar, para
from embedding.utils import reverse_dict
from embedding.parameters import *


def get_agents():
    """
    Agents start from zero.
    """
    servers = [str(n) for n in range(para.get_server_num())]
    return ''.join(servers)


all_agents = get_agents()
Event = namedtuple('Event', 'job start end')


class HEFT:
    def __init__(self, G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream):
        # get the generated edge computing scenario
        self.G, self.bw, self.pp = G, bw, pp
        self.simple_paths, self.reciprocals_list, self.proportions_list = simple_paths, reciprocals_list, proportions_list
        # get the generated functions' requirements
        self.pp_required, self.data_stream = pp_required, data_stream

    def get_response_time(self, sorted_DAG_path=SORTED_DAG_PATH):
        """
        Calculate the overall finish time of all DAGs achieved by HEFT algorithm.
        """
        if not os.path.exists(sorted_DAG_path):
            print('DAGs\' topological order has not been obtained! Please get topological order firstly.')
            return

        df = pd.read_csv(sorted_DAG_path)
        df_len = df.shape[0]
        idx = 0

        makespan_of_all_DAGs = 0
        DAGs_deploy = []
        DAGs_orders = []

        required_num = REQUIRED_NUM
        all_DAG_num = sum(required_num)
        calculated_num = 0
        print('\nGetting makespan for %d DAGs by HEFT algorithm...' % all_DAG_num)
        while idx < df_len:
            # get a DAG
            DAG_name = df.loc[idx, 'job_name']
            DAG_len = 0
            while (idx + DAG_len < df_len) and (df.loc[idx + DAG_len, 'job_name'] == DAG_name):
                DAG_len = DAG_len + 1
            DAG = df.loc[idx: idx + DAG_len]
            DAG_pp_required = self.pp_required[:DAG_len]
            DAG_data_stream = self.data_stream[:DAG_len]

            # get the information of the DAG
            funcs_num = HEFT.get_funcs_num(DAG, idx, DAG_len)
            succ = HEFT.parse_DAG_structure(DAG, idx, DAG_len)
            comp_cost_array = self.get_comp_cost(funcs_num, DAG_pp_required)
            comm_cost_array = self.get_comm_cost(succ, DAG_data_stream)

            # schedule for this DAG
            orders, jobson, makespan = HEFT.schedule(succ, all_agents,
                                                     HEFT.compcost, comp_cost_array,
                                                     HEFT.commcost, comm_cost_array)

            makespan_of_all_DAGs += makespan
            DAGs_deploy.append(jobson)
            DAGs_orders.append(orders)

            calculated_num += 1
            percent = calculated_num / float(all_DAG_num) * 100
            # for overflow
            if percent > 100:
                percent = 100
            bar.update(percent)
            idx += DAG_len

        print('The overall makespan achieved by HEFT: %f second' % makespan_of_all_DAGs)
        print('The average makespan: %f second' % (makespan_of_all_DAGs / sum(REQUIRED_NUM)))
        return DAGs_orders, DAGs_deploy

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
        """
        Get a DAG structure from the dataset. For example:
        for DAG "
        M1,12846.0,j_3,1,Terminated,157213,157295,100.0,0.3
        R2_1,371.0,j_3,1,Terminated,157297,157322,100.0,0.49
        R3,371.0,j_3,1,Terminated,157297,157325,100.0,0.49
        M4,1.0,j_3,1,Terminated,157322,157328,100.0,0.39
        R5,1.0,j_3,1,Terminated,157326,157330,100.0,0.39
        M6,1.0,j_3,1,Terminated,157326,157330,100.0,0.39
        M7,1.0,j_3,1,Terminated,157326,157330,100.0,0.39
        J8_6_7,1111.0,j_3,1,Terminated,157329,157376,100.0,0.59
        R9,1.0,j_3,1,Terminated,157376,157381,100.0,0.39
        J10_8_9,1111.0,j_3,1,Terminated,157331,157376,100.0,0.59
        R11_5_10,1.0,j_3,1,Terminated,157376,157381,100.0,0.39
        R12_4_11,1.0,j_3,1,Terminated,157376,157381,100.0,0.39
        R13_2_3_12,1.0,j_3,1,Terminated,157376,157381,100.0,0.39
        R14_13,1.0,j_3,1,Terminated,157376,157381,100.0,0.39
        ", the output is
        {1: (2,),
         2: (13,),
         3: (13,),
         4: (12,),
         5: (11,),
         6: (8,),
         7: (8,),
         8: (10,),
         9: (10,),
         10: (11,),
         11: (12,),
         12: (13,),
         13: (14,),
         14: ()}.
        """
        succ_funcs = [[] for _ in range(DAG_len)]
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
        succ = dict()
        for i in range(DAG_len):
            succ[i + 1] = tuple(succ_funcs[i])
        del succ_funcs
        return succ

    def get_comp_cost(self, funcs_num, DAG_pp_required):
        """
        Get computation cost of each function on each server for a given DAG.
        """
        comp_cost_array = np.zeros((len(funcs_num) + 1, para.get_server_num()))
        for i in range(len(funcs_num)):
            func_num = funcs_num[i]
            comp_cost_array[func_num] = DAG_pp_required[func_num - 1] / self.pp
        return comp_cost_array

    @staticmethod
    def compcost(job, agent, comp_cost_array):
        a = int(agent)
        return comp_cost_array[job][a]

    @staticmethod
    def wbar(ni, agents, compcost, comp_cost_array):
        """
        Average computation cost.
        """
        return sum(compcost(ni, agent, comp_cost_array) for agent in agents) / len(agents)

    def get_comm_cost(self, succ, DAG_data_stream):
        """
        Get the data transmission cost between any two servers for a given DAG.
        """
        # fix the path chosen between any two node
        fix_path_reciprocals = np.zeros((para.get_server_num(), para.get_server_num()))
        for n1 in range(para.get_server_num()):
            for n2 in range(para.get_server_num()):
                if n1 != n2:
                    paths_num = len(self.reciprocals_list[n1][n2])
                    chosen_path = random.randint(0, paths_num - 1)
                    fix_path_reciprocals[n1][n2] = self.reciprocals_list[n1][n2][chosen_path]

        comm_cost_array = []
        for dependent_func_num, funcs_num in succ.items():
            if funcs_num is ():
                pass
            else:
                trans_size = DAG_data_stream[dependent_func_num - 1]
                trans_cost = np.zeros((para.get_server_num(), para.get_server_num()))
                for n1 in range(para.get_server_num()):
                    for n2 in range(para.get_server_num()):
                        if n1 != n2:
                            trans_cost[n1][n2] = trans_size * fix_path_reciprocals[n1][n2]
                comm_cost_array.append([dependent_func_num, funcs_num, trans_cost])
        del fix_path_reciprocals
        return comm_cost_array

    @staticmethod
    def commcost(ni, nj, A, B, comm_cost_array):
        """
        Get the data transmission cost from ni to nj when ni is placed on A and nj is placed on B.
        """
        a1 = int(A)
        a2 = int(B)
        for d in range(len(comm_cost_array)):
            if ni == comm_cost_array[d][0]:
                funcs_num = comm_cost_array[d][1]
                for f in range(len(funcs_num)):
                    if nj == funcs_num[f]:
                        return comm_cost_array[d][2][a1][a2]
        return 0.

    @staticmethod
    def cbar(ni, nj, agents, commcost, comm_cost_array):
        """
        Average communication cost.
        """
        n = len(agents)
        if n == 1:
            return 0
        n_pairs = para.get_n_pairs()
        return 1. * sum(commcost(ni, nj, a1, a2, comm_cost_array) for a1 in agents for a2 in agents
                        if a1 != a2) / n_pairs

    @staticmethod
    def ranku(ni, agents, succ, compcost, commcost, comp_cost_array, comm_cost_array):
        """
        Rank of job.
        This code is designed to mirror the wikipedia entry.
        Please see http://en.wikipedia.org/wiki/Heterogeneous_Earliest_Finish_Time for details.
        """
        rank = partial(HEFT.ranku, compcost=compcost, commcost=commcost,
                       succ=succ, agents=agents, comp_cost_array=comp_cost_array, comm_cost_array=comm_cost_array)
        w = partial(HEFT.wbar, agents=agents, compcost=compcost, comp_cost_array=comp_cost_array)
        c = partial(HEFT.cbar, agents=agents, commcost=commcost, comm_cost_array=comm_cost_array)

        if ni in succ and succ[ni]:
            return w(ni) + max(c(ni, nj) + rank(nj) for nj in succ[ni])
        else:
            return w(ni)

    @staticmethod
    def endtime(job, events):
        """
        Endtime of job in list of events.
        """
        for e in events:
            if e.job == job:
                return e.end

    @staticmethod
    def find_first_gap(agent_orders, desired_start_time, duration):
        """
        Find the first gap in an agent's list of jobs. The gap must be after `desired_start_time`
        and of length at least `duration`.
        """
        # No jobs: can fit it in whenever the job is ready to run
        if (agent_orders is None) or (len(agent_orders)) == 0:
            return desired_start_time

        # Try to fit it in between each pair of Events, but first prepend a
        # dummy Event which ends at time 0 to check for gaps before any real
        # Event starts.
        a = chain([Event(None, None, 0)], agent_orders[:-1])
        for e1, e2 in zip(a, agent_orders):
            earliest_start = max(desired_start_time, e1.end)
            if e2.start - earliest_start > duration:
                return earliest_start

        # No gaps found: put it at the end, or whenever the task is ready
        return max(agent_orders[-1].end, desired_start_time)

    @staticmethod
    def start_time(agent, job, orders, jobson, prec, commcost, comm_cost_array, compcost, comp_cost_array):
        """
        Earliest time that job can be executed on agent.
        """
        duration = compcost(job, agent, comp_cost_array)

        if job in prec:
            comm_ready = max([HEFT.endtime(p, orders[jobson[p]])
                              + commcost(p, job, agent, jobson[p], comm_cost_array) for p in prec[job]])
        else:
            comm_ready = 0

        return HEFT.find_first_gap(orders[agent], comm_ready, duration)

    @staticmethod
    def allocate(job, orders, jobson, prec, commcost, comm_cost_array, compcost, comp_cost_array):
        """
        Allocate job to the machine with earliest finish time. Operates in place.
        """
        st = partial(HEFT.start_time, job=job, orders=orders, jobson=jobson, prec=prec,
                     commcost=commcost, comm_cost_array=comm_cost_array,
                     compcost=compcost, comp_cost_array=comp_cost_array)
        # ft = lambda machine: st(machine) + compcost(job, machine)
        def ft(machine): return st(machine) + compcost(job, machine, comp_cost_array)

        agent = min(orders.keys(), key=ft)
        start = st(agent)
        end = ft(agent)

        orders[agent].append(Event(job, start, end))
        orders[agent] = sorted(orders[agent], key=lambda e: e.start)
        # Might be better to use a different data structure to keep each
        # agent's orders sorted at a lower cost.

        jobson[job] = agent

    @staticmethod
    def makespan(orders):
        """
        Finish time of last job.
        """
        return max(v[-1].end for v in orders.values() if v)

    @staticmethod
    def schedule(succ, agents, compcost, comp_cost_array, commcost, comm_cost_array):
        """
        Schedule computation dag onto worker agents.
        inputs:
        succ - DAG of tasks {a: (b, c)} where b, and c follow a
        agents - set of agents that can perform work
        compcost - function :: job, agent -> runtime
        commcost - function :: j1, j2, a1, a2 -> communication time
        """
        rank = partial(HEFT.ranku, agents=agents, succ=succ,
                       compcost=compcost, comp_cost_array=comp_cost_array,
                       commcost=commcost, comm_cost_array=comm_cost_array)
        prec = reverse_dict(succ)

        jobs = set(succ.keys()) | set(x for xx in succ.values() for x in xx)
        jobs = sorted(jobs, key=rank)

        orders = {agent: [] for agent in agents}
        jobson = dict()
        for job in reversed(jobs):
            HEFT.allocate(job, orders, jobson, prec, commcost, comm_cost_array, compcost, comp_cost_array)

        return orders, jobson, HEFT.makespan(orders)

    @staticmethod
    def recvs(job, jobson, prec, recv):
        """
        Collect all necessary recvs for job.
        """
        if job not in prec:
            return []
        return [recv(jobson[p], jobson[job], p, job) for p in prec[job]
                if jobson[p] != jobson[job]]

    @staticmethod
    def sends(job, jobson, succ, send):
        """
        Collect all necessary sends for job.
        """
        if job not in succ:
            return []
        return [send(jobson[job], jobson[s], job, s) for s in succ[job]
                if jobson[s] != jobson[job]]

    @staticmethod
    def insert_recvs(order, jobson, prec, recv):
        if not order:
            return order

        this_agent = jobson[order[0].job]

        receives = partial(HEFT.recvs, jobson=jobson, prec=prec, recv=recv)
        recv_events = {e.job: [Event(r, e.start, e.start)
                               for r in receives(e.job)]
                       for e in order}

        for job, revents in recv_events.items():
            i = [e.job for e in order].index(job)
            order = order[:i] + revents + order[i:]

        jobson.update({e.job: this_agent for es in recv_events.values() for e in es})

        return order

    @staticmethod
    def insert_sends(order, jobson, succ, send):
        if not order:
            return order

        this_agent = jobson[order[0].job]

        sends2 = partial(HEFT.sends, jobson=jobson, succ=succ, send=send)
        send_events = {e.job: [Event(s, e.end, e.end)
                               for s in sends2(e.job)]
                       for e in order}

        for job, sevents in send_events.items():
            i = [e.job for e in order].index(job)
            order = order[:i + 1] + sevents + order[i + 1:]

        jobson.update({e.job: this_agent for es in send_events.values() for e in es})

        return order

    @staticmethod
    def insert_sendrecvs(orders, jobson, succ, send, recv):
        """
        Insert send an recv events into the orders at appropriate places.
        """
        prec = reverse_dict(succ)
        jobson = jobson.copy()
        new_orders = dict()
        for agent, order in orders.items():
            order = HEFT.insert_sends(order, jobson, succ, send)
            order = HEFT.insert_recvs(order, jobson, prec, recv)
            new_orders[agent] = order
        return new_orders, jobson
