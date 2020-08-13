"""
This script processes the Alibaba cluster trace dataset (v2018). v2018 has 6 CSV files. We only use the file
batch_task.csv.

- Firstly, we sample 2119 DAGs from the dataset and save them into selected_DAGs.csv.
- Then, we sort the functions of each DAG in topological order (used for DPE and FixDoc) and save the results to
    topological_order.csv.
- At last, we 'rank' the functions of each DAG (used for HEFT) and save the results to rank.scv.

(task --> function, job --> DAG)

    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""
import os
import numpy as np
import pandas as pd
from embedding.utils import ProgressBar
from embedding.parameters import *


bar = ProgressBar()


def sample_DAG(dataset_path=DATASET_PATH, selected_data_path=SELECTED_DAG_PATH):
    """
    Select 200 DAGs with number of functions being 2;
    800 DAGs with number of functions between 3 and 10;
    600 DAGs with number of functions between 11 and 50;
    400 DAGs with number of functions between 51 and 100;
    119 DAGs with functions more than 100.

    The selected DAGs are saved in selected_DAGs.csv.
    """
    if os.path.exists(selected_data_path):
        print('DAGs have been sampled!')
        return

    if not os.path.exists(dataset_path):
        print('batch_task.csv is not exist! Please download this file from '
              'http://clusterdata2018pubcn.oss-cn-beijing.aliyuncs.com/batch_task.tar.gz '
              'and uncompress it into the dataset dir.')
        return

    # download online and get data (rewrite)
    df = pd.read_csv(dataset_path, header=None)
    df.columns = ['task_name', 'instance_num', 'job_name', 'task_type', 'status',
                  'start_time', 'end_time', 'plan_cpu', 'plan_mem']
    print(df.describe)

    df_len = df.shape[0]
    required_num = REQUIRED_NUM
    counters = np.zeros(5)
    idx = 0

    DAGs = df.loc[0: 0]
    print('DAGs are sampling ...')
    while idx < df_len:
        task_name = df.loc[idx, 'task_name']
        if task_name.find('task_') != -1:
            idx = idx + 1
            continue

        DAG_name = df.loc[idx, 'job_name']
        DAG_len = 0
        while (idx + DAG_len < df_len) and (df.loc[idx + DAG_len, 'job_name'] == DAG_name):
            DAG_len = DAG_len + 1
        if DAG_len == 2:
            if counters[0] < required_num[0]:
                DAGs = pd.concat([DAGs, df.loc[idx: idx + DAG_len - 1].copy()], axis=0)
                counters[0] = counters[0] + 1
        elif 3 <= DAG_len <= 10:
            if counters[1] < required_num[1]:
                DAGs = pd.concat([DAGs, df.loc[idx: idx + DAG_len - 1].copy()], axis=0)
                counters[1] = counters[1] + 1
        elif 11 <= DAG_len <= 50:
            if counters[2] < required_num[2]:
                DAGs = pd.concat([DAGs, df.loc[idx: idx + DAG_len - 1].copy()], axis=0)
                counters[2] = counters[2] + 1
        elif 51 <= DAG_len <= 100:
            if counters[3] < required_num[3]:
                DAGs = pd.concat([DAGs, df.loc[idx: idx + DAG_len - 1].copy()], axis=0)
                counters[3] = counters[3] + 1
        elif DAG_len > 100:
            if counters[4] < required_num[4]:
                DAGs = pd.concat([DAGs, df.loc[idx: idx + DAG_len - 1].copy()], axis=0)
                counters[4] = counters[4] + 1
        idx = idx + DAG_len

        percent = sum(counters) / float(sum(required_num)) * 100
        bar.update(percent)

        if (sum(counters) == all):
            break

    DAGs.to_csv(selected_data_path, index=0)


def get_topological_order(selected_DAG_path=SELECTED_DAG_PATH, sorted_DAG_path=SORTED_DAG_PATH):
    """
    Get the topoligical ordering of each DAG, sabe the results into the file topological_order.csv.
    """
    if os.path.exists(sorted_DAG_path):
        print('DAGs\' topological order has been obtained!')
        return

    if not os.path.exists(selected_DAG_path):
        print('The sampling procedure has not been executed! Please sampling DAGs firstly.')
        return

    df = pd.read_csv(selected_DAG_path)
    df_len = df.shape[0]
    idx = 0

    required_num = REQUIRED_NUM
    all_DAG_num = sum(required_num)
    sorted_num = 0

    print('Getting topological order for %d DAGs...' % all_DAG_num)
    while idx < df_len:
        # get a DAG
        DAG_name = df.loc[idx, 'job_name']
        DAG_len = 0
        while (idx + DAG_len < df_len) and (df.loc[idx + DAG_len, 'job_name'] == DAG_name):
            DAG_len = DAG_len + 1
        DAG = df.loc[idx: idx + DAG_len].copy()

        # get the number and dependencies of each function of the DAG
        funcs_num = np.zeros(DAG_len)
        dependencies = [[] * 1] * DAG_len
        for i in range(DAG_len):
            name_str_list = DAG.loc[i + idx, 'task_name'].split('_')
            name_str_list_len = len(name_str_list)
            func_str_len = len(name_str_list[0])
            func_num = int(name_str_list[0][1:func_str_len])
            dependent_funcs = []
            for j in range(name_str_list_len):
                if j == 0:
                    # the func itself
                    continue
                if name_str_list[j].isnumeric():
                    # the func's dependencies
                    dependent_func_num = int(name_str_list[j])
                    dependent_funcs.append(dependent_func_num)
            funcs_num[i] = func_num
            dependencies[i] = dependent_funcs

        # sort the functions accroding to their dependencies
        funcs_left = DAG_len
        DAG_sorted = DAG.copy()
        while funcs_left > 0:
            # find a source func, and place the funcs who depend on it after this source func
            # the topological ordering we take is actually a Depth-first Search algorithm
            # as a result, the entry functions may not have the smallest number

            # ==== this is where we can improved ====
            # Use Breadth-first Search algorithm to optain the topological ordering and compare the results.
            # The makespan might be decreased further.
            # =======================================
            for i in range(len(dependencies)):
                if len(dependencies[i]) == 0:
                    running_func = i
                    dependencies[i].append(-1)
                    break
            func_running = int(funcs_num[running_func])
            for i in range(len(dependencies)):
                if dependencies[i].count(func_running) > 0:
                    dependencies[i].remove(func_running)
            DAG_sorted.loc[DAG_len - funcs_left + idx] = DAG.loc[running_func + idx].copy()
            funcs_left = funcs_left - 1
        df.loc[idx: idx + DAG_len - 1] = DAG_sorted.copy()
        idx = idx + DAG_len
        sorted_num = sorted_num + 1
        percent = sorted_num / float(all_DAG_num) * 100
        # for overflow
        if percent > 100:
            percent = 100
        bar.update(percent)

    df.to_csv(sorted_DAG_path, index=0)
