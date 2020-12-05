"""
Step 1: Get the topological order of DAGs.
Step 2: Generate the scenario.
Step 3: Run the three algorithms and compare the results.

    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""
from embedding.dataset_processing import sample_DAG, get_topological_order
from embedding.scenario import *
from embedding.algos.dpe import DPE
from embedding.algos.fixdoc import FixDoc
from embedding.algos.heft import HEFT
from embedding.algos.interpretate_result import *
from embedding.parameters import *
import datetime


if __name__ == '__main__':
    print('------------------------ Step 1 ------------------------')
    sample_DAG()
    get_topological_order()

    print('\n\n------------------------ Step 2 ------------------------')
    G, bw, pp = generate_scenario()
    print_scenario(G, bw, pp)
    simple_paths = get_simple_paths(G)
    print_simple_paths(simple_paths)
    reciprocals_list, proportions_list = get_ratio(simple_paths, bw)
    pp_required, data_stream = set_funcs()

    print('\n\n------------------------ Step 3 ------------------------')
    dpe = DPE(G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream)
    start = datetime.datetime.now()
    T_optimal_all_dpe, DAGs_deploy_dpe, process_sequence_all_dpe, start_time_all_dpe = dpe.get_response_time(sorted_DAG_path=SORTED_DAG_PATH)
    end = datetime.datetime.now()
    print('Computer\'s running time:', (end - start).seconds, 'seconds')
    print_scheduling_results(T_optimal_all_dpe, DAGs_deploy_dpe, process_sequence_all_dpe, start_time_all_dpe, 2010)

    fixdoc = FixDoc(G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream)
    start = datetime.datetime.now()
    T_optimal_all_fixdoc, DAGs_deploy_fixdoc, process_sequence_all_fixdoc, start_time_all_fixdoc = fixdoc.get_response_time(sorted_DAG_path=SORTED_DAG_PATH)
    end = datetime.datetime.now()
    print('Computer\'s running time:', (end - start).seconds, 'seconds')
    print_scheduling_results(T_optimal_all_fixdoc, DAGs_deploy_fixdoc, process_sequence_all_fixdoc, start_time_all_fixdoc, 2010)

    heft = HEFT(G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream)
    start = datetime.datetime.now()
    DAGs_orders, DAGs_deploy = heft.get_response_time(sorted_DAG_path=SORTED_DAG_PATH)
    end = datetime.datetime.now()
    print('Computer\'s running time:', (end - start).seconds, 'seconds')
    print('\nThe finish time of each function on the chosen server for DAG #%d:' % 2010)
    pprint.pprint(DAGs_orders[2010])
