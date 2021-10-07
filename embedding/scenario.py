"""
Generate the edge computing scenario and set functions. We simply set \sigma_{ij} as zero.
    Author: Hailiang Zhao (hliangzhao@zju.edu.cn)
"""
import numpy as np
import pandas as pd
import random
import pprint
from embedding.utils import ProgressBar
from embedding.parameters import *


bar = ProgressBar()
para = Parameter()


def generate_scenario():
    """
    Generate the edge computing scenario, i.e., generate a connected graph of edge servers,
    including the
        connectivity,
        processing power of each server,
        bandwidth of each physical link.
    """
    # step 1: generate a connected graph
    # initialize
    G = np.zeros((para.get_server_num(), para.get_server_num()))
    D = np.ones((para.get_server_num(), para.get_server_num())) * MAX_VALUE

    is_connected = False
    for i in range(para.get_server_num()):
        G[i, i] = 1
        D[i, i] = 0

    while not is_connected:
        for i in range(para.get_server_num()):
            # randomly connect i and at most 'DENSITY' other servers
            conn_node_num = random.randint(0, para.get_density())
            for j in range(conn_node_num):
                k = random.randint(0, para.get_server_num() - 1)
                G[i, k], G[k, i] = 1, 1

        for i in range(para.get_server_num()):
            for j in range(para.get_server_num()):
                if G[i, j] == 1:
                    D[i, j], D[j, i] = 1, 1
        for i in range(para.get_server_num()):
            for j in range(para.get_server_num()):
                for k in range(para.get_server_num()):
                    if D[j, k] > D[j, i] + D[i, k]:
                        D[j, k] = D[j, i] + D[i, k]

        is_continue = False
        for i in range(para.get_server_num()):
            for j in range(para.get_server_num()):
                if D[i, j] == MAX_VALUE:
                    # the graph is not a connected graph
                    is_continue = True
                    break
        if is_continue == False:
            is_connected = True


    # step 2: set the bandwidth
    bw = np.ones((para.get_server_num(), para.get_server_num())) * -1
    for i in range(para.get_server_num()):
        j = 0
        while j < i:
            if G[i, j] == 1:
                b = random.randint(para.get_bw_lower(), para.get_bw_upper())
                bw[i, j], bw[j, i] = b, b
            j = j + 1

    # step 3: set the processing power
    pp = np.random.randint(para.get_pp_lower(), para.get_pp_upper(), (para.get_server_num()))

    return G, bw, pp


def print_scenario(G, bw, pp):
    print('\nThe connected graph of edge servers (represented by adjcent matrix):')
    pprint.pprint(G)
    print('\n====> throughput of each link <====')
    pprint.pprint(bw)
    print('\n====> processing power of edge server <====')
    pprint.pprint(pp)


def go_forward(node, node_dst, paths_ij, path_ij, path_nodes_ij, G):
    """
    The recursive algorithm (OSM) to find all the simple paths between any two node i and j.
    """
    if node == node_dst:
        path_ij.append(node)
        paths_ij.append(path_ij[:])
        path_ij.pop()
    else:
        path_ij.append(node)
        path_nodes_ij.add(node)
        for i in range(para.get_server_num()):
            if G[node][i] and (i not in path_nodes_ij):
                go_forward(i, node_dst, paths_ij, path_ij, path_nodes_ij, G)
        path_ij.pop()
        path_nodes_ij.discard(node)


def get_simple_paths(G):
    """
    Get all the simple paths between any two edge servers. Call the subroutine go_forward().
    """
    simple_paths = []
    for i in range(para.get_server_num()):
        paths_from_i = []
        for j in range(para.get_server_num()):
            node = i
            node_dst = j
            paths_ij, path_ij = [], []
            path_nodes_ij = set()
            go_forward(node, node_dst, paths_ij, path_ij, path_nodes_ij, G)
            paths_from_i.append(paths_ij)
        simple_paths.append(paths_from_i)
    return simple_paths


def print_simple_paths(simple_paths):
    print('\n====> All simple paths between any two server <====')
    pprint.pprint(simple_paths)


def print_simple_path(simple_paths, i, j):
    print('\n====> All simple paths between server %d and %d <====' % (i + 1, j + 1))
    print('\nFrom server %d to server %d:' % (i + 1, j + 1))
    pprint.pprint(simple_paths[i][j])
    print('\nFrom server %d to server %d:' % (j + 1, i + 1))
    pprint.pprint(simple_paths[j][i])


def get_ratio(simple_paths, bw):
    """
    Calculate the sum of the reciprocal of bandwidth of each link for every simple path.
    Then, get the proportion of data stream size which routes through the first simple path between any two nodes.
    (The first simple path between i and j is stored in simple_paths[i][j][0].)
    """
    reciprocals_list = []
    proportions_list = []

    for i in range(para.get_server_num()):
        reciprocals = []
        proportions = []
        for j in range(para.get_server_num()):
            paths = simple_paths[i][j]
            paths_len = len(paths)
            reciprocal_sum_list = []
            if i != j:
                for k in range(paths_len):
                    path = paths[k]
                    path_len = len(path)
                    reciprocal_sum = 0
                    for l in range(path_len - 1):
                        reciprocal_sum = reciprocal_sum + 1 / bw[path[l], path[l + 1]]
                    reciprocal_sum_list.append(reciprocal_sum)
            reciprocals.append(reciprocal_sum_list)

            if len(reciprocal_sum_list) > 0:
                # the source node and the des. node are different nodes
                proportions.append(1. / sum(reciprocal_sum_list[0] / reciprocal_sum_list))
            else:
                proportions.append(-1)
        reciprocals_list.append(reciprocals)
        proportions_list.append(proportions)

    return reciprocals_list, proportions_list


def set_funcs():
    """
    Set the processing power required and the output data stream size of functions.
    The two variables are reused for all DAGs (yes I am lazy :-)).
    """
    # set the processing power required
    pp_required = np.random.randint(
        para.get_pp_required_lower(),
        para.get_pp_required_upper(),
        (para.get_max_func_num()))
    data_stream = np.random.randint(
        para.get_data_stream_size_lower(),
        para.get_data_stream_size_upper(),
        (para.get_max_func_num()))
    return pp_required, data_stream
