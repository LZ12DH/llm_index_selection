# @TIME : 1/10/24 2:42 PM
# @AUTHOR : LZDH
import copy
import pickle
import ast


def get_all_index(db_id):
    if db_id == 'job':
        with open('data/indexes/IMDB_JOB_index_query_pairs_w_cost.pkl', 'rb') as f:
            Q_I_pairs = pickle.load(f)
    elif db_id == 'tpcds':
        with open('data/indexes/TPCDS_10_index_query_pairs_w_cost.pkl', 'rb') as f:
            Q_I_pairs = pickle.load(f)
    data = []
    print(Q_I_pairs[0])
    for i in range(len(Q_I_pairs)):
        # print(i)
        query = ast.literal_eval(Q_I_pairs[i][0])[1].replace('\n', ' ').split('-- end query')[0]
        if len(Q_I_pairs[i][1]) > 1:
            cand_index = Q_I_pairs[i][1][1:]
            costs = Q_I_pairs[i][2][1:]
        else:
            cand_index = 'No index'
            costs = Q_I_pairs[i][2]
        data.append([query, cand_index, costs])
    return data


def get_best_index(db_id):
    if db_id == 'job':
        with open('data/indexes/IMDB_JOB_index_query_pairs_w_cost.pkl', 'rb') as f:
            Q_I_pairs = pickle.load(f)
    elif db_id == 'tpcds':
        with open('data/indexes/TPCDS_10_index_query_pairs_w_cost.pkl', 'rb') as f:
            Q_I_pairs = pickle.load(f)
    data = []
    for i in range(len(Q_I_pairs)):
        # print(i)
        costs = Q_I_pairs[i][2]
        min_cost = min(costs)
        min_cost_ind = costs.index(min_cost)
        query = ast.literal_eval(Q_I_pairs[i][0])[1].replace('\n', ' ').split('-- end query')[0]
        if len(Q_I_pairs[i][1]) > 1:
            min_index = Q_I_pairs[i][1][min_cost_ind]
            cost = Q_I_pairs[i][2][min_cost_ind]
        else:
            min_index = 'No index'
            cost = Q_I_pairs[i][2][0]
        data.append([query, min_index, cost])
    return data


def get_top_n_index(db_id, n):
    if db_id == 'job':
        with open('data/indexes/IMDB_JOB_index_query_pairs_w_cost.pkl', 'rb') as f:
            Q_I_pairs = pickle.load(f)
    elif db_id == 'tpcds':
        with open('data/indexes/TPCDS_10_index_query_pairs_w_cost.pkl', 'rb') as f:
            Q_I_pairs = pickle.load(f)
    data = []
    for i in range(len(Q_I_pairs)):
        # print(i)
        # print(Q_I_pairs[i])
        costs = Q_I_pairs[i][2]
        costs_0 = copy.deepcopy(costs)
        costs_0 = sorted(costs_0, reverse=True)
        top_n_indexes = {}
        query = ast.literal_eval(Q_I_pairs[i][0])[1].replace('\n', ' ').split('-- end query')[0]
        if len(Q_I_pairs[i][1]) > n:
            for j in range(n):
                min_cost = costs_0.pop(-1)
                min_cost_ind = costs.index(min_cost)
                min_index = Q_I_pairs[i][1][min_cost_ind]
                cost = Q_I_pairs[i][2][min_cost_ind]
                if min_index == '[]':
                    min_index = 'No Index'
                top_n_indexes[min_index] = cost
        elif len(Q_I_pairs[i][1]) > 1:
            for k in range(len(Q_I_pairs[i][1])-1):
                if Q_I_pairs[i][1][k+1] == '[]':
                    Q_I_pairs[i][1][k + 1] = 'No Index'
                top_n_indexes[Q_I_pairs[i][1][k+1]] = Q_I_pairs[i][2][k+1]
        data.append([query, top_n_indexes])
    return data


# print(get_top_n_index('tpcds', 4)[80])
# print(get_top_n_index('tpcds', 4)[55][1])
