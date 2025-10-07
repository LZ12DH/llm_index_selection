# @TIME : 4/2/25 11:10 AM
# @AUTHOR : LZDH
import copy

import pandas as pd


def sample_tpch(k=1):
    df = pd.read_csv('queries_tpch_train_labelled.csv').fillna('NA')
    queries = df['original_sql'].tolist()
    templates = df['template'].tolist()
    num_templates = list(set(templates))
    sampled_queries = []
    for i in num_templates:
        t = copy.deepcopy(templates)
        for j in range(k):
            ind = t.index(i)
            query = queries[ind]
            sampled_queries.append(query)
            t.pop(ind)
            queries.pop(ind)
    print(sampled_queries)
    print(len(sampled_queries))
    # df.to_csv('queries_tpch_train_labelled.csv')


def sample_job(k=1):
    df = pd.read_csv('queries_job_syn_train_labelled.csv').fillna('NA')
    queries = df['original_sql'].tolist()
    templates = df['template'].tolist()
    num_templates = list(set(templates))
    sampled_queries = []
    for i in num_templates:
        t = copy.deepcopy(templates)
        for j in range(k):
            ind = t.index(i)
            query = queries[ind]
            sampled_queries.append(query)
            t.pop(ind)
            queries.pop(ind)
    print(sampled_queries)
    print(len(sampled_queries))
    # df.to_csv('queries_tpch_train_labelled.csv')


def sample_tpcds(k=1):
    df = pd.read_csv('queries_tpcds_train.csv').fillna('NA')
    queries = df['Query_text'].tolist()
    templates = df['Query_id'].tolist()
    run_id = df['Run_id'].tolist()
    num_templates = list(set(templates))
    print(num_templates)
    sampled_queries = []
    for i in num_templates:
        t = copy.deepcopy(templates)
        for j in range(k):
            ind = t.index(i)
            query = queries[ind]
            sampled_queries.append(query)
            t.pop(ind)
            queries.pop(ind)
    print(sampled_queries)
    print(len(sampled_queries))
    # df.to_csv('queries_tpch_train_labelled.csv')


sample_tpcds()