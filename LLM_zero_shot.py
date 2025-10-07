# @TIME : 12/2/25 8:18 AM
# @AUTHOR : LZDH
import copy
import json
import os.path
import random

import pandas as pd
import re
import openai
import time
import glob
from openai import OpenAI
from openai import AzureOpenAI
from get_query_meta import *
from get_index import *
from index_graph import *
from eval_index import *
from agents import reasoning_pipe, planning_agent_one_step

client = OpenAI(
    api_key="",  # 填入AccessKey
    # api_key=os.environ["afdfd0b8bacc7e7c86b5e59fa6fc027b"],
    base_url="",
)


def query_gpt_attempts(prompt, trys):
    try:
        output = query_turbo_model(prompt)
    except Exception as error:
        print(trys)
        print(error)
        trys += 1
        if trys <= 3:
            output = query_gpt_attempts(prompt, trys)
        else:
            output = {'content': 'NA'}
    return output


def query_turbo_model(prompt):
    chat_completion = client.chat.completions.create(
        messages=prompt,
        model="gpt-4o-0806",
        # model="gpt-3.5-turbo",
        temperature=0,
    )
    return chat_completion.choices[0].message.content
    # response = ollama.chat(model='qwen3:14b', messages=prompt)
    # response = ollama.chat(model='qwen3:32b', messages=prompt)
    # response = ollama.chat(model='deepcoder:14b', messages=prompt)
    # return response['message']['content']


def generate_turbo_prompt_pioneer(schema, workload, candidates=[], is_workload=False, storage=500):
    # w/wo index candidates and demo
    if is_workload:
        p = [{'role': "system", 'content': 'You are an online SQL index selection agent. You will be given a schema of '
                                           'a database, a workload of SQL queries querying this database and the list '
                                           'of index column candidates for the workload . You are required to propose '
                                           'an index selection '
                                           'ONLY based on the candidate columns so that the execution of the workload '
                                           'will be optimised. Each candidate in the given list is a Python list '
                                           'formatting: "[column_name, table_name, column_min_value, column_max_value'
                                           ', column_utility]", where column_utility represents the cost reduction of '
                                           'the workload if the column is selected as index alone. You should select '
                                           'no more than ' + str(storage) + ' candidates. '
                                                                            'You should return only the selected index, in the form of '
                                                                            '"Index selection: [Table_name1(column_name1, column_name2), '
                                                                            'Table_name2(column_name3)]". '
              }, ]
        p_0 = [{
            'role': "user",
            'content': "Schema: " + str(schema) + ". Workload: " + str(workload) + ". Index candidates: " + str(
                candidates) + "."
        }]
    else:
        p = [{'role': "system", 'content': 'You are an online SQL index selection agent. You will be given a schema of '
                                           'a database and the list of index column candidates from a workload of '
                                           'SQL queries '
                                           'querying this database. You are required to propose an index selection '
                                           'ONLY based on the candidate columns so that the execution of the workload '
                                           'will be optimised. Each candidate in the given list is a Python list '
                                           'formatting: "[table_name.column_name, column_type, column_rows_selected, '
                                           'column_utility, column_storage_cost, '
                                           ' column_min_value, column_max_value]", where column_utility represents the '
                                           'cost reduction of the workload if the column is selected as index alone, '
                                           'and column_storage_cost represents the STORAGE COST if the candidate is '
                                           'selected. '
                                           'Your candidates selected should have TOTAL STORAGE COST less than '
                                           + str(storage) + '. Try to fully utilize the budget given. You should '
                                                            'return only the selected index, in the form of '
                                                            '"Index selection: [Table_name1(column_name1, column_name2), '
                                                            'Table_name2(column_name3)]". '
              }, ]
        p_0 = [{
            'role': "user",
            'content': "Schema: " + str(schema) + ". Index candidates: " + str(candidates) + "."
        }]
    p = p + p_0
    return p


def generate_turbo_prompt_demo(schema, candidates, demos, storage=500, shots=1):
    p = [{'role': "system", 'content': 'You are an online SQL index selection agent. You will be given a schema of '
                                       'a database and the list of index column candidates from a workload of '
                       \
                                       'SQL queries querying this database. You are required to propose an index '
                                       'selection ONLY based on the candidate columns so that the execution of the '
                                       'workload will be optimised. Each candidate in the given list is a Python list '
                                       'formatting: "[table_name.column_name, column_type, column_rows_selected, '
                                       'column_utility, column_storage_cost, column_min_value, column_max_value]", '
                                       'where column_utility represents the cost reduction of the workload if the '
                                       'column is selected as index alone, and column_storage_cost represents the '
                                       'STORAGE COST if the candidate is selected. Your candidates selected should '
                                       'have TOTAL STORAGE COST less than ' + str(storage) +
                                       '. Try to fully utilize the budget given. You should return only the selected '
                                       'index, in the form of "Index selection: '
                                       '[Table_name1(column_name1, column_name2), Table_name2(column_name3)]". '
          }, ]
    p_0 = []
    for i in range(shots):
        schema_0, candidates_0, index_0 = demos[i]
        p_0 = p_0 + [{'role': "user",
                      'content': "Schema: " + str(schema_0) + ". Index candidates: " + str(candidates_0) + "."
                      },
                     {
                         'role': "assistant",
                         'content': str(index_0),
                     }, ]
    p_0 = p_0 + [{
                    'role': "user",
                    'content': "Schema: " + str(schema) + ". Index candidates: " + str(candidates) + "."
                }, ]
    p = p + p_0
    return p


def generate_turbo_prompt_post_process(schema, indexes):
    remove_rule = "Remove Rule: Remove the index on columns that may have too many distinct values"
    dedup_rule = "Deduplicate Rule: Simplify the index on columns from the same table that sharing too same " \
                 "information by only keeping the high utility columns."
    rules = [remove_rule, dedup_rule]
    p = [{'role': "system", 'content': 'You are an online SQL index selection agent. You will be given a schema of '
                                       'a database and the list of indexes selected for a workload given the form of '
                                       '"Index selection: [Table_name1(column_name1, column_name2), '
                                       'Table_name2(column_name3)]". You are required to refine the index '
                                       'selection based on the rules provided below: ' + str(rules) +
                                       '. You should return only the selected '
                                       'index, in the form of "Index selection: '
                                       '[Table_name1(column_name1, column_name2), Table_name2(column_name3)]". '
          }, ]
    p_0 = [{
        'role': "user",
        'content': "Schema: " + str(schema) + ". Index selected: " + str(indexes) + "."
    }]
    p = p + p_0
    return p


def extract_ind_col(index):
    pattern = r"\((.*?)\)"
    matches = re.findall(pattern, index)
    cols_in_ind = [x.replace('C ', '').split(',') for x in matches]
    cols = []
    for i in cols_in_ind:
        cols = cols + i
    return cols


def get_all_indexable_cols(db_id, workload):
    indexable_cols = []
    with open('data/schemas/' + db_id + '.json') as f_sch:
        data = f_sch.read()
        schema = json.loads(data)
    all_tabs = [x['table'] for x in schema]
    all_cols = {}
    for t in all_tabs:
        all_cols[t] = [y['name'] for x in schema for y in x['columns'] if x['table'] == t]
    for tab in list(all_cols.keys()):
        for col in all_cols[tab]:
            for query in workload:
                if col in query:
                    indexable_cols.append([tab, col])
    return indexable_cols


def filt_schema(schema, workload, candidates):
    filted_schema = []
    cand_cols = [x[0].split('.')[1] for x in candidates]
    for tab in schema:
        table_name = tab['table']
        if all([table_name not in query for query in workload]):
            continue
        else:
            filted_tab = {}
            filted_tab['table'] = table_name
            filted_tab['rows'] = tab['rows']
            filted_tab['columns'] = []
            for col in tab['columns']:
                col_name = col['name']
                for query in workload:
                    if col_name not in query or col_name not in cand_cols:
                        continue
                    else:
                        filted_tab['columns'].append(col)
                        break
            filted_schema.append(filted_tab)
    return filted_schema


def predict_index_storage(output, working_db_id):
    if output and 'I(C' not in output:
        pattern = r"\[(.*?)\]"
        matches = re.findall(pattern, output)
        if len(matches[0].split('), ')) != 1:
            inds_selected = [x + ')' if x[-1] != ')' else x for x in matches[0].split('), ')]
        else:
            inds_selected = [x for x in matches[0].split('), ')]
        indexes = []
        for ind in inds_selected:
            tab = ind.split('(')[0]
            cols = ind.split('(')[1][:-1].split(', ')
            inds = [tab + '.' + x for x in cols]
            indexes.append(inds)
    elif output and 'I(C' in output:
        pattern = r"\((.*?)\)"
        matches = re.findall(pattern, output)
        cols_in_ind = [x.replace('C ', '').split(',') for x in matches]
        indexes = []
        for i in cols_in_ind:
            tab = i[0].split('.')[0]
            cols = [x.split('.')[1] for x in i]
            ind = tab + '.'
            inds = []
            for c in cols:
                ind_i = ind + c
                inds.append(ind_i)
            indexes.append(inds)
    else:
        indexes = []
    est_storage_cost = predict_index_sizes(indexes, working_db_id)
    return est_storage_cost


if __name__ == "__main__":
    # can fix these params if testing
    w_type = 'random'
    dataset = 'test'
    reasoning = True
    one_shot = False
    same = False
    postprocess = False
    is_workload = False
    filtered = False
    one_step = False
    # end of fixed params

    # the dataset name
    db_id = 'tpch'
    # the name of the database
    working_db_id = f"indexselection_{db_id}___10"
    # storage budgets given
    storages = [2048]
    org_time_dir = f"results/{dataset}/{db_id}/result_naive_1024.csv"
    # exp_id defined by num of queries in the workload
    exps = [19]
    for exp in exps:
        for storage in storages:
            # path = "data/workloads/" + dataset + "/" + db_id + "/train_workload_" + w_type + "_" + str(exp)
            path = "data/workloads/" + dataset + "/" + db_id + "/" + db_id + "_test_" + str(exp) + "q.csv"
            # exp_files = glob.glob(path + "_**.csv")
            exp_files = glob.glob(path)
            w_id = exp_files[0].split('/')[-1].split('.')[0]
            print(w_id)
            result_dir = f"results/{dataset}/{db_id}/result_naive_{str(storage)}.csv"
            if os.path.isfile(result_dir):
                finished_results = pd.read_csv(result_dir)
                finished_ids = finished_results['workload_id'].tolist()
            else:
                finished_results = pd.DataFrame()
                finished_ids = []
            if w_id not in finished_ids:
                # continue
                log_dir = f"logs/{db_id}/{dataset}/{str(storage)}/{w_id}/"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                with open('data/schemas/' + db_id + '.json') as f_sch:
                    data = f_sch.read()
                    schema = json.loads(data)
                min_max_df = pd.read_csv('qp_evaluation/data/' + db_id + '/col_min_max.csv').fillna('NA')
                df = pd.read_csv(exp_files[0])
                # df = pd.read_csv('data/workloads/' + db_id + '/workload_' + w_type + '_' + str(w_size) + '.csv')
                workload = df['Query_text'].tolist()
                if filtered:
                    with open('../selected_index_list_top_6.json') as fs:
                        data = fs.read()
                        selected_dict = json.loads(data)
                        selected_list = selected_dict['index_selected']
                    candidates = get_cand_nodes(db_id, working_db_id, workload, selected_list)
                    candidates = [x for x in candidates if x[0] in selected_list]
                else:
                    candidates = get_cand_nodes(db_id, working_db_id, workload, [])
                for j in range(len(candidates)):
                    tab = candidates[j][0].split('.')[0]
                    col = candidates[j][0].split('.')[1]
                    candidates[j].append(
                        min_max_df[(min_max_df['table'] == tab) & (min_max_df['column'] == col)]['min'].tolist()[0])
                    candidates[j].append(
                        min_max_df[(min_max_df['table'] == tab) & (min_max_df['column'] == col)]['max'].tolist()[0])
                candidates = [x for x in candidates if x[3] != 0]
                # print(len(candidates))
                candidates = [x for x in candidates if x[4] <= storage]
                print(candidates)
                # further filter top k
                # top_k = 50
                # if len(candidates) > top_k:
                #     candidates_seq = copy.deepcopy(candidates)
                #     candidates_seq.sort(key=lambda x: x[3], reverse=True)
                #     threshold = candidates_seq[50][3]
                #     candidates = [x for x in candidates if x[3] >= threshold]
                print('num of candidates: ', len(candidates))

                if org_time_dir and os.path.isfile(org_time_dir):
                    df_org = pd.read_csv(org_time_dir)
                    hypo_runtime_org = df_org[df_org['workload_id'] == w_id]['hypo_runtime_org'].tolist()[0]
                    actual_runtime_org = df_org[df_org['workload_id'] == w_id]['actual_runtime_org'].tolist()[0]
                else:
                    print('No data for the original workload, evaluating...')
                    output_o = ''
                    hypo_runtime_org = eval_index_hypo(working_db_id, workload, output_o)
                    actual_runtime_org = eval_index_run(working_db_id, workload, output_o, log_dir, 'original')
                    print('Hypo Original Runtime: ', hypo_runtime_org)
                    print('Actual Original Runtime: ', actual_runtime_org)

                # if you hope to run any baselines together
                output_extend = ''
                hypo_runtime_extend = 0
                actual_runtime_extend = actual_runtime_org
                actual_storage_extend = 1
                print('output_extend: ', output_extend)
                print('Hypo Runtime Extend: ', hypo_runtime_extend)
                print('Actual Runtime Extend: ', actual_runtime_extend)
                # end of baseline

                schema0 = filt_schema(schema, workload, candidates)
                if reasoning:
                    if one_step:
                        p_act = planning_agent_one_step(schema, workload, candidates, storage)
                        output = query_turbo_model(p_act)
                    else:
                        # output, storage_used, actions_log = reasoning_pipe(working_db_id, schema0, workload, candidates, storage)
                        output, storage_used, actions_log = reasoning_pipe(working_db_id, schema0, workload, candidates,
                                                                           storage)
                        with open(log_dir + 'action_log.json', 'w+') as f_log:
                            f_log.write(json.dumps(actions_log, indent=4))
                else:
                    if one_shot:
                        if same:
                            candidates_0 = candidates
                            index_0 = output_extend
                        else:
                            demo_dir = f"results/{dataset}/{db_id}/result_naive_{str(storage)} copy.csv"
                            demo_results = pd.read_csv(demo_dir)
                            demo_ids = demo_results['workload_id'].tolist()
                            sampled_demo_id = random.sample(demo_ids, 1)[0]
                            candidates_0 = demo_results[demo_results['workload_id'] == sampled_demo_id]['candidates_llm'].tolist()[0]
                            index_0 = demo_results[demo_results['workload_id'] == sampled_demo_id]['selection_extend'].tolist()[0]
                        demos = [[schema0, candidates_0, index_0]]
                        p = generate_turbo_prompt_demo(schema0, candidates, demos, storage)
                        # p = generate_turbo_prompt_pioneer(schema0, workload, candidates, is_workload, storage)
                    else:
                        p = generate_turbo_prompt_pioneer(schema0, workload, candidates, is_workload, storage)
                    print(p)
                    output = query_turbo_model(p)
                if postprocess:
                    p_postprocess = generate_turbo_prompt_post_process(schema0, output)
                    output = query_turbo_model(p_postprocess)
                print('LLM Selection: ', output)
                output = output.split('Index selection:')[-1]
                print('LLM Selection: ', output)
                if '[]' in output:
                    hypo_runtime = hypo_runtime_org
                    actual_runtime = actual_runtime_org
                else:
                    hypo_runtime = eval_index_hypo(working_db_id, workload, output)
                    actual_runtime = eval_index_run(working_db_id, workload, output, log_dir)
                print('LLM Selection Filtered: ', output)
                print('Hypo Runtime LLM: ', hypo_runtime)
                print('Actual Runtime LLM: ', actual_runtime)

                actual_storage = get_index_actual_storage(working_db_id, output)
                print('Storage Cost LLM: ', actual_storage)
                print('Storage Cost Extend: ', actual_storage_extend)

                if actual_storage == 0:
                    score_llm = 0
                else:
                    score_llm = (actual_runtime_org - actual_runtime) / actual_storage
                if actual_storage_extend == 0:
                    score_extend = 0
                else:
                    score_extend = (actual_runtime_org - actual_runtime_extend) / actual_storage_extend
                if not os.path.isfile(result_dir):
                    result_dict = {'workload_id': [w_id],
                                   'queries': [workload],
                                   'hypo_runtime_org': [hypo_runtime_org],
                                   'actual_runtime_org': [actual_runtime_org],
                                   'selection_extend': [output_extend],
                                   'hypo_runtime_extend': [hypo_runtime_extend],
                                   'actual_runtime_extend': [actual_runtime_extend],
                                   'est_storage_cost_extend': [actual_storage_extend],
                                   'candidates_llm': [candidates],
                                   'selection_llm': [output],
                                   'est_storage_cost_llm': [actual_storage],
                                   'hypo_runtime_llm': [hypo_runtime],
                                   'actual_runtime_llm': [actual_runtime],
                                   'score_extend': [score_extend],
                                   'score_llm': [score_llm]}
                    df = pd.DataFrame(result_dict)
                    df.to_csv(result_dir)
                else:
                    df_o = pd.read_csv(result_dir)
                    result_dict = {'workload_id': [w_id],
                                   'queries': [workload],
                                   'hypo_runtime_org': [hypo_runtime_org],
                                   'actual_runtime_org': [actual_runtime_org],
                                   'selection_extend': [output_extend],
                                   'hypo_runtime_extend': [hypo_runtime_extend],
                                   'actual_runtime_extend': [actual_runtime_extend],
                                   'est_storage_cost_extend': [actual_storage_extend],
                                   'candidates_llm': [candidates],
                                   'selection_llm': [output],
                                   'est_storage_cost_llm': [actual_storage],
                                   'hypo_runtime_llm': [hypo_runtime],
                                   'actual_runtime_llm': [actual_runtime],
                                   'score_extend': [score_extend],
                                   'score_llm': [score_llm]}
                    df = pd.DataFrame(result_dict)
                    df_o = pd.concat([df_o, df], ignore_index=True)
                    df_o.to_csv(result_dir, index=False)
            else:
                continue

