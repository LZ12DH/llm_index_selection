# @TIME : 8/5/25 12:03 PM
# @AUTHOR : LZDH
from openai import OpenAI
from eval_index import *

# GPT-4
client = OpenAI(
    api_key="",  # 填入AccessKey
    # api_key=os.environ.get('api_key'),
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


def planning_agent(schema, candidates, current_index, current_storage, storage, prev_plan, suggestion='None'):
    p = [{'role': "system", 'content': 'You are an online SQL index selection planner. You will be given a schema of '
                                       'a database, the list of index column candidates from a workload of '
                                       'SQL queries querying this database, the current list of selected index, the '
                                       'current storage used, the total storage budget, the previous plan steps '
                                       'before this one and a suggestion for the next planning step.'
                                       'Each candidate in the given list is a Python list '
                                       'formatting: "[table_name.column_name, column_type, column_rows_selected, '
                                       'column_utility, column_storage_cost, column_min_value, column_max_value]", '
                                       'where column_utility represents the cost reduction of the workload if the '
                                       'column is selected as index alone, and column_storage_cost represents the '
                                       'STORAGE COST if the candidate is selected. You are required to plan the next '
                                       'action ONLY from the following options:\n'
                                       '"Selection Agent": call the selection agent to select more columns as index.\n'
                                       '"Revision Agent": call the revision agent to revise the current indexes '
                                       'selected, removing potential duplicates, regressions and out-of budget.\n'
                                       '"Combination Agent": call the combination agent to merge single-column '
                                       'indexes from the same table if more efficient.\n'
                                       '"Stop": the current index selection is optimal and no more actions are '
                                       'required.\n Given the previous plan steps, avoid using more than three '
                                       '"Revision Agent" or "Combination Agent" continuously. '
                                       'Output your ONE action choosing from: ["Selection Agent", '
                                       '"Revision Agent", "Combination Agent", "Stop"].'}, ]
    p_0 = [{
        'role': "user",
        'content': "Schema: " + str(schema) + ". Index candidates: " + str(candidates) + ". Current index selection: "
                   + str(current_index) + ". Current storage used: " + str(current_storage) + ". Total storage budget: "
                   + str(storage) + ". Previous plan steps: " + str(prev_plan) + ". Suggestion: " + str(suggestion) + "."
    }]
    p = p + p_0
    return p


def planning_agent_one_step(schema, workload, candidates, storage):
    p = [{'role': "system", 'content': 'You are an online SQL index selection planner. You will be given a schema of '
                                       'a database, the list of SQL queries from a workload querying this database, the columns with their storages '
                                       'and the total storage budget. You are required to select columns '
                                       'from the SQL queries without explanation to form indexes optimising '
                                       'the workload execution, in the form of "Index selection: '
                                       '[Table_name1(column_name1, column_name2), Table_name2(column_name3)].'}, ]
    p_0 = [{
        'role': "user",
        'content': "Schema: " + str(schema) + ". Workload: " + str(workload) + ". Column storages: "
                   + str([[x[0], x[4]] for x in candidates]) + ". Total storage budget: " + str(storage) + "."
    }]
    p = p + p_0
    return p


def planning_agent_no_cand(schema, workload, current_index, current_storage, storage, prev_plan, suggestion='None'):
    p = [{'role': "system", 'content': 'You are an online SQL index selection planner. You will be given a schema of '
                                       'a database, the SQL queries from a workload querying this database, the current list of selected index, the '
                                       'current storage used, the total storage budget, the previous plan steps '
                                       'before this one and a suggestion for the next planning step. You are required to plan the next '
                                       'action ONLY from the following options:\n'
                                       '"Selection Agent": call the selection agent to select more columns as index.\n'
                                       '"Revision Agent": call the revision agent to revise the current indexes '
                                       'selected, removing potential duplicates, regressions and out-of budget.\n'
                                       '"Combination Agent": call the combination agent to merge different single-column '
                                       'indexes from the same table to make it more efficient.\n'
                                       '"Stop": the current index selection is optimal and no more actions are '
                                       'required.\n Given the previous plan steps, avoid using too many '
                                       '"Revision Agent" or "Combination Agent" continuously. '
                                       'Output your ONE action choosing from: ["Selection Agent", '
                                       '"Revision Agent", "Combination Agent", "Stop"].'}, ]
    p_0 = [{
        'role': "user",
        'content': "Schema: " + str(schema) + ". Index candidates: " + str(workload) + ". Current index selection: "
                   + str(current_index) + ". Current storage used: " + str(current_storage) + ". Total storage budget: "
                   + str(storage) + ". Previous plan steps: " + str(prev_plan) + ". Suggestion: " + str(suggestion) + "."
    }]
    p = p + p_0
    return p


def selection_agent(schema, candidates, current_index):
    p = [{'role': "system", 'content': 'You are an online SQL index selection agent. You will be given a schema of '
                                       'a database, the list of index column candidates from a workload of '
                                       'SQL queries querying this database and the current list of index'
                                       '. You are required to select one column from the candidates to add into the '
                                       'index, so that the execution of the workload'
                                       ' will be more efficient. Each candidate in the given list is a Python list '
                                       'formatting: "[table_name.column_name, column_type, column_rows_selected, '
                                       'column_utility, column_storage_cost,  column_min_value, column_max_value]", '
                                       'where column_utility represents the cost reduction of the workload if the '
                                       'column is selected as index alone, and column_storage_cost represents the '
                                       'STORAGE COST if the candidate is selected. You should return only the selected '
                                       'column without explanation, in the form of "Index selection: '
                                       '[table_name(column_name)]". '
          }, ]
    p_0 = [{
        'role': "user",
        'content': "Schema: " + str(schema) + ". Index candidates: " + str(candidates) + ". Current index selection: "
                   + str(current_index) + "."
    }]
    p = p + p_0
    return p


def selection_agent_nocand(schema, workload, candidates, current_index, storage_left):
    p = [{'role': "system", 'content': 'You are an online SQL index selection agent. You will be given a schema of '
                                       'a database, the SQL queries from a workload'
                                       ' querying this database, the storage cost list of the columns, the storage budget left and the current list of index'
                                       '. You are required to select one column to add into the '
                                       'index, so that the execution of the workload'
                                       ' will be more efficient. You should return only the selected '
                                       'column without explanation, in the form of "Index selection: '
                                       '[table_name(column_name)]". '
          }, ]
    p_0 = [{
        'role': "user",
        'content': "Schema: " + str(schema) + ". Workload: " + str(workload) + ". Column storages: "
                   + str([[x[0], x[4]] for x in candidates]) + ". Storage budget left: "
                   + str(storage_left) + ". Current index selection: "
                   + str(current_index) + "."
    }]
    p = p + p_0
    return p


def revision_agent(schema, indexes, storage, storage_used):
    remove_rule = "Remove Rule: Remove the index on columns that may have too many distinct values"
    dedup_rule = "Deduplicate Rule: Simplify the index on columns from the same table that sharing too same " \
                 "information by only keeping the high utility columns."
    selectivity_rule = "High Selectivity Rule: If the number of rows selected from a column is high, it is likely not an efficient index to recommend."
    distinct_ratio_rule = "Low Distinct Ratio Rule: Columns having lots of repeated same values are likely not good indexes to recommend."
    storage_limit_rule = f"Shrink Rule: If the current storage used exceeds the storage budget {storage}, shrink the " \
                         f"index by removing some less efficient columns. "
    rules = [remove_rule, dedup_rule, selectivity_rule, distinct_ratio_rule, storage_limit_rule]
    p = [{'role': "system", 'content': 'You are an online SQL index revision agent. You will be given a schema of '
                                       'a database and the list of indexes selected for a workload given the form of '
                                       '"[Table_name1(column_name1, column_name2), Table_name2(column_name3)]". '
                                       'You are required to refine the index '
                                       'selection based on the rules provided below: ' + str(rules) +
                                       '. You should return ONLY the revised '
                                       'index without explanation. DO NOT select new index and STRICTLY in the form '
                                       'of "[Table_name1(column_name1, column_name2), Table_name2(column_name3)]". '
          }, ]
    p_0 = [{
        'role': "user",
        'content': "Schema: " + str(schema) + ". Index selected: " + str(indexes) + ". Storage used: " + str(storage_used) + '.'
    }]
    p = p + p_0
    return p


def combination_agent(candidates, indexes):
    sort_rule = "SORT order rule: If multiple columns in the same ORDER BY clause are indexed, combine them by their sort order."
    group_rule = "GROUP order rule: If multiple columns in the same GROUP BY clause are indexed, combine them by their group order."
    indepedent_rule = "Independent columns rule: If all the columns only appear in the JOIN conditions, there is no need to combine them."
    leftmost_rule = "Leftmost Prefix rule: If composite index (a, b) is constructed and b is also used along, create the single index b."
    cross_op_rules_1 = "Combine rule: Combine the indexed columns from the same table by the order of (in selection, in join)."
    cross_op_rules_2 = "Combine rule: Combine the indexed columns from the same table by the order of (in join, in selection)."
    cross_op_rules_3 = "Combine rule: Combine the indexed columns from the same table by the order of (in order by, in selection, in join)."
    cross_op_rules_4 = "Combine rule: Combine the indexed columns from the same table by the order of (in order by, in join, in selection)."
    cross_op_rules_5 = "Combine rule: Combine the indexed columns from the same table by the order of (in group by, in selection, in join)."
    cross_op_rules_6 = "Combine rule: Combine the indexed columns from the same table by the order of (in group by, in join, in selection)."
    rules = [sort_rule, group_rule, indepedent_rule, leftmost_rule, cross_op_rules_1, cross_op_rules_2, cross_op_rules_3,
             cross_op_rules_4, cross_op_rules_5, cross_op_rules_6]
    p = [{'role': "system", 'content': 'You are an online SQL index combination agent. You will be given a set of index candidates of '
                                       'queries and the list of indexes selected for this workload given the form of '
                                       '"[Table_name1(column_name1), Table_name1(column_name2), '
                                       'Table_name2(column_name3)]". You are required to refine the index '
                                       'selection by merging indexes on the same table into composed indexes if the '
                                       'merged index is more efficient. You should only do possible merging based '
                                       'on the rules provided below: ' + str(rules) +
                                       '. You should return ONLY the refined index without explanation. DO NOT select '
                                       'new index or remove index. STRICTLY output in the form of "'
                                       '[Table_name1(column_name1, column_name2), Table_name2(column_name3)]". '
          }, ]
    p_0 = [{
        'role': "user",
        'content': "Index candidates: " + str(candidates) + ". Index selected: " + str(indexes) + "."
    }]
    p = p + p_0
    return p


def reflection_agent(plan_path):
    rules = ["If the last three steps are all 'combination agent', output suggestion 'DO NOT output 'Combination Agent''", "If the last three steps are all 'revision agent', output suggestion 'DO NOT output 'Revision Agent''"]
    p = [{'role': "system", 'content': 'You are an online SQL index selection planning reflection agent. You will be given a '
                                       'planning path of the current index recommendation. You are required to review the planning '
                                       'path and offer suggestions for the next planning step. You should suggest based '
                                       'on the rules provided here: ' + str(rules) +
                                       '.  If no rules fit the current planning step, output "None". You should return ONLY the suggestion without explanation. '
          }, ]
    p_0 = [{
        'role': "user",
        'content': "Planning Path: " + str(plan_path) + "."
    }]
    p = p + p_0
    return p


def reasoning_pipe(working_db_id, schema0, workload, candidates, storage, iters=10, true_storage=False, no_cand=False):
    ind_outputs = []
    selected_indexes = []
    storage_used = 0
    prev_steps = []
    actions_log = []
    startover = False
    suggestion = 'None'
    prompts = []
    i = 0
    start_time = time.time()
    while i <= iters:
        i += 1
        if startover:
            print('No selection, start over.')
            startover = False
            LLM_action = 'Selection Agent'
        elif storage_used >= storage:
            LLM_action = 'Revision Agent'
        else:
            if no_cand:
                p_act = planning_agent_no_cand(schema0, workload, selected_indexes, storage_used, storage, prev_steps, suggestion)
            else:
                p_act = planning_agent(schema0, candidates, selected_indexes, storage_used, storage, prev_steps, suggestion)
            # print(p_act)
            LLM_action = query_turbo_model(p_act)
            prompts.append(p_act)
        try:
            if 'Selection Agent' in LLM_action:
                LLM_action = 'Selection Agent'
                print('New Action: ', LLM_action)
                prev_steps.append(LLM_action)
                if no_cand:
                    storage_left = storage - storage_used
                    p_sel = selection_agent_nocand(schema0, workload, candidates, selected_indexes, storage_left)
                else:
                    p_sel = selection_agent(schema0, candidates, selected_indexes)
                # p_sel = selection_agent(schema0, candidates, selected_indexes)
                prompts.append(p_sel)
                output_sel = query_turbo_model(p_sel)
                # print('New Index Selection: ', output_sel)
                output_sel = output_sel.split('Index selection: ')[-1]
                if '[]' in output_sel:
                    continue
                # if no_cand:
                #     output_sel = '[' + output_sel + ']'
                print('New Index Selection: ', output_sel)
                pattern = r"\[(.*?)\]"
                matches = re.findall(pattern, output_sel)
                if len(matches[0].split('), ')) != 1:
                    inds_selected = [x + ')' if x[-1] != ')' else x for x in matches[0].split('), ')]
                else:
                    inds_selected = [x for x in matches[0].split('), ')]
                if not no_cand:
                    new_ind_name = inds_selected[0].split('(')[0] + '.' + inds_selected[0].split('(')[1][:-1]
                    new_ind = [x for x in candidates if x[0] == new_ind_name]
                    selected_indexes = selected_indexes + new_ind
                else:
                    selected_indexes = selected_indexes + [inds_selected]
                # print('Current Indexes: ', selected_indexes)
                if true_storage:
                    storage_used = storage_used + get_index_actual_storage(working_db_id, output_sel)
                else:
                    storage_used = storage_used + predict_index_storage(output_sel, working_db_id)
                # print('Current Budget: ', storage_used)
                ind_outputs.append(output_sel[1:-1])
                # candidates = [x for x in candidates if x not in new_ind]
                # print('[' + ', '.join(ind_outputs) + ']')
                # update candidates
                # candidates = update_cand_nodes(working_db_id, workload, candidates, '[' + ', '.join(ind_outputs) + ']')
                # print('candidates updated')
                # print(candidates[:3])
                candidates = [x for x in candidates if x[4] <= storage]
            elif 'Revision Agent' in LLM_action:
                LLM_action = 'Revision Agent'
                print('New Action: ', LLM_action)
                prev_steps.append(LLM_action)
                p_rev = revision_agent(schema0, selected_indexes, storage, storage_used)
                output_rev = query_turbo_model(p_rev)
                output_rev = output_rev.split('Index selection: ')[-1]
                # print(output_rev)
                prompts.append(p_rev)
                output_rev = output_rev.replace("'", "")
                print(output_rev)
                if '[]' in output_rev:
                    startover = True
                    selected_indexes = []
                    ind_outputs = []
                    continue
                pattern = r"\[(.*?)\]"
                matches = re.findall(pattern, output_rev)
                if len(matches[0].split('), ')) != 1:
                    inds_selected = [x + ')' if x[-1] != ')' else x for x in matches[0].split('), ')]
                else:
                    inds_selected = [x for x in matches[0].split('), ')]
                selected_indexes = [[x, 'storage:' + str(get_index_actual_storage(working_db_id, '[' + x + ']'))] for x in inds_selected]
                if true_storage:
                    storage_used = get_index_actual_storage(working_db_id, output_rev)
                else:
                    storage_used = predict_index_storage(output_rev, working_db_id)
                ind_outputs = inds_selected
            elif 'Combination Agent' in LLM_action:
                LLM_action = 'Combination Agent'
                print('New Action: ', LLM_action)
                prev_steps.append(LLM_action)
                p_com = combination_agent(candidates, selected_indexes)
                output_com = query_turbo_model(p_com)
                output_com = output_com.split('Index selection: ')[-1]
                # print(output_com)
                prompts.append(p_com)
                output_com = output_com.replace("'", "")
                print(output_com)
                pattern = r"\[(.*?)\]"
                matches = re.findall(pattern, output_com)
                if len(matches[0].split('), ')) != 1:
                    inds_selected = [x + ')' if x[-1] != ')' else x for x in matches[0].split('), ')]
                else:
                    inds_selected = [x for x in matches[0].split('), ')]
                selected_indexes = [[x, 'storage:' + str(get_index_actual_storage(working_db_id, '[' + x + ']'))] for x in inds_selected]
                if true_storage:
                    storage_used = get_index_actual_storage(working_db_id, output_com)
                else:
                    storage_used = predict_index_storage(output_com, working_db_id)
                ind_outputs = inds_selected
            elif 'Stop' in LLM_action:
                print('New Action: ', LLM_action)
                break
            # print('Current Indexes: ', selected_indexes)
            p_ref = reflection_agent(prev_steps)
            suggestion = query_turbo_model(p_ref)
            print('Ind_outputs: ', ind_outputs)
            print('Current Budget: ', storage_used)
            outputs = copy.deepcopy(ind_outputs)
            actions_log.append({'action': LLM_action, 'output': outputs, 'storage': storage_used})
            print('current time: ', time.time() - start_time)
        except Exception as e:
            print('Planning error: ', e)
            continue
    ind_outputs = '[' + ', '.join(ind_outputs) + ']'
    ind_outputs = ind_outputs.replace("'", "")
    return ind_outputs, storage_used, actions_log

