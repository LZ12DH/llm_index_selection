# @TIME : 12/7/23 5:33 PM
# @AUTHOR : LZDH
import copy
import subprocess
import json
import re
from get_postgre import get_postgres

from index_selection_evaluation.selection.cost_evaluation import CostEvaluation
from index_selection_evaluation.selection.dbms.postgres_dbms import PostgresDatabaseConnector
from index_selection_evaluation.selection.index import Index


# example 1 (?)
# sql_input = "SELECT MAX(distinct l_orderkey) FROM lineitem where exists( SELECT MAX(c_custkey) FROM customer where c_custkey = l_orderkey GROUP BY c_custkey )";
# rule_input = ['Aggregate_project_merge'.upper(), 'Project_Merge'.upper()]
# example 2 (good)
# sql_input = "SELECT (SELECT o_orderdate FROM orders limit 1 offset 6 ) AS c0 from region as ref_0 where false limit 76"
# rule_input = ['Sort_Project_Transpose'.upper(), 'Project_To_Calc'.upper()]
# example 3 (good)
# sql_input = "select distinct l_orderkey, sum(l_extendedprice + 3 + (1 - l_discount)) as revenue, o_orderkey, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderkey, o_shippriority order by revenue desc, o_orderkey"
# rule_input = ['Filter_Into_Join'.upper(), 'Project_To_Calc'.upper(), 'Join_extract_Filter'.upper()]
# example 4
# sql_input = "SELECT DISTINCT COUNT ( t3.paperid ) FROM venue AS t4 JOIN paper AS t3 ON t4.venueid  =  t3.venueid JOIN writes AS t2 ON t2.paperid  =  t3.paperid JOIN author AS t1 ON t2.authorid  =  t1.authorid WHERE t1.authorname  =  'David M. Blei' AND t4.venuename  =  'AISTATS'"
# rule_input = ['JOIN_EXTRACT_FILTER'.upper(), 'FILTER_INTO_JOIN'.upper(), 'PROJECT_TO_CALC']
# example 5
# db_id = 'tpch'
# sql_input = "select ps_partkey, sum(ps_supplycost * ps_availqty) as calcite_value from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' ) order by calcite_value desc;;"
# sql_input_1 = "select ps_partkey, sum(ps_supplycost * ps_availqty) as calcite_value from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'AKSJ' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.01000000 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'AKSJ' ) order by calcite_value desc;;"

# sql_input = "SELECT t1.ps_partkey, t1.value FROM (SELECT partsupp.ps_partkey, SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS value FROM partsupp, supplier, nation WHERE partsupp.ps_suppkey = supplier.s_suppkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_name = 'PERU' GROUP BY partsupp.ps_partkey) AS t1 LEFT JOIN (SELECT SUM(partsupp0.ps_supplycost * partsupp0.ps_availqty) * 0.0001000000 AS EXPR0 FROM partsupp AS partsupp0, supplier AS supplier0, nation AS nation0 WHERE partsupp0.ps_suppkey = supplier0.s_suppkey AND supplier0.s_nationkey = nation0.n_nationkey AND nation0.n_name = 'PERU') AS t5 ON TRUE WHERE t1.value > t5.EXPR0 ORDER BY t1.value DESC;"

# node_str = "LogicalFilter(condition=[AND(=($7, 'CProxy'), >($5, 1998))]): rowcount = 4.221408232700325E11, cumulative cost = {6.051060523576132E12 rows, 5.6285445353031E12 cpu, 0.0 io}, id = 17"


def get_pg_explain(db_id, sql_input):
    sql_input = 'EXPLAIN ' + sql_input
    plan, err = get_postgres(db_id, sql_input)
    return plan


def get_pg_explain_dict(db_id, sql_input):
    # print(sql_input)
    sql_input = 'EXPLAIN (FORMAT JSON) ' + sql_input
    plan, err = get_postgres(db_id, sql_input)
    return plan


def nested_tree_pg(plan_nodes, text=True):
    # print(plan_nodes)
    if len(plan_nodes) <= 3:
        plan_nodes = ['  '.join(x.split('  ')[1:]) if x[0].isdigit() else x for x in plan_nodes]
        new_nodes = []
        if not text:
            new_nodes = [re.sub(r'\(cost.*?\)', '(' + re.search(r'rows=\d+', x).group(0) + ')', x) for x in plan_nodes]
        else:
            for n in plan_nodes:
                cardinality = re.search(r'\((.*?)\)', n).group(1)
                cardinality = 'with ' + cardinality.split(' ')[1].replace('rows=', '') + ' rows'
                operator = n.split('  ')[0] + ' operator '
                if len(n.split('  ')) >= 3:
                    cond = ' with ' + ' '.join(n.split('  ')[3:])
                else:
                    cond = ''
                col_card = '  '.join([operator, cardinality, cond])
                new_nodes.append(col_card)
        return new_nodes
    else:
        if plan_nodes[0][0].isdigit():
            root = '  '.join(plan_nodes[0].split('  ')[1:])
        else:
            root = plan_nodes[0]
        if not text:
            root = re.sub(r'\(cost.*?\)', '(' + re.search(r'rows=\d+', root).group(0) + ')', root)
        else:
            cardinality = re.search(r'\((.*?)\)', root).group(1)
            cardinality = 'with ' + cardinality.split(' ')[1].replace('rows=', '') + ' rows'
            operator = root.split('  ')[0] + ' operator '
            if len(root.split('  ')) >= 3:
                cond = ' with ' + ' '.join(root.split('  ')[3:])
            else:
                cond = ''
            root = '  '.join([operator, cardinality, cond])
        # print([x for x in plan_nodes[1:] if x.startswith('22')])
        heights = [int(s) for s in set([x.split('  ')[0] for x in plan_nodes[1:] if x[0].isdigit()])]
        heights.sort()
        # print(heights)
        subtree_h = heights[0]
        # print(subtree_h)
        subtree_inds = [i for i, x in enumerate(plan_nodes) if x.startswith(str(subtree_h) + '  ')]
        # print(subtree_inds)
        if len(subtree_inds) == 1:
            left_ind = subtree_inds[0]
            left_subtree = nested_tree_pg(plan_nodes[left_ind:])
            right_subtree = []
            return [root, left_subtree, right_subtree]
        elif len(subtree_inds) == 2:
            left_ind = subtree_inds[0]
            right_ind = subtree_inds[1]
            left_subtree = nested_tree_pg(plan_nodes[left_ind:right_ind])
            right_subtree = nested_tree_pg(plan_nodes[right_ind:])
            return [root, left_subtree, right_subtree]
        # tpcds has non-binary plan tree
        elif len(subtree_inds) == 3:
            left_ind = subtree_inds[0]
            middle_ind = subtree_inds[1]
            right_ind = subtree_inds[2]
            left_subtree = nested_tree_pg(plan_nodes[left_ind:middle_ind])
            mid_subtree = nested_tree_pg(plan_nodes[middle_ind:right_ind])
            right_subtree = nested_tree_pg(plan_nodes[right_ind:])
            return [root, left_subtree, mid_subtree, right_subtree]


# Todo: Refine by defining a node class
def node_to_template(node, all_cols, col_dict):
    cols = []
    operator = node.split(' (')[0]
    if 'Scan' in node:
        operator = node.split(' on ')[0]
        operator = operator.split(' using ')[0]
    operator = operator.split('  ')[0] + '  {"operator": "' + operator.split('  ')[1] + '",'
    if 'Sort' in node:
        cols = [x.split('.')[-1] for x in node.split('Sort Key: ')[-1].split(', ')]
    elif 'Hash Join' in node:
        cols = [x.split('.')[-1] for x in node.split('Hash Cond: (')[-1].split(')')[0].split(' = ')]
    elif 'Scan' in node:
        if 'Filter' in node:
            cols = [x for x in all_cols if x in node.split('Filter: (')[-1]]
    elif 'Group Key' in node:
        cols = [x.split('.')[-1] for x in node.split('Group Key: ')[-1].split(', ')]
    dum_cols = []
    for col in cols:
        if col in col_dict.keys():
            dum_cols.append(col_dict[col])
        else:
            new_dum = 'col ' + str(len(list(col_dict.keys())))
            dum_cols.append(new_dum)
            col_dict[col] = new_dum
    cols = '"cols": "' + str(dum_cols) + '"}'
    return operator + cols, col_dict


def nested_template_pg(plan_nodes, text=True):
    if len(plan_nodes) <= 3:
        plan_nodes = ['  '.join(x.split('  ')[1:]) if x[0].isdigit() else x for x in plan_nodes]
        return plan_nodes
    else:
        if plan_nodes[0][0].isdigit():
            root = '  '.join(plan_nodes[0].split('  ')[1:])
        else:
            root = plan_nodes[0]
        heights = [int(s) for s in set([x.split('  ')[0] for x in plan_nodes[1:] if x[0].isdigit()])]
        heights.sort()
        subtree_h = heights[0]
        subtree_inds = [i for i, x in enumerate(plan_nodes) if x.startswith(str(subtree_h) + '  ')]
        if len(subtree_inds) == 1:
            left_ind = subtree_inds[0]
            left_subtree = nested_template_pg(plan_nodes[left_ind:])
            right_subtree = []
            return [root, left_subtree, right_subtree]
        elif len(subtree_inds) == 2:
            left_ind = subtree_inds[0]
            right_ind = subtree_inds[1]
            left_subtree = nested_template_pg(plan_nodes[left_ind:right_ind])
            right_subtree = nested_template_pg(plan_nodes[right_ind:])
            return [root, left_subtree, right_subtree]
        # tpcds has non-binary plan tree
        elif len(subtree_inds) == 3:
            left_ind = subtree_inds[0]
            middle_ind = subtree_inds[1]
            right_ind = subtree_inds[2]
            left_subtree = nested_template_pg(plan_nodes[left_ind:middle_ind])
            mid_subtree = nested_template_pg(plan_nodes[middle_ind:right_ind])
            right_subtree = nested_template_pg(plan_nodes[right_ind:])
            return [root, left_subtree, mid_subtree, right_subtree]


def get_tree_pg(db_id, sql_input, text=True):
    plan_raw = [re.sub(r' {2,}->', lambda m: str(len(m.group())), x[0]) for x in get_pg_explain(db_id, sql_input)]
    # print(sql_input)
    plan_nodes = []
    while plan_raw:
        item = plan_raw.pop(0)
        if plan_raw:
            while not plan_raw[0][0].isdigit():
                sub_item = plan_raw.pop(0)
                item = item + '  ' + sub_item
                if not plan_raw:
                    break
        plan_nodes.append(item)
    plan_nodes = [re.sub(r' {4,}', lambda m: '    ', x) for x in plan_nodes]
    # print(plan_nodes)
    return nested_tree_pg(plan_nodes, text)


def get_col_card(db_id, sql_input, col_name):
    cards = []
    plan_raw = [re.sub(r' {2,}->', lambda m: str(len(m.group())), x[0]) for x in get_pg_explain(db_id, sql_input)]
    plan_nodes = []
    while plan_raw:
        item = plan_raw.pop(0)
        if plan_raw:
            while not plan_raw[0][0].isdigit():
                sub_item = plan_raw.pop(0)
                item = item + '  ' + sub_item
                if not plan_raw:
                    break
        plan_nodes.append(item)
    plan_nodes = [re.sub(r' {4,}', lambda m: '    ', x) for x in plan_nodes]
    plan_nodes = ['  '.join(x.split('  ')[1:]) if x[0].isdigit() else x for x in plan_nodes]
    for item in plan_nodes:
        if col_name in item:
            cards.append(item)
    return cards


def get_col_subtree(db_id, sql_input, col_name):
    plan_raw = [re.sub(r' {2,}->', lambda m: str(len(m.group())), x[0]) for x in get_pg_explain(db_id, sql_input)]
    print(get_pg_explain(db_id, sql_input))
    print(plan_raw)
    plan_nodes = []
    while plan_raw:
        item = plan_raw.pop(0)
        if plan_raw:
            while not plan_raw[0][0].isdigit():
                sub_item = plan_raw.pop(0)
                item = item + '  ' + sub_item
                if not plan_raw:
                    break
        plan_nodes.append(item)
    plan_nodes = [re.sub(r' {4,}', lambda m: '    ', x) for x in plan_nodes]
    plan_nodes[0] = '0  ' + plan_nodes[0]
    subtrees = []
    for i in range(len(plan_nodes)):
        if col_name in plan_nodes[i]:
            col = copy.deepcopy(plan_nodes[i])
            subtree = [col]
            if col != plan_nodes[-1]:
                j = 1
                next_item = copy.deepcopy(plan_nodes[i+j])
                while int(next_item.split('  ')[0]) > int(col.split('  ')[0]):
                    subtree.append(next_item)
                    j += 1
                    if next_item != plan_nodes[-1]:
                        next_item = copy.deepcopy(plan_nodes[i + j])
                    else:
                        break
            subtrees.append(subtree)
    return [nested_tree_pg(x) for x in subtrees]


def get_col_template(db_id, sql_input, col_name, all_cols):
    plan_raw = get_pg_explain(db_id, sql_input)
    print(plan_raw)
    plan_nodes = []
    while plan_raw:
        item = plan_raw.pop(0)
        if plan_raw:
            while not plan_raw[0][0].isdigit():
                sub_item = plan_raw.pop(0)
                item = item + '  ' + sub_item
                if not plan_raw:
                    break
        plan_nodes.append(item)
    plan_nodes = [re.sub(r' {4,}', lambda m: '    ', x) for x in plan_nodes]
    plan_nodes[0] = '0  ' + plan_nodes[0]
    # print('plan_nodes: ', plan_nodes)
    # print(plan_nodes)
    # plan_nodes = [node_to_template(node, all_cols, col_dict) for node in plan_nodes]
    subtrees = []
    for i in range(len(plan_nodes)):
        if col_name in plan_nodes[i]:
            col = copy.deepcopy(plan_nodes[i])
            subtree = [col]
            if col != plan_nodes[-1]:
                j = 1
                next_item = copy.deepcopy(plan_nodes[i+j])
                while int(next_item.split('  ')[0]) > int(col.split('  ')[0]):
                    subtree.append(next_item)
                    j += 1
                    if next_item != plan_nodes[-1]:
                        next_item = copy.deepcopy(plan_nodes[i + j])
                    else:
                        break
            # every subtree has its own dummy dict
            col_dict = {col_name: 'col 0'}
            subtree0 = []
            for node in subtree:
                new_node, col_dict = node_to_template(node, all_cols, col_dict)
                subtree0.append(new_node)
            subtree = subtree0
            # col_name = 'col 0'
            subtrees.append(subtree)
            # print('subtrees: ', subtrees)
            # print(len(subtrees))
            break
    return [nested_template_pg(x) for x in subtrees]


def col_in(node, col_name):
    if col_name in str(node):
        return True
    else:
        return False


def dict_plan_locate_col(dict_plan, col_name):
    if 'Plans' not in list(dict_plan.keys()):
        if col_in(dict_plan, col_name):
            return [dict_plan]
        else:
            return []
    else:
        dict_plan_c = copy.deepcopy(dict_plan)
        del dict_plan_c['Plans']
        if col_in(dict_plan_c, col_name):
            return [dict_plan]
        else:
            left_subtree = dict_plan['Plans'][0]
            result = dict_plan_locate_col(left_subtree, col_name)
            if len(dict_plan['Plans']) >= 2:
                right_subtree = dict_plan['Plans'][-1]
                result = result + dict_plan_locate_col(right_subtree, col_name)
            if len(dict_plan['Plans']) == 3:
                middle_subtree = dict_plan['Plans'][1]
                result = result + dict_plan_locate_col(middle_subtree, col_name)
            return result


def find_power_of_ten(num):
    if num == 0:
        return 0
    n = 1
    while num // 10 >= 1:  # Keep dividing by 10 to find the power of 10
        num /= 10
        n += 1
    return n


# def format_digits(root):
#     for key, value in root.items():
#         if str(value).isdigit():
#             power = find_power_of_ten(float(value))
#             root[key] = power
#     if 'Plans' not in list(root.keys()):
#
#     return root


def traverse_query_plan(root):
    for key, value in root.items():
        if isinstance(value, float) or (isinstance(value, int) and not isinstance(value, bool)):
            power = find_power_of_ten(value)
            root[key] = power
    cond_list = ['Hash Cond', 'Sort Key', 'Hash Keys', 'Group Keys', 'Filter']
    relation_list = ['Relation Name', 'Alias']
    for k in list(root.keys()):
        if k in relation_list:
            root[k] = 'tab_name'
        elif k in cond_list:
            cond_cols = []
            for c in all_cols:
                related_cols_l = list(related_cols.keys())
                if c in root[k] and c in related_cols_l:
                    cond_cols.append(related_cols[c])
                elif c in root[k] and c not in related_cols_l:
                    related_cols[c] = 'col ' + str(len(related_cols_l))
                    cond_cols.append(related_cols[c])
            root[k] = cond_cols
    if 'Plans' not in list(root.keys()):
        return [root]
    else:
        left_subtree = root['Plans'][0]
        processing_traverse_plan(left_subtree, col_name, related_cols, all_cols)
        if len(root['Plans']) >= 2:
            right_subtree = root['Plans'][-1]
            processing_traverse_plan(right_subtree, col_name, related_cols, all_cols)
        if len(root['Plans']) == 3:
            middle_subtree = root['Plans'][1]
            processing_traverse_plan(middle_subtree, col_name, related_cols, all_cols)
        return [root]


def get_query_template(db_id, sql_input):
    plan_raw = get_pg_explain_dict(db_id, sql_input)
    plan_dict = plan_raw[0][0][0]['Plan']
    # print(sql_input)
    templates_raw = dict_plan_locate_col(plan_dict, col_name)
    # related_cols = [{} for i in range(len(templates_raw))]
    results = []
    for x in templates_raw:
        related_cols = {}
        template = processing_traverse_plan(x, col_name, related_cols, all_cols)
        results.append((template, related_cols[col_name]))
    return results


def processing_traverse_plan(root, col_name, related_cols, all_cols):
    for key, value in root.items():
        if isinstance(value, float) or (isinstance(value, int) and not isinstance(value, bool)):
            power = find_power_of_ten(value)
            root[key] = power
    cond_list = ['Hash Cond', 'Sort Key', 'Hash Keys', 'Group Keys', 'Filter']
    relation_list = ['Relation Name', 'Alias']
    for k in list(root.keys()):
        if k in relation_list:
            root[k] = 'tab_name'
        elif k in cond_list:
            cond_cols = []
            for c in all_cols:
                related_cols_l = list(related_cols.keys())
                if c in root[k] and c in related_cols_l:
                    cond_cols.append(related_cols[c])
                elif c in root[k] and c not in related_cols_l:
                    related_cols[c] = 'col ' + str(len(related_cols_l))
                    cond_cols.append(related_cols[c])
            root[k] = cond_cols
    if 'Plans' not in list(root.keys()):
        return [root]
    else:
        left_subtree = root['Plans'][0]
        processing_traverse_plan(left_subtree, col_name, related_cols, all_cols)
        if len(root['Plans']) >= 2:
            right_subtree = root['Plans'][-1]
            processing_traverse_plan(right_subtree, col_name, related_cols, all_cols)
        if len(root['Plans']) == 3:
            middle_subtree = root['Plans'][1]
            processing_traverse_plan(middle_subtree, col_name, related_cols, all_cols)
        return [root]


def get_col_template_dict(db_id, sql_input, col_name, all_cols):
    plan_raw = get_pg_explain_dict(db_id, sql_input)
    plan_dict = plan_raw[0][0][0]['Plan']
    # print(sql_input)
    templates_raw = dict_plan_locate_col(plan_dict, col_name)
    # related_cols = [{} for i in range(len(templates_raw))]
    results = []
    for x in templates_raw:
        related_cols = {}
        template = processing_traverse_plan(x, col_name, related_cols, all_cols)
        results.append((template, related_cols[col_name]))
    return results


def get_row_type(db_id, col_name, tab_name=''):
    with open('data/schemas/' + db_id + '.json') as f_sch:
        data = f_sch.read()
        schema = json.loads(data)
    col_type = 'NA'
    col_row = 'NA'
    for tab in schema:
        if tab_name and tab['table'] == tab_name:
            col_row = tab['rows']
            columns = tab['columns']
            for c in columns:
                if c['name'] == col_name:
                    col_type = c['type']
                    break
            break
        else:
            columns = tab['columns']
            for c in columns:
                if c['name'] == col_name:
                    col_type = c['type']
                    col_row = tab['rows']
                    break
    return col_type, col_row


# Todo: This could be improved by passing index candidates as input
# we make the storage estimation in gigabytes
def predict_index_sizes(column_combinations, database_name):
    connector = PostgresDatabaseConnector(database_name, autocommit=True)
    connector.drop_indexes()

    cost_evaluation = CostEvaluation(connector)

    predicted_index_sizes = []

    parent_index_size_map = {}
    for column_combination in column_combinations:
        potential_index = Index(column_combination)
        # print(column_combination)
        # print(potential_index.columns)
        cost_evaluation.what_if.simulate_index(potential_index, True)

        full_index_size = potential_index.estimated_size
        index_delta_size = full_index_size
        # if len(column_combination) > 1:
        #     # print(predict_index_sizes([[x] for x in column_combination], database_name))
        #     # print(index_delta_size)
        #     # index_delta_size = index_delta_size - predict_index_sizes([[x] for x in column_combination], database_name) * 1024 * 1024
        #     # print(index_delta_size)
        #     index_delta_size -= parent_index_size_map[str(column_combination)]

        predicted_index_sizes.append(index_delta_size)
        cost_evaluation.what_if.drop_simulated_index(potential_index)

        parent_index_size_map[str(column_combination)] = full_index_size

    return sum([x / 1024 / 1024 for x in predicted_index_sizes])


# db_id = 'job'
# with open('data/schemas/' + db_id + '.json') as f_sch:
#     data = f_sch.read()
#     schema = json.loads(data)
# all_cols = [y['name'] for x in schema for y in x['columns']]
# # sql_input = "with ssr as  (select s_store_id,         sum(sales_price) as sales,         sum(profit) as profit,         sum(return_amt) as returns,         sum(net_loss) as profit_loss  from   ( select  ss_store_sk as store_sk,             ss_sold_date_sk  as date_sk,             ss_ext_sales_price as sales_price,             ss_net_profit as profit,             cast(0 as decimal(7,2)) as return_amt,             cast(0 as decimal(7,2)) as net_loss     from store_sales     union all     select sr_store_sk as store_sk,            sr_returned_date_sk as date_sk,            cast(0 as decimal(7,2)) as sales_price,            cast(0 as decimal(7,2)) as profit,            sr_return_amt as return_amt,            sr_net_loss as net_loss     from store_returns    ) salesreturns,      date_dim,      store  where date_sk = d_date_sk        and d_date between cast('2001-08-11' as date)                    and (cast('2001-08-11' as date) +  interval '14 days')        and store_sk = s_store_sk  group by s_store_id)  ,  csr as  (select cp_catalog_page_id,         sum(sales_price) as sales,         sum(profit) as profit,         sum(return_amt) as returns,         sum(net_loss) as profit_loss  from   ( select  cs_catalog_page_sk as page_sk,             cs_sold_date_sk  as date_sk,             cs_ext_sales_price as sales_price,             cs_net_profit as profit,             cast(0 as decimal(7,2)) as return_amt,             cast(0 as decimal(7,2)) as net_loss     from catalog_sales     union all     select cr_catalog_page_sk as page_sk,            cr_returned_date_sk as date_sk,            cast(0 as decimal(7,2)) as sales_price,            cast(0 as decimal(7,2)) as profit,            cr_return_amount as return_amt,            cr_net_loss as net_loss     from catalog_returns    ) salesreturns,      date_dim,      catalog_page  where date_sk = d_date_sk        and d_date between cast('2001-08-11' as date)                   and (cast('2001-08-11' as date) +  interval '14 days')        and page_sk = cp_catalog_page_sk  group by cp_catalog_page_id)  ,  wsr as  (select web_site_id,         sum(sales_price) as sales,         sum(profit) as profit,         sum(return_amt) as returns,         sum(net_loss) as profit_loss  from   ( select  ws_web_site_sk as wsr_web_site_sk,             ws_sold_date_sk  as date_sk,             ws_ext_sales_price as sales_price,             ws_net_profit as profit,             cast(0 as decimal(7,2)) as return_amt,             cast(0 as decimal(7,2)) as net_loss     from web_sales     union all     select ws_web_site_sk as wsr_web_site_sk,            wr_returned_date_sk as date_sk,            cast(0 as decimal(7,2)) as sales_price,            cast(0 as decimal(7,2)) as profit,            wr_return_amt as return_amt,            wr_net_loss as net_loss     from web_returns left outer join web_sales on          ( wr_item_sk = ws_item_sk            and wr_order_number = ws_order_number)    ) salesreturns,      date_dim,      web_site  where date_sk = d_date_sk        and d_date between cast('2001-08-11' as date)                   and (cast('2001-08-11' as date) +  interval '14 days')        and wsr_web_site_sk = web_site_sk  group by web_site_id)   select  channel         , id         , sum(sales) as sales         , sum(returns) as returns         , sum(profit) as profit  from   (select 'store channel' as channel         , 'store' || s_store_id as id         , sales         , returns         , (profit - profit_loss) as profit  from   ssr  union all  select 'catalog channel' as channel         , 'catalog_page' || cp_catalog_page_id as id         , sales         , returns         , (profit - profit_loss) as profit  from  csr  union all  select 'web channel' as channel         , 'web_site' || web_site_id as id         , sales         , returns         , (profit - profit_loss) as profit  from   wsr  ) x  group by rollup (channel, id)  order by channel          ,id  limit 100;  "
# sql_input = "SELECT MIN(chn.name) AS uncredited_voiced_character,        MIN(t.title) AS russian_movie FROM char_name AS chn,      cast_info AS ci,      company_name AS cn,      company_type AS ct,      movie_companies AS mc,      role_type AS rt,      title AS t WHERE ci.note LIKE '%(voice)%'   AND ci.note LIKE '%(uncredited)%'   AND cn.country_code = '[ru]'   AND rt.role = 'actor'   AND t.production_year > 2005   AND t.id = mc.movie_id   AND t.id = ci.movie_id   AND ci.movie_id = mc.movie_id   AND chn.id = ci.person_role_id   AND rt.id = ci.role_id   AND cn.id = mc.company_id   AND ct.id = mc.company_type_id"
# # print(get_col_template_dict(db_id, sql_input, 'name', all_cols))
# print(get_pg_explain_dict(db_id, sql_input))

# db_id = 'tpch'
# s = "select nation, max(o_year), sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%indian%' ) as profit group by nation, o_year order by nation, o_year desc ;"
# p = get_pg_explain_dict(db_id, s)[0][0][0]['Plan']
# print(p)
# with open('../example_dict_plan.json', 'w+') as f_sch:
#     ps = json.dumps(p, indent=2)
#     f_sch.write(ps)

#  [item(i_current_price), web_sales(ws_sales_price), date_dim(d_year), date_dim(d_week_seq), date_dim(d_date), household_demographics(hd_demo_sk)]
# costs = predict_index_sizes([['date_dim.d_week_seq'], ['item.i_item_sk'], ['store_sales.ss_store_sk']], 'tpcds')
# print(costs)
# 1917.5234375
