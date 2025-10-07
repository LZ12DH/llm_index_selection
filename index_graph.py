# @TIME : 5/3/25 6:24 PM
# @AUTHOR : LZDH
import copy
import json
import tqdm
import torch.nn as nn
import torch.nn.functional as F
from collections import defaultdict
# from sentence_transformers import SentenceTransformer
from get_query_meta import *
from eval_index import *
from index_candidate_generation.distill_model.pre_filter_data import get_utility
from index_candidate_generation.distill_model.distill_utils.distill_workload import Index


def get_all_tabs_cols(db_id):
    with open('data/schemas/' + db_id + '.json') as f_sch:
        data = f_sch.read()
        schema = json.loads(data)
    all_tabs = [x['table'] for x in schema]
    all_cols = {}
    for t in all_tabs:
        cols_t = [y['name'] for x in schema for y in x['columns'] if x['table'] == t]
        for c in cols_t:
            all_cols[c] = t
    return all_tabs, all_cols


def get_cand_nodes(db_id, working_db_id, workload, selected_list):
    all_tabs, all_cols = get_all_tabs_cols(db_id)
    all_cand_nodes = []
    query_paths = []
    original_cost = eval_index_hypo(working_db_id, workload, '')
    utility_dict = {}
    if selected_list:
        for selected_cand in selected_list:
            utility_dict[selected_cand] = -1
    # print(len(workload))
    i = 0
    # print(i)
    for query in workload:
        # print(query)
        try:
            indexable_cols = []
            for col in list(all_cols.keys()):
                if col in query:
                    indexable_cols.append(col)
            # print(indexable_cols)
            plan = get_pg_explain_dict(working_db_id, query)[0][0][0]['Plan']
            node_list_q = []
            # print(plan)
            cand_nodes_q = tranverse_plan(plan, indexable_cols, all_cols, node_list_q)
            if selected_list:
                node_list_q = [x for x in node_list_q if x[0] in selected_list]
            # add utility to nodes
            for item in node_list_q:
                col = item[0].split('.')[1]
                tab = item[0].split('.')[0]
                if utility_dict and item[0] in list(utility_dict.keys()) and utility_dict[item[0]] != -1:
                    item_cost = utility_dict[item[0]]
                else:
                    item_cost = eval_index_hypo(working_db_id, workload, '[' + tab + '(' + col + ')' + ']')
                    utility_dict[item[0]] = item_cost
                utility = original_cost - item_cost
                # print(utility)
                # utility = get_col_utility(db_id, col, tab, plan)
                cost = predict_index_sizes([[item[0]]], working_db_id)
                # print(cost)
                # utillity devided by storage cost
                utility = utility / cost
                item.append(utility)
                item.append(cost)
            all_cand_nodes = all_cand_nodes + node_list_q
            query_paths.append(cand_nodes_q)
        except Exception as error1:
            print('error in node encoding for query ', i)
            print(error1)
            continue
        i += 1
        print('processed queries: ', i)

    input_nodes = merge_nodes(all_cand_nodes)
    return input_nodes


def update_cand_nodes(working_db_id, workload, candidates, ind_selected):
    original_cost = eval_index_hypo(working_db_id, workload, ind_selected)
    for cand in candidates:
        ind_name = cand[0]
        total_utility = 0
        try:
            col = ind_name.split('.')[1]
            tab = ind_name.split('.')[0]
            item_cost = eval_index_hypo(working_db_id, workload, '[' + tab + '(' + col + ')' + ']')
            utility = original_cost - item_cost
            total_utility = total_utility + utility
        except Exception as error1:
            print('error in node updating')
            print(error1)
            continue
        utility = total_utility / cand[4]
        cand[3] = utility
    return candidates


def merge_nodes(cand_nodes):
    # cand_nodes = [tab.col, node_type, num_rows, utility, storage]
    data_dict = defaultdict(lambda: [0, 0, 0, 0, ''])  # { (tab.col, storage): [node_type, num_rows, utility] }
    for first, node_type, num_rows, utility, last in cand_nodes:
        data_dict[(first, last)][0] += num_rows
        data_dict[(first, last)][1] += 1
        data_dict[(first, last)][2] += utility
        data_dict[(first, last)][3] += 1
        if data_dict[(first, last)][4] == '':
            data_dict[(first, last)][4] = node_type
        elif node_type not in data_dict[(first, last)][4]:
            data_dict[(first, last)][4] = ', '.join([data_dict[(first, last)][4], node_type])

    merged_nodes = [[key[0], node_type, total_rows / count1, total_utility, key[1]] for key, (total_rows, count1, total_utility, count2, node_type) in data_dict.items()]
    return merged_nodes


def tranverse_plan(plan, indexable_cols, all_cols, node_list):
    if 'Plans' not in list(plan.keys()):
        cand_nodes = extract_node_feat(plan, all_cols)
        if cand_nodes:
            for x in cand_nodes:
                node_list.append(x)
        return cand_nodes
    else:
        subtrees = plan['Plans']
        cand_nodes = extract_node_feat(plan, all_cols)
        if cand_nodes:
            for x in cand_nodes:
                node_list.append(x)
        return cand_nodes + [tranverse_plan(x,  indexable_cols, all_cols, node_list) for x in subtrees]


def extract_node_feat(plan_node, all_cols):
    flags = ['Filter', 'Hash Cond', 'Group Key', 'Sort Key']
    plan_rows = plan_node['Plan Rows']
    node_type = plan_node['Node Type']
    conds = []
    for cond in flags:
        if cond in list(plan_node.keys()):
            if isinstance(plan_node[cond], list):
                conds = conds + plan_node[cond]
            else:
                conds.append(plan_node[cond])
    col_nodes = []
    if conds:
        for c in list(all_cols.keys()):
            for k in conds:
                if c in k:
                    new_node = [all_cols[c] + '.' + c, node_type, plan_rows]
                    col_nodes.append(new_node)
    return col_nodes


class Node:
    def __init__(self, col_name, tab_name, col_min, col_max, card, node_dict):
        self.col_name = col_name
        self.tab_name = tab_name

        self.col_min = col_min
        self.col_max = col_max
        self.card = card
        self.col_type, self.selectivity = self.extract_type_selectivity(node_dict)

    def extract_type_selectivity(self, node_dict):
        node_type = node_dict['Node Type']
        selectivity = node_dict['Plan Rows']
        return node_type, selectivity


def get_col_utility(db_id, col, tab, plan):
    schema_load = f"data/schemas/{db_id}.json"
    with open(schema_load, "r") as rf:
        schema = json.load(rf)
    row = dict()
    for item in schema:
        row[item["table"]] = item["rows"]
    root = plan
    table = tab
    columns = [col]
    index = Index(columns, table)
    feat = get_utility(root, index, row)
    return feat

# class Encoding:
#     def __init__(self, ds_info):
#         self.column_min_max_vals = ds_info.column_min_max_vals
#         self.col2idx = {'NA': 0}
#         self.op2idx = {'>': 0, '=': 1, '<': 2}
#         self.type2idx = {'NA': 0}
#         self.join2idx = {'NA': 0}
#
#         self.table2idx = {'NA': 0}
#
#     def is_number(s):
#         try:
#             float(s)
#             return True
#         except ValueError:
#             return False
#
#     def normalize_val(self, column, val):
#         if column not in self.column_min_max_vals or (not is_number(val)):
#             # print('column {} not in col_min_max'.format(column))
#             return 0.
#         mini, maxi = self.column_min_max_vals[column]
#         val_norm = 0.
#         val = float(val)
#         if maxi > mini:
#             val_norm = (val - mini) / (maxi - mini)
#         return val_norm
#
#     def encode_join(self, join):
#         if join not in self.join2idx:
#             self.join2idx[join] = len(self.join2idx)
#         return self.join2idx[join]
#
#     def encode_table(self, table):
#         if table not in self.table2idx:
#             self.table2idx[table] = len(self.table2idx)
#         return self.table2idx[table]
#
#     def encode_type(self, nodeType):
#         if nodeType not in self.type2idx:
#             self.type2idx[nodeType] = len(self.type2idx)
#         return self.type2idx[nodeType]
#
#     def encode_op(self, op):
#         if op not in self.op2idx:
#             self.op2idx[op] = len(self.op2idx)
#         return self.op2idx[op]
#
#     def encode_col(self, col):
#         if col not in self.col2idx:
#             self.col2idx[col] = len(self.col2idx)
#         return self.col2idx[col]


# db_id = 'tpch'
# all_tabs, all_cols = get_all_tabs_cols(db_id)
# workload = ["-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nl_orderkey,\nsum(l_extendedprice * (1 - l_discount)) as revenue,\no_orderdate,\no_shippriority\nfrom\ncustomer,\norders,\nlineitem\nwhere\nc_mktsegment = 'BUILDING'\nand c_custkey = o_custkey\nand l_orderkey = o_orderkey\nand o_orderdate < date '1995-03-23'\nand l_shipdate > date '1995-03-23'\ngroup by\nl_orderkey,\no_orderdate,\no_shippriority\norder by\nrevenue desc,\no_orderdate limit 10;\n-- $ID$\n-- TPC-H/TPC-R Order Priority Checking ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\no_orderpriority,\ncount(*) as order_count\nfrom\norders\nwhere\no_orderdate >= date '1993-01-01'\nand o_orderdate < date '1993-01-01' + interval '3' month\nand exists (\nselect\n*\nfrom\nlineitem\nwhere\nl_orderkey = o_orderkey\nand l_commitdate < l_receiptdate\n)\ngroup by\no_orderpriority\norder by\no_orderpriority ;\n-- $ID$\n-- TPC-H/TPC-R Local Supplier Volume ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nn_name,\nsum(l_extendedprice * (1 - l_discount)) as revenue\nfrom\ncustomer,\norders,\nlineitem,\nsupplier,\nnation,\nregion\nwhere\nc_custkey = o_custkey\nand l_orderkey = o_orderkey\nand l_suppkey = s_suppkey\nand c_nationkey = s_nationkey\nand s_nationkey = n_nationkey\nand n_regionkey = r_regionkey\nand r_name = 'AFRICA'\nand o_orderdate >= date '1997-01-01'\nand o_orderdate < date '1997-01-01' + interval '1' year\ngroup by\nn_name\norder by\nrevenue desc ;\n-- $ID$\n-- TPC-H/TPC-R Forecasting Revenue Change ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nsupp_nation,\ncust_nation,\nl_year,\nsum(volume) as revenue\nfrom\n(\nselect\nn1.n_name as supp_nation,\nn2.n_name as cust_nation,\nextract(year from l_shipdate) as l_year,\nl_extendedprice * (1 - l_discount) as volume\nfrom\nsupplier,\nlineitem,\norders,\ncustomer,\nnation n1,\nnation n2\nwhere\ns_suppkey = l_suppkey\nand o_orderkey = l_orderkey\nand c_custkey = o_custkey\nand s_nationkey = n1.n_nationkey\nand c_nationkey = n2.n_nationkey\nand (\n(n1.n_name = 'UNITED STATES' and n2.n_name = 'ALGERIA')\nor (n1.n_name = 'ALGERIA' and n2.n_name = 'UNITED STATES')\n)\nand l_shipdate between date '1995-01-01' and date '1996-12-31'\n) as shipping\ngroup by\nsupp_nation,\ncust_nation,\nl_year\norder by\nsupp_nation,\ncust_nation,\nl_year ;\n-- $ID$\n-- TPC-H/TPC-R National Market Share ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\no_year,\nsum(case\nwhen nation = 'VIETNAM' then volume\nelse 0\nend) / sum(volume) as mkt_share\nfrom\n(\nselect\nextract(year from o_orderdate) as o_year,\nl_extendedprice * (1 - l_discount) as volume,\nn2.n_name as nation\nfrom\npart,\nsupplier,\nlineitem,\norders,\ncustomer,\nnation n1,\nnation n2,\nregion\nwhere\np_partkey = l_partkey\nand s_suppkey = l_suppkey\nand l_orderkey = o_orderkey\nand o_custkey = c_custkey\nand c_nationkey = n1.n_nationkey\nand n1.n_regionkey = r_regionkey\nand r_name = 'ASIA'\nand s_nationkey = n2.n_nationkey\nand o_orderdate between date '1995-01-01' and date '1996-12-31'\nand p_type = 'STANDARD ANODIZED COPPER'\n) as all_nations\ngroup by\no_year\norder by\no_year ;\n-- $ID$\n-- TPC-H/TPC-R Product Type Profit Measure ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nnation,\no_year,\nsum(amount) as sum_profit\nfrom\n(\nselect\nn_name as nation,\nextract(year from o_orderdate) as o_year,\nl_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\nfrom\npart,\nsupplier,\nlineitem,\npartsupp,\norders,\nnation\nwhere\ns_suppkey = l_suppkey\nand ps_suppkey = l_suppkey\nand ps_partkey = l_partkey\nand p_partkey = l_partkey\nand o_orderkey = l_orderkey\nand s_nationkey = n_nationkey\nand p_name like '%peru%'\n) as profit\ngroup by\nnation,\no_year\norder by\nnation,\no_year desc ;\n-- $ID$\n-- TPC-H/TPC-R Returned Item Reporting ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nps_partkey,\nsum(ps_supplycost * ps_availqty) as value\nfrom\npartsupp,\nsupplier,\nnation\nwhere\nps_suppkey = s_suppkey\nand s_nationkey = n_nationkey\nand n_name = 'INDONESIA'\ngroup by\nps_partkey having\nsum(ps_supplycost * ps_availqty) > (\nselect\nsum(ps_supplycost * ps_availqty) * 0.0000100000\nfrom\npartsupp,\nsupplier,\nnation\nwhere\nps_suppkey = s_suppkey\nand s_nationkey = n_nationkey\nand n_name = 'INDONESIA'\n)\norder by\nvalue desc ;\n-- $ID$\n-- TPC-H/TPC-R Shipping Modes and Order Priority ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nl_shipmode,\nsum(case\nwhen o_orderpriority = '1-URGENT'\nor o_orderpriority = '2-HIGH'\nthen 1\nelse 0\nend) as high_line_count,\nsum(case\nwhen o_orderpriority <> '1-URGENT'\nand o_orderpriority <> '2-HIGH'\nthen 1\nelse 0\nend) as low_line_count\nfrom\norders,\nlineitem\nwhere\no_orderkey = l_orderkey\nand l_shipmode in ('TRUCK', 'SHIP')\nand l_commitdate < l_receiptdate\nand l_shipdate < l_commitdate\nand l_receiptdate >= date '1993-01-01'\nand l_receiptdate < date '1993-01-01' + interval '1' year\ngroup by\nl_shipmode\norder by\nl_shipmode ;\n-- $ID$\n-- TPC-H/TPC-R Customer Distribution ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nc_count,\ncount(*) as custdist\nfrom\n(\nselect\nc_custkey,\ncount(o_orderkey) c_count\nfrom\ncustomer left outer join orders on\nc_custkey = o_custkey\nand o_comment not like '%express%requests%'\ngroup by\nc_custkey\n) as alias123 \ngroup by\nc_count\norder by\ncustdist desc,\nc_count desc ;\n-- $ID$\n-- TPC-H/TPC-R Promotion Effect ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\n100.00 * sum(case\nwhen p_type like 'PROMO%'\nthen l_extendedprice * (1 - l_discount)\nelse 0\nend) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\nfrom\nlineitem,\npart\nwhere\nl_partkey = p_partkey\nand l_shipdate >= date '1994-01-01'\nand l_shipdate < date '1994-01-01' + interval '1' month ;\n-- $ID$\n-- TPC-H/TPC-R Top Supplier ", "-- Functional Query Definition\n-- Approved February 1998\n\ncreate view revenue0 (supplier_no, total_revenue) as\nselect\nl_suppkey,\nsum(l_extendedprice * (1 - l_discount))\nfrom\nlineitem\nwhere\nl_shipdate >= date '1994-05-01'\nand l_shipdate < date '1994-05-01' + interval '3' month\ngroup by\nl_suppkey;\n\n\nselect\ns_suppkey,\ns_name,\ns_address,\ns_phone,\ntotal_revenue\nfrom\nsupplier,\nrevenue0\nwhere\ns_suppkey = supplier_no\nand total_revenue = (\nselect\nmax(total_revenue)\nfrom\nrevenue0\n)\norder by\ns_suppkey;\n\ndrop view revenue0 ;\n-- $ID$\n-- TPC-H/TPC-R Parts/Supplier Relationship ", "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\np_brand,\np_type,\np_size,\ncount(distinct ps_suppkey) as supplier_cnt\nfrom\npartsupp,\npart\nwhere\np_partkey = ps_partkey\nand p_brand <> 'Brand#41'\nand p_type not like 'STANDARD BURNISHED%'\nand p_size in (22, 44, 43, 6, 13, 19, 20, 49)\nand ps_suppkey not in (\nselect\ns_suppkey\nfrom\nsupplier\nwhere\ns_comment like '%Customer%Complaints%'\n)\ngroup by\np_brand,\np_type,\np_size\norder by\nsupplier_cnt desc,\np_brand,\np_type,\np_size ;\n-- $ID$\n-- TPC-H/TPC-R Small-Quantity-Order Revenue ", '-- Function Query Definition\n-- Approved February 1998\n\n\nselect\nc_name,\nc_custkey,\no_orderkey,\no_orderdate,\no_totalprice,\nsum(l_quantity)\nfrom\ncustomer,\norders,\nlineitem\nwhere\no_orderkey in (\nselect\nl_orderkey\nfrom\nlineitem\ngroup by\nl_orderkey having\nsum(l_quantity) > 313\n)\nand c_custkey = o_custkey\nand o_orderkey = l_orderkey\ngroup by\nc_name,\nc_custkey,\no_orderkey,\no_orderdate,\no_totalprice\norder by\no_totalprice desc,\no_orderdate limit 100;\n-- $ID$\n-- TPC-H/TPC-R Discounted Revenue ', "-- Functional Query Definition\n-- Approved February 1998\n\n\nselect\nsum(l_extendedprice* (1 - l_discount)) as revenue\nfrom\nlineitem,\npart\nwhere\n(\np_partkey = l_partkey\nand p_brand = 'Brand#34'\nand p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\nand l_quantity >= 1 and l_quantity <= 1 + 10\nand p_size between 1 and 5\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n)\nor\n(\np_partkey = l_partkey\nand p_brand = 'Brand#55'\nand p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\nand l_quantity >= 13 and l_quantity <= 13 + 10\nand p_size between 1 and 10\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n)\nor\n(\np_partkey = l_partkey\nand p_brand = 'Brand#24'\nand p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\nand l_quantity >= 22 and l_quantity <= 22 + 10\nand p_size between 1 and 15\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n) ;\n-- $ID$\n-- TPC-H/TPC-R Potential Part Promotion "]
# nodes = get_cand_nodes(db_id, workload)
# print(nodes[:5])


