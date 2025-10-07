# @TIME : 15/6/23 4:05 PM
# @AUTHOR : LZDH

import psycopg2
import json


def get_postgres(db_id, test_query):
    # with open('data/schemas/' + db_id + '.json', 'r') as f_sche:
    #     data = f_sche.read()
    #     table_details = json.loads(data)
    # table_names = [i['table'] for i in table_details]
    # print(table_names)

    # Connect to the PostgreSQL database
    pg_conn = psycopg2.connect(host="localhost",
        port="5432",
        database=db_id,
        user="postgres",
        password="19981212lzdhMJK"

    )
    # disable_query_cache(pg_conn)
    pg_cursor = pg_conn.cursor()
    clr_q = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '" + db_id + "';"
    pg_cursor.execute(clr_q)
    # pg_cursor.execute("SET enable_seqscan = off;")
    # pg_cursor.execute("SET enable_indexscan = off;")
    try:
        error = ''
        # Commit the changes and close the connections
        # start_time = time.time()
        # print('in')
        pg_cursor.execute(test_query)
        pg_conn.commit()
        results = pg_cursor.fetchall()
        # end_time = time.time()
        # execution_time = end_time - start_time
        # print('stop')
        # print("Query Results:")
        # for row in results:
        #     print(row)
        # print("Execution Time:", execution_time)
        pg_cursor.close()
        pg_conn.close()
        return results, error
    except Exception as error1:
        print('error')
        print(error1)
        pg_cursor.close()
        pg_conn.close()
        results = 'NA'
        return results, error1


def get_pg_tables(db_id):
    pg_conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database=db_id,
        user="postgres",
        password="19981212lzdhMJK"
    )
    # disable_query_cache(pg_conn)
    pg_cursor = pg_conn.cursor()
    clr_q = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '" + db_id + "';"
    pg_cursor.execute(clr_q)
    # pg_cursor.execute("SET enable_seqscan = off;")
    # pg_cursor.execute("SET enable_indexscan = off;")
    try:
        error = ''
        # Commit the changes and close the connections
        # start_time = time.time()
        # print('in')
        pg_cursor.execute('SELECT relname AS table_name FROM pg_stat_user_tables;')
        pg_conn.commit()
        results = pg_cursor.fetchall()
        # end_time = time.time()
        # execution_time = end_time - start_time
        # print('stop')
        # print("Query Results:")
        # for row in results:
        #     print(row)
        # print("Execution Time:", execution_time)
        pg_cursor.close()
        pg_conn.close()
        return results, error
    except Exception as error1:
        print('error')
        print(error1)
        pg_cursor.close()
        pg_conn.close()
        results = 'NA'
        return results, error1


def get_pg_tablerows(db_id, table):
    sql_input = 'SELECT count(*) FROM ' + table + ';'
    num_rows, err = get_postgres(db_id, sql_input)
    return num_rows, err


def get_pg_colrows(db_id, table, colname):
    sql_input = 'SELECT count(' + colname + ') FROM ' + table + ';'
    num_rows, err = get_postgres(db_id, sql_input)
    return num_rows, err


def get_pg_schema(db_id):
    tables, err_0 = get_pg_tables(db_id)
    tables = [x[0] for x in tables]
    schema = []
    for t in tables:
        table = {}
        num_rows, err = get_pg_tablerows(db_id, t)
        num_rows = num_rows[0][0]
        sql_input = "select column_name from information_schema.columns where table_schema='public' and table_name='" + t + "';"
        cols, err = get_postgres(db_id, sql_input)
        cols = [x[0] for x in cols]
        sql_input_1 = "select data_type from information_schema.columns where table_schema='public' and table_name='" + t + "';"
        types, err_1 = get_postgres(db_id, sql_input_1)
        types = [x[0] for x in types]
        table['table'] = t
        table['rows'] = num_rows
        columns = []
        for i in range(len(cols)):
            columns.append({'name': cols[i], 'type': types[i]})
        table['columns'] = columns
        # print(table)
        schema.append(table)
    with open(db_id + '.json', 'w+') as f:
        f.write(json.dumps(schema))


# get_pg_schema('job')