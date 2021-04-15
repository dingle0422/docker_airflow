import psycopg2
import gc
import configparser
import pandas as pd
import datetime
import zipfile
import os


def getdata():
    """get data from database and convert it into dataframe"""
    config = configparser.ConfigParser()
    config.read("/usr/local/airflow/app_folder/config.ini")
    db = config.get("source_db","database")
    ur = config.get("source_db","user")
    pw = config.get("source_db","password")
    hs = config.get("source_db","host")
    pt = config.get("source_db","port")
    print(db,ur,pw,hs,pt)

    # conn = psycopg2.connect(database = db, user = ur, password = pw, host = hs, port = pt) # please change the parameters in this connector function thx~
    # print("congrats! connect successfully!")
    # cs = conn.cursor()


    # colname = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'implant_mining'" # get columns' names
    # alldata = "SELECT * FROM implant_mining" # get values for columns

    start = datetime.datetime.now()

    # rows = []
    conn =  psycopg2.connect(database = db, user = ur, password = pw, host = hs, port = pt)

    print("Connected to DB")
    cursor = conn.cursor(name='implant_cursor')

    print("Start to retrieve data from DB")
    cursor.itersize = 10000
    query = "SELECT * FROM implant_mining;"
    cursor.execute(query)

    print('start fetch data')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    print("start writing down the original data")
    data_out = pd.DataFrame(rows,
                      columns=['关联号', '证件号', '性别', '年龄', '消费项目', '项目金额(实付)', '消费时间',
                               '收费分类', '院区', '科室', '初复诊', '诊断代码',
                               '诊断名称', '治疗计划'])

    data_out.to_csv(r"/usr/local/airflow/app_folder/data/implant_data.gz", compression="gzip", encoding= 'utf-8', index=False)


    end = datetime.datetime.now()
    run_time = end-start

    print("Running time: ",run_time)
    del rows, data_out
    gc.collect()

    return



def commitdata(data):
    """commit the result data to the temporary database"""
    config = configparser.ConfigParser()
    config.read("/usr/local/airflow/app_folder/config.ini")
    db = config.get("target_db", "database")
    ur = config.get("target_db", "user")
    pw = config.get("target_db", "password")
    hs = config.get("target_db", "host")
    pt = config.get("target_db", "port")

    conn = psycopg2.connect(database=db, user=ur, password=pw, host=hs, port=pt)  # please change the parameters in this connector function thx~
    print("congrats! connect successfully!")
    cs = conn.cursor()

    # try:
    #     # create a new table
    #     newtab = "CREATE TABLE IF NOT EXISTS {} (关联号 text, 种植 bigint);".format(name)
    #     cs.execute(newtab)
    #     print("Successfully created a new table {} containing output".format(name))
    # except Exception as e1:
    #     print(e1)

    try:
        # insert values
        for i in range(len(data['关联号'])):
            cs.execute(" INSERT INTO " + "implant_prediction"
                        + "(key_number, prediction_time, implant_prob) "
                        + " values(%s, to_timestamp(%s,'yyyy-MM-dd hh24:mi:ss'), %s);",
                        (data['关联号'][i], data['预测时间'], data['种植概率'][i])
                        )
        print("Successfully interted values into table implant_prediction")
    except Exception as e2:
        print(e2)

    conn.commit()
    cs.close()
    conn.close()

    return
