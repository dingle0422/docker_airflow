#!/usr/bin/env python
# coding: utf-8
import sys
import os
sys.path.append(r"/usr/local/airflow/app_implant/models/modules")
import prediction_data_preparation as pdp
import connector
import multiprocessing



# # ##########################################################################################################3

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn') # 将进程在linux环境下改成spawn模式
    connector.getdata() # 由于直接从数据库取数占用太多内存，且不释放，暂定先将数据保存，后进入算法处理阶段
    output = pdp.multi_out() # 算法正式开始运作
    connector.commitdata(output) # commit the output to database
    os.remove(r"/usr/local/airflow/app_implant/data/implant_data.gz")

