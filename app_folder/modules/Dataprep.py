#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import numpy as np
import re
import pandas as pd


# In[ ]:



def diag_encode(data, reference):
    '''
    对测试集中的诊断名称进行编码作为单独特征加入数据集中
    data：测试集
    reference：用于求并集的高相关性诊断名称
    '''
    data['诊断名称']=data['诊断名称'].astype(str)
    vectorizer = TfidfVectorizer(min_df=1)
    X = vectorizer.fit_transform(data['诊断名称'])
    
    rm_col = list(set(vectorizer.get_feature_names()) - set(reference))  # 删除与相关诊断不重叠的诊断
    
    df1 = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names())
    f_col = list(reference)
    data = pd.concat([data, df1], axis=1).drop(axis=1, columns=['诊断名称'])
    data = data.drop(axis = 1, columns = rm_col) # 只留下相关诊断中的某些/全部诊断
    
    return data


# In[217]:


def str_filter(data, str_dict, allorany):
    '''
    返回去除“指定字符串”的数据行
    data：待处理dataframe
    str_dict：{列名：[字符串]} 即待去除的在指定列中包含的字符串
    allorany: 需要满足条件all(): containing every string需求, 还是any(): containing any of those strings需求
    '''
    df = data.copy()
    colnum = len(str_dict) # 需要识别的列的数量
    colname = list(str_dict.keys())
    string = list(str_dict.values())
    idx = set(df.index)
    df[colname] = df[colname].astype(str)
    
    if allorany == "any":
        for i in range(colnum):
            print("正在对列名'",colname[i],"'进行处理,", "筛除包含字段", string[i],"的数据行")
            series = df[colname[i]].apply(lambda x: True if any([True if i in x else False for i in string[i]]) else False)
            #series = df[colname[i]].apply(lambda x: True if any([True if s in x else False for s in string[i]]) else False)
            idx = idx & set(series[series == True].index)    
            df.drop(idx, inplace = True)
            
    elif allorany == "all":
        for i in range(colnum):
            print("正在对列名'",colname[i],"'进行处理,", "筛除包含字段", string[i],"的数据行")
            series = df[colname[i]].apply(lambda x: True if all([True if i in x else False for i in string[i]]) else False)
            idx = idx & set(series[series == True].index)    
        # 当all时，我们不能在每一个循环中都将已挑出的数据先筛除，应该将所有idx的重叠项进行筛选
        df.drop(idx, inplace = True)

            
    else:
        print('error')
        
    return df


# In[ ]:


def combine_data(path, suffix = 'csv'):
    '''
    合并指定路径文件夹下的某些文件（注：数据若存在中文，请事先以UTF-8格式编码保存）
    path：路径
    suffix：文件名后缀，代表sql-like数据的保存形式
    '''
    filelist=[]
    df = pd.DataFrame()

    for root, dirs, files in os.walk(top = path, topdown=False):
        for name in files:
            str=os.path.join(root, name)
            if str.split('.')[-1]=='csv':
                filelist.append(str)
        num_of_file = len(filelist)

    print(filelist)
    idx = input("please input the indexes of the files you want to combine (split by comma, ie: 1,2,3)")
    idx = [int(i) for i in idx.split(",")]
    
    for i in idx:
        df = df.append(pd.read_csv(r"{}".format(filelist[i]))).reset_index(drop = True)
        
    return df


def all_data(path, suffix='csv'):
    '''
    合并指定路径文件夹下的某些文件（注：数据若存在中文，请事先以UTF-8格式编码保存）
    path：路径
    suffix：文件名后缀，代表sql-like数据的保存形式
    '''
    filelist = []
    df = pd.DataFrame()

    for root, dirs, files in os.walk(top=path, topdown=False):
        for name in files:
            str = os.path.join(root, name)
            if str.split('.')[-1] == 'csv':
                filelist.append(str)
        num_of_file = len(filelist)

    print(filelist)
    for i in range(num_of_file):
        df = df.append(pd.read_csv(r"{}".format(filelist[i]))).reset_index(drop=True)

    return df


def fetch_data(data_dir):
    """
    YH's version of loading csv file containg Chinsese characters
    laod all csv formatted files in one directory into a dataframe
    """
    ## input testing
    if not os.path.isdir(data_dir):
        raise Exception("specified data dir does not exist")
    if not len(os.listdir(data_dir)) > 0:
        raise Exception("specified data dir does not contain any files")

    # Read column names from file
    file_list = [os.path.join(data_dir,f) for f in os.listdir(data_dir) if re.search("\.csv",f)]
    cols = list(pd.read_csv(file_list[0], encoding = "gb18030", nrows =1))
    
    # load data except the following columns 
    read_cols =[i for i in cols if i not in {'证件号', '院区', '诊断代码'}]

    ## read data into a temp structure
    all_months = {}
    for file_name in file_list:
        #df = pd.read_csv(file_name)
        df = pd.read_csv(file_name, encoding = "gb18030", usecols = read_cols)
        all_months[os.path.split(file_name)[-1]] = df

    ## concat all of the data
    df = pd.concat(list(all_months.values()),sort=True)
   
    dates=df['消费时间'].values
    df['消费时间'] = np.array(dates, dtype='datetime64[D]')
    #df['消费时间'].values.astype('datetime64[D]')
    
    ## sort by date and reset the index
    df.sort_values(by='消费时间',inplace=True)
    df.reset_index(drop=True,inplace=True)
    
    return(df)
# In[47]:


def bag_of_pats(bag_num, pats_id, pats_data):
    """
    将所有已种植客户的数据按照患者id进行分包处理，患者的所有病程数据会被完整保留，不会被截断
    bag_num: 将总数据集分为几个包
    pats_id: 该数据集的所有患者id
    pats_data: 总数据集
    """
    sub_num = bag_num 
    size_list = [round(i) for i in np.linspace(0,len(pats_id)-1, sub_num + 1)] 
    count = 1
    name = globals()

    for i in range(1,len(size_list)): #1,2,3,4

        if count == 1:
            name["sub_pat{}".format(count)] = pats_id[size_list[i-1]:size_list[i]+1]
            name["sub_imp{}".format(count)] = pats_data[pats_data['关联号'].isin(pats_id[size_list[i-1]:size_list[i]+1])]
        else:
            name["sub_pat{}".format(count)] = pats_id[size_list[i-1]+1:size_list[i]+1]
            name["sub_imp{}".format(count)] = pats_data[pats_data['关联号'].isin(pats_id[size_list[i-1]+1:size_list[i]+1])]

        count += 1
    
    #分包后打包进元组方便遍历
    imp_list = []
    pat_list = []
    for o in range(1, len(size_list)):
        pat_list.append(globals()["sub_pat{}".format(o)])
        imp_list.append(globals()["sub_imp{}".format(o)])


    print("分包完毕，得到的两类数据为sub_pat()和sub_imp()分别表示分包后的患者id集和对应患者病程数据，括号以子集编号代替，如sub_pat1")
    return pat_list, imp_list


# In[49]:


#找出种植前的所有消费记录

def valid_implant(sub_pat, sub_imp, earliest, interval):
    """
    找出所有符合时间窗要求的有效种植记录
    
    sub_pat: 病人id子集
    sub_imp: 相应消费数据子集
    earliest: 时间截取下限，即时间窗口最早日期(必须以时间戳形式带入，如datetime.date(2019, 1, 1))
    interval：时间间隔、周期
    """
    #print("子进程",p,'进入')
    sub_imp['implant'] = 0

    for i in sub_pat: #把每个种植过的患者筛出
        pat = sub_imp[sub_imp['关联号'].values == i]
        implant_dates = pat[pat['收费分类'].values == '种植费']['date'].values

        first_implant = implant_dates.min() #找出该患者第一次种植牙的时间
        yearago = first_implant - datetime.timedelta(days = interval) #365
        
        #只取第一次种植之前一年的记录 ##########################20201104 
        if yearago > earliest: #根据数据时间范围自行修改 datetime.date(2019, 1, 1)
            sub_imp.loc[pat[(pat['date'].values < first_implant)&(pat['date'].values > yearago)].index, "implant"] = "selected"
            
        else:
            continue

    return sub_imp[sub_imp['implant'] == "selected"]


#1m 20s part1 51.6s part2 30s


# In[ ]:


def valid_noimplant(sub_PAT, sub_IMP, earliest, interval):
    """
    找出所有符合时间窗要求的有效未种植记录
    sub_PAT: 病人id子集
    sub_IMP: 相应消费数据子集
    earliest: 时间截取下限，即时间窗口最早日期(必须以时间戳形式带入，如datetime.date(2019, 1, 1))
    interval：时间间隔、周期
    """
    count = 0
    #print("子进程",p,'进入')
    sub_IMP['noimplant'] = 0
    
    for i in sub_PAT: #把每个种植过的患者筛出
        pat1 = sub_IMP[(sub_IMP['关联号'].isin([i]))] #某种植牙病患的所有问诊记录
        #找出该患者最后一次问诊的时间
        last_visit = pat1['date'].max()
        #只取最后一次问诊之前一年的记录 ##########################20201104
        yearago1 = last_visit - datetime.timedelta(days = interval)
        if yearago1 > earliest:
            before = pat1[(pat1['date'].values < last_visit)&(pat1['date'].values > yearago1)]
            part_idx1 = before.index
            sub_IMP.loc[part_idx1,"noimplant"] = "selected"

        else:
            continue
            
        count += 1


    #print("子进程",p,"完成")
    return sub_IMP[sub_IMP['noimplant'] == "selected"]


# In[10]:


def multimodel(data,threshold,model_names):
    '''
    返回多个模型的综合结果。由于我们以后用到得模型会不同，此函数通用性不高，一般不用调用，只做备份
    '''
    SVC_feature = ['关联号','implant','牙齿缺少', '根尖囊肿', '牙列缺损', '根折', '项目金额(实付)', '牙折裂', '牙体缺失', '残留牙根', '牙根纵裂', '牙列部分缺失', '项目金额(实付)_mean', '后天性牙齿缺失', '牙折断', '残冠', '残根', '根尖周脓肿', '残根伴慢性根尖牙周炎', '冠折', '牙髓联合病变', '年龄', '创伤性牙折断', '慢性髓炎']
    LR_feature = ['关联号','implant','牙龈瘘管', '牙槽骨骨折', '口腔上颌窦瘘', '根尖肉芽肿', '逆行性牙髓炎', '下颌骨囊肿', '牙冠周脓肿', '急性牙髓源性根尖牙周炎', '残根', '牙列缺损', '牙折裂', '项目金额(实付)', '牙齿缺少', '项目金额(实付)_mean', '根尖囊肿', '牙根纵裂', '牙列部分缺失', '牙体缺失', '根分叉病变', '牙折断', '创伤性牙折断', '牙髓联合病变', '残根伴慢性根尖牙周炎', '年龄', '残留牙根', '根尖周脓肿', '急性牙周炎', '牙周牙髓联合病变', '牙位置异常', '牙髓坏疽', '残冠', '后天性牙齿缺失', '根折', '单颌牙列缺失', '冠折']
    XGB_feature = ['关联号','implant','项目金额(实付)', '项目金额(实付)_mean', '性别', '年龄', 'duration', 'freq', 'freq/duration', '慢性牙周炎', '龋齿', '残留牙根', '慢性根尖周炎', '阻生齿', '牙体缺失', '牙体缺损', '牙髓炎', '牙齿缺少', '深龋', '牙列部分缺失', '牙周病', '残冠', '牙根纵裂', '牙折断', '牙周牙髓综合征', '根管治疗后疾病', '残根伴慢性根尖牙周炎', '牙隐裂', '后天性牙齿缺失', '慢性髓炎', '根折', '牙列缺损', '根分叉病变', '急性根尖周炎', '根尖囊肿', '牙周脓肿', '埋伏牙', '牙髓坏死', '创伤性牙折断', '急性牙髓炎', '牙位置异常', '阻生牙', '冠折', '单颌牙列缺失', '乳牙滞留', '种植体周围炎', '根尖脓肿', '牙折裂', '根尖周脓肿', '拔除或局部牙周病引起的牙缺失', '意外事故', '牙龈瘘管', '牙冠周脓肿', '牙齿大小和形状异常', '残根', '逆行性牙髓炎', '牙髓坏疽', '急性牙周炎', '根尖周脓肿伴窦道', '牙髓联合病变', '急性牙髓源性根尖牙周炎', '牙周牙髓联合病变', '牙槽骨骨折', '口腔上颌窦瘘', '根尖肉芽肿', '种植体周围粘膜炎', '下颌骨囊肿']
    name = locals()
    outl = []
    
    # we calculate and append the prediction results of all models, for th further overlap-searching
    for i in model_names:
        choice = i
        model =  globals()['{}'.format(choice)]
        f = locals()['{}_feature'.format(choice)] #  get the correspongding feature set
        frame = pd.DataFrame(columns = f) # initialize the data frame
        df3 = frame.append(data)[f].fillna(0) # append the data we've prepared
    
        # Indepandent Variables
        for i in ['关联号','implant']:
            f.remove(i)
        X = df3[f].values
        
        # dependant Variable
        Y = df3['implant'] 
        
        # standardize the feature values if necessary
        if choice in ['LR','SVC']: 
            min_max_scaler = preprocessing.MinMaxScaler() 
            X_minMax = min_max_scaler.fit_transform(X)
            X = X_minMax

        name['{}_out'.format(choice)] = [1 if i > 0.5 else 0 for i in model.predict_proba(X)[:,1]]
        print("the confusion matrix of {} model shows below: \n".format(choice))
        print(confusion_matrix_lucid(locals()["{}_out".format(choice)],Y),"\n\n ----------------------------------------------------")
        
        outl.append(locals()["{}_out".format(choice)])
    
    # overlap-searching step
    trans_out = np.array(outl).T # put it into array frame to use transform fuction "T"
    
    return [i[0] if len(set(i)) == 1 else 0 for i in trans_out]


# In[9]:


if __name__ == "__main__":
    print("Successfully import the module!")

