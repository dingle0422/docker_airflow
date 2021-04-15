#!/usr/bin/env python
# coding: utf-8
# The corresponding notebook prediction-data-preparation
import sys
sys.path.append(r"/usr/local/airflow/app_implant/modules")

import joblib
from sklearn import preprocessing
import valid_implant as vi
import valid_noimplant as vni
import valid_og as vo
from Dataprep import *
import pandas as pd
import connector
import datetime
import configparser
import gc
from zipfile import ZipFile

def getconfig(section,option):
    config = configparser.ConfigParser()
    config.read(r"/usr/local/airflow/app_implant/config.ini")

    if section == "source_db" or section == "target_db":
        return config.get(section,option)
    else:
        try:
            return config.getint(section,option)
        except:
            return config.get(section,option)


def exportdata():
    global df0
    # extract data from sql databse
    #df0 = connector.getdata()

     # 本地文件测试用代码
    df0 = pd.read_csv(r'/usr/local/airflow/app_implant/data/implant_data.gz', encoding = 'utf-8')


    # df0.rename(columns = {'patient_key': '关联号', 'patient_id': '证件号', 'gender': '性别', 'age': '年龄', 'item_name': '消费项目',
    #                       'item_charge': '项目金额(实付)', 'charge_time': '消费时间', 'type_name': '收费分类', 'hospital_name':'院区',
    #                       'office_name': '科室', 'examination_flag': '初复诊', 'icd_code': '诊断代码', 'diagnosis_name': '诊断名称',
    #                       'Treatment': '治疗计划'}, inplace = True)

    print("Congrats! The original data was extracted successfully from database! shape: {}".format(df0.shape))
    print("Dataset's columns are {}".format(df0.columns))
    print(df0)

    return df0

#df.to_csv(r"pinghai_1920_all.csv") # 用于对模型预测结果的进一步筛选,请务必运行此代码并保存到工作目录下，将在预测结果筛选中用到


def clean_prep(): ### 后期优化
    global data0
    data0 = df0 #.drop(columns=['证件号'])  # 证件号已废，对身份识别无用，删去
    del globals()['df0']
    gc.collect()
    data0 = str_filter(data0,{"科室":['儿','畸']}, allorany = 'any')
    data0 = data0[(data0['项目金额(实付)'].values >= 0)&(data0['年龄'].values != 0)].reset_index(drop = True) #删去单个消费金额小于0的代金券抵扣和年龄为0的数据\ndata0.shape")
    data0 = data0.fillna("Unknown")
    data0['date'] = pd.to_datetime(data0['消费时间']).dt.date
    print('The preliminary data cleaning and preparation was completed')



    return data0


# ## 筛出有计划进行种植的消费记录
def find_implant():
    data1 = clean_prep() #这步df0和data0都被声明全局并赋值
    l = list(data1[data1['收费分类'].isin(["种植费"])].index)
    implant_buy = str_filter(data1.loc[l,:], {"收费分类":['种植费'],"消费项目":['手术一次性材料费（手术时收）']}, allorany = 'all')


    # ## 我们需要找出这些消费记录都是哪些顾客消费的。知道了消费对应的顾客，我们就能去追溯他们的过往问诊、消费记录了

    patients = list(set(implant_buy['关联号'].unique()))
    pat_implant1 = data1[data1['关联号'].isin(patients)]
    # pat_for_dedup = pat_implant1 #!!!!!!!!!!!!!!!!!
    print("The group of implant patients was splited out")

    return patients, pat_implant1





# # ################################################################################################

def prepfeature(data,implant):
    # 得到所有种植患者的种植前数据，我们将它们聚合，使得每一行代表一个患者的病程
    # data['关联号'].value_counts()
    # 诊断
    diags = data.groupby(['关联号', 'date'])['诊断名称'].unique().reset_index()  # 不同次的问诊不应作unique处理，否则将消除病种出现的频数对结果的影响
    diags['诊断名称'] = diags['诊断名称'].astype(str)
    diags = diags.groupby(['关联号'])['诊断名称'].sum().reset_index()
    # 治疗计划
    treats = data.groupby(['关联号'])['治疗计划'].unique().reset_index()
    # 消费项目
    purchases = data.groupby(['关联号'])['消费项目'].unique().reset_index()
    # 消费总额
    total_charge = data.groupby(['关联号'])['项目金额(实付)'].sum().reset_index()  # 种植前所有花在口腔的钱
    # 每次平均消费
    mean_charge = (data.groupby(['关联号', 'date'])['项目金额(实付)'].sum().reset_index()).groupby(['关联号'])[
        '项目金额(实付)'].mean().reset_index()  # 种植前每次问诊平均花费的钱
    # 科室
    office = data.groupby(['关联号'])['科室'].unique().reset_index()
    # 性别
    gender = data.groupby(['关联号'])['性别'].unique().reset_index()
    # 年龄
    age = data.groupby(['关联号'])['年龄'].max().reset_index()
    # 种植前来看病总周期长度
    duration = data.groupby(['关联号'])['date'].unique().reset_index()
    duration['duration'] = duration['date'].map(lambda x: x.max()) - duration['date'].map(lambda x: x.min())
    duration['duration'] = duration['duration'].map(lambda x: x.days)  # 去单位
    # 诊断代码
    diag_code = data.groupby(['关联号'])['诊断代码'].unique().reset_index()
    # 消费分类
    purchase_type = data.groupby(['关联号'])['收费分类'].unique().reset_index()

    # 将所有处理好的feature重新组合
    linked = diags
    for f in [diag_code, treats, purchases, purchase_type, total_charge, mean_charge, office, gender, age, duration]:
        linked = linked.join(f.set_index(['关联号']), on=['关联号'], how='left', rsuffix="_mean")

    linked['implant'] = implant

    print('The features for the implant/noimplant subset were grouped')

    return linked


def multipros(early = getconfig("valid_implant_noimplant","earliest"), interval = getconfig("valid_implant_noimplant","interval"), sub_num = getconfig("multiproc","sub_num")):
    """
    sub_num: 分包个数
    early: 最早时间点
    interval：时间间隔
    """
    print("The multiprocessing task to separate implant/noimplant is running now")
    # transform the str to date.format
    YMD = [int(i) for i in early.split(".")]
    earliest = datetime.date(YMD[0],YMD[1],YMD[2])

    find_p = find_implant()
    # 分包
    bags0 = bag_of_pats(sub_num,find_p[0],find_p[1])
    pat_list = bags0[0]
    imp_list = bags0[1]
    print("first bag for implant \n\n {}".format(imp_list[0]))

    # 多线程处理
    a = vi.sub_implant(pat_list,imp_list,earliest,interval)
    out = a.find_valid()
    pre_implant = pd.DataFrame()
    for i in range(sub_num):
        pre_implant = pre_implant.append(out[i])

    # 组合、处理特征
    link_implant = prepfeature(pre_implant,implant=1)

    #################################################################################################

    # ### 接下去我们找出无种植记录的数据
    #得到无种植记录的病患的所有历史记录,并给数据打上时间戳及数据方面的清理
    noimplant = data0.append(find_p[1]).drop_duplicates(keep = False).reset_index(drop = True)

    del globals()['data0']
    gc.collect()

    patients_noimplant = noimplant['关联号'].value_counts().index

    # 分包
    bags1 = bag_of_pats(sub_num,patients_noimplant,noimplant)
    pat_list = bags1[0]
    imp_list = bags1[1]
    print("first bag for noimplant \n\n {}".format(imp_list[0]))


    # 多线程
    b = vni.sub_noimplant(pat_list,imp_list,earliest,interval)
    out = b.find_valid()
    pre_noimplant = pd.DataFrame()
    for i in range(sub_num):
        pre_noimplant = pre_noimplant.append(out[i])

    link_noimplant = prepfeature(pre_noimplant, implant = 0)

    print("Congrats! The multiprocessing for spliting implant/noimplant succeed")



    return link_implant, link_noimplant



def comb_data():
    # ### 接下去我们将有/无种植记录的数据合并
    data1 = multipros()
    combined = data1[0].append(data1[1]).reset_index(drop = True)

    ##加frequency
    combined['freq'] = None
    combined['freq'] = combined['date'].apply(lambda x: len(x))

    ##加fre/duration
    combined['freq/duration'] = combined['freq']/(combined['duration']+1)

    #将性别量化
    combined['性别'] = combined['性别'].astype(str).map({"['男']":int(1),"['女']":int(0)})
    #删除性别为[男,女]的用户
    combined = combined.drop(combined[combined.isnull().T.any() == True].index).reset_index(drop=True)
    print("The dataset for prediction is merged and ready to fit your model!")

    return combined


### model prediction

# 导入数据
def get_pred_data():
    ori_data = comb_data()
    model_data = ori_data[
        ['关联号', '治疗计划', '诊断名称', '项目金额(实付)', '项目金额(实付)_mean', '科室', '性别', '年龄', 'duration', 'freq', 'freq/duration',
         'implant']]

    return model_data


def rele_diag():
    relevant_diag = set(pd.read_csv(r'/usr/local/airflow/app_implant/data/implant_diags.csv', index_col=0)['诊断名称'].unique())
    return relevant_diag


# # 编码相关诊断、科室
def tfidf_encode():
    # The following codes using TFIDF for diags 诊断名称. The new diags sparse matrix are concatenated to the original dataset
    data_for_pred = diag_encode(get_pred_data(), rele_diag())
    del_redun_val()

    return data_for_pred


def del_redun_val():
    '''多线程算法后，全局变量中会有很多子集对内存造成负担，我们他们删除，只保留组合结果'''

    del_global = [i for i in globals().keys() if ("sub_" in i) and (isinstance(int(i[-1]), int))]
    for i in del_global:
        del globals()[i]

    gc.collect()

    print("The redundant global variables were eliminated")
    return


# # #################################################################################################

# # 上方特征编码完成后，我们正式进入模型应用阶段：
# ## 确定完三个模型的泛化性之后，我们编写函数，来直接导出三个模型的重合项作为最终预测结果


def multimodel(data, model_names):
    '''
    返回多个模型的综合结果。由于我们以后用到得模型会不同，此函数通用性不高，一般不用调用，只做备份
    '''
    name = locals()
    SVC = joblib.load(r"/usr/local/airflow/app_implant/models/SVC_SMOTE.m")
    XGB = joblib.load(r"/usr/local/airflow/app_implant/models/XGB_OUT_oversample_20201210_131900.m")
    LR = joblib.load(r"/usr/local/airflow/app_implant/models/LR_20201207_30.m")

    SVC_feature = ['关联号', 'implant', '牙齿缺少', '根尖囊肿', '牙列缺损', '根折', '项目金额(实付)', '牙折裂', '牙体缺失', '残留牙根', '牙根纵裂', '牙列部分缺失',
                   '项目金额(实付)_mean', '后天性牙齿缺失', '牙折断', '残冠', '残根', '根尖周脓肿', '残根伴慢性根尖牙周炎', '冠折', '牙髓联合病变', '年龄', '创伤性牙折断',
                   '慢性髓炎']
    LR_feature = ['关联号', 'implant', '牙龈瘘管', '牙槽骨骨折', '口腔上颌窦瘘', '根尖肉芽肿', '逆行性牙髓炎', '下颌骨囊肿', '牙冠周脓肿', '急性牙髓源性根尖牙周炎', '残根',
                  '牙列缺损', '牙折裂', '项目金额(实付)', '牙齿缺少', '项目金额(实付)_mean', '根尖囊肿', '牙根纵裂', '牙列部分缺失', '牙体缺失', '根分叉病变', '牙折断',
                  '创伤性牙折断', '牙髓联合病变', '残根伴慢性根尖牙周炎', '年龄', '残留牙根', '根尖周脓肿', '急性牙周炎', '牙周牙髓联合病变', '牙位置异常', '牙髓坏疽', '残冠',
                  '后天性牙齿缺失', '根折', '单颌牙列缺失', '冠折']
    XGB_feature = ['关联号', 'implant', '项目金额(实付)', '项目金额(实付)_mean', '性别', '年龄', 'duration', 'freq', 'freq/duration',
                   '慢性牙周炎', '龋齿', '残留牙根', '慢性根尖周炎', '阻生齿', '牙体缺失', '牙体缺损', '牙髓炎', '牙齿缺少', '深龋', '牙列部分缺失', '牙周病', '残冠',
                   '牙根纵裂', '牙折断', '牙周牙髓综合征', '根管治疗后疾病', '残根伴慢性根尖牙周炎', '牙隐裂', '后天性牙齿缺失', '慢性髓炎', '根折', '牙列缺损', '根分叉病变',
                   '急性根尖周炎', '根尖囊肿', '牙周脓肿', '埋伏牙', '牙髓坏死', '创伤性牙折断', '急性牙髓炎', '牙位置异常', '阻生牙', '冠折', '单颌牙列缺失', '乳牙滞留',
                   '种植体周围炎', '根尖脓肿', '牙折裂', '根尖周脓肿', '拔除或局部牙周病引起的牙缺失', '意外事故', '牙龈瘘管', '牙冠周脓肿', '牙齿大小和形状异常', '残根',
                   '逆行性牙髓炎', '牙髓坏疽', '急性牙周炎', '根尖周脓肿伴窦道', '牙髓联合病变', '急性牙髓源性根尖牙周炎', '牙周牙髓联合病变', '牙槽骨骨折', '口腔上颌窦瘘',
                   '根尖肉芽肿', '种植体周围粘膜炎', '下颌骨囊肿']
    outl = []

    # we calculate and append the prediction results of all models, for th further overlap-searching
    for i in model_names:
        choice = i
        model = locals()['{}'.format(choice)]
        f = locals()['{}_feature'.format(choice)]  # get the correspongding feature set
        frame = pd.DataFrame(columns=f, dtype=object)  # initialize the data frame
        df3 = frame.append(data)[f].fillna(0)  # append the data we've prepared

        # Indepandent Variables
        for i in ['关联号', 'implant']:
            f.remove(i)
        X = df3[f].values

        # dependant Variable
        Y = df3['implant']

        # standardize the feature values if necessary
        if choice in ['LR', 'SVC']:  ####
            min_max_scaler = preprocessing.MinMaxScaler()
            X_minMax = min_max_scaler.fit_transform(X)
            X = X_minMax

        name['{}_out'.format(choice)] = model.predict_proba(X)[:, 1]

        outl.append(locals()["{}_out".format(choice)])

    # overlap-searching step
    trans_out = np.array(outl).T  # put it into array frame to use transform fuction "T"


    del locals()['LR'], locals()['SVC'], locals()['XGB']
    gc.collect()

    return [np.mean(i) for i in trans_out]  # [i[0] if len(set(i)) == 1 else 0 for i in trans_out]


def call_group(dt):
    ## 进行结果导出
    dt['predicted_implant'] = multimodel(dt, ("LR", "SVC", "XGB"))  ###!!!!!
    dt[['关联号']] = dt[['关联号']].astype(str)
    prob_refer = dt

    # 筛选出无种植记录的人,这些也就是我们需要做预测营销的客户群
    noimplant = dt[dt['implant'].values == 0]
    # 初始名单
    call = noimplant[noimplant['predicted_implant'].values > 0.5]  # 我们将再接下去的部分里对该部分纯预测结果进行特征分析
    set_call = set(call['关联号'].value_counts().index)  # 保证关联号无重复
    print("We have {} customers predicted as high-value-implant-customer group".format(len(set_call)))

    return set_call, prob_refer


# 接下去我们按照院区要求进行进一步筛选
# # 根据院区要求，对预测结果进行筛选:4个月到1年前拔过牙，但却还没有来种植的人(年龄大于等于20)

def special_demand():
    '''the filter procedure based on hospital's demands'''
    # get special demands from config.ini
    config = configparser.ConfigParser()
    config.read("/usr/local/airflow/app_implant/config.ini")
    lower_age = config.getint("special_demand", "lower_age")
    # start filtering
    pinghai_og = exportdata().reset_index(drop=True)  # pdp.exportdata().reset_index(drop = True)
    pinghai_og = pinghai_og[pinghai_og['年龄'].values >= lower_age].reset_index(drop=True) # 筛除20岁以下的青少年客户
    pinghai_og = str_filter(pinghai_og, {'科室': ['儿', '畸', '种植', '信息', '财务', '发展']}, allorany='any')
    pinghai_og = str_filter(pinghai_og, {'收费分类': ['种植费'], '消费项目': ['手术一次性材料费（手术时收）']}, allorany='all')
    pinghai_og['date'] = pd.to_datetime(pinghai_og['消费时间']).dt.date
    all_patients = list(pinghai_og['关联号'].unique())

    return pinghai_og, all_patients


def multi_out():
    # get params from config.ini
    config = configparser.ConfigParser()
    config.read("/usr/local/airflow/app_implant/config.ini")
    bag_n = config.getint("multiproc", "sub_num")

    # split in n bags
    ph_og = special_demand()
    bags = bag_of_pats(bag_num=bag_n, pats_id=ph_og[1], pats_data=ph_og[0])
    pat_list = bags[0]
    valid_list = bags[1]
    call_g = call_group(tfidf_encode())

    # multiprocessing based on hospital's special demand described in module vo
    a = vo.sub_implant(valid_list, pat_list, call_g[0])  # predict first, then filter
    out = a.find_valid()
    FINAL = []
    try:
        for i in range(bag_n):
            FINAL.append(out[i])
            print('线程均完成工作，开始合并结果')
    except Exception as err:
        print("Error in append process: ", err)

    print('The combination result of FINAL: ', FINAL)

    # extract final keys into new list to output
    F = []
    for i in FINAL:
        if len(i) > 0:
            for o in i:
                F.append(o)  #
        else:
            continue

    submit = dict()
    submit['关联号'] = F
    submit['种植概率'] = [round(call_g[1][call_g[1]['关联号'].values == i]['predicted_implant'].values[0], 2) for i in
                      F]  # round(call_g[1][call_g[1]['关联号'].values == i]['predicted_implant'].values[0],2)
    submit['预测时间'] = datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")
    # submit = [[i,TS] for i in F] #pd.DataFrame({'关联号': F,"种植":np.ones(len(F)).tolist()})

    return submit
