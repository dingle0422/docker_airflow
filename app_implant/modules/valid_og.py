
import multiprocessing
import datetime
import gc
import re
import configparser

#找出种植前的所有消费记录


class sub_implant(object):
    
    def __init__(self,valid_list,pat_list,set_call):
        
        self.valid_list = valid_list
        self.pat_list = pat_list
        self.call_og = set_call
    
    def valid_og(self, p, sub_valid, sub_pat):
        """
        """
        print("子进程", p, '进入')
        sub_valid['selection'] = 0
        config = configparser.ConfigParser()
        config.read(r"/usr/local/airflow/app_implant/config.ini")
        upper_back = config.getint('valid_og',"upper_time_backward")
        lower_back = config.getint('valid_og',"lower_time_backward")


        for i in sub_pat:
            pat = sub_valid[sub_valid['关联号'].isin([i])] #某种植牙病患的所有问诊记录
            pat = pat.sort_values(by = 'date')
            #找出最近一次问诊时间，并且往前推4个月
            thelastvisit = pat['date'].max()

            #if thelastvisit > datetime.date(2019, 1, 1): #院区要求客户至少在19年以后有来院记录，否则视为流失
            fourmonthback = thelastvisit - datetime.timedelta(days = lower_back)
            oneyearback = thelastvisit - datetime.timedelta(days = upper_back)

            #if fourmonthback > datetime.date(2018, 1, 1): #数据收集的起点（4个月前）起码在2018年之后
            valid = pat[(pat['date'].values <= fourmonthback) & (pat['date'].values >= oneyearback)] #大于4个月小于一年内的问诊数据
            idxs = valid.index
            sub_valid.loc[idxs,'selection'] = 1

            #else:
             #   pass
        fourmonthago = sub_valid.loc[sub_valid['selection'].values == 1, : ] ### 找出了时间合格的人
        
        #去除消费项目中括号内的项目，以防止其内容干扰识别拔除、拔牙字符
        for i in fourmonthago.index:
            buy_list = []
            buy = fourmonthago['消费项目'][i].split(" ")
            for b in buy:        
                if any([("(" in b),("（" in b)]):
                    candi1 = re.sub(r"\(.*?\)","",b) #删除字符串中的括号与括号内的字符
                    candi2 = re.sub(r"\（.*?\）","",b) 
                    candi3 = re.sub(r"\(.*?\）","",b) 
                    candi4 = re.sub(r"\（.*?\)","",b) 
                    buy_list.append(min([candi1,candi2,candi3,candi4], key = len))
                else:
                    buy_list.append(b)

            fourmonthago.loc[i,'消费项目'] = str(buy_list)

    
        fourmonthago = fourmonthago.astype(str)

        
        baya_idx = []
        for i in fourmonthago.index:
            if any([("拔牙" in fourmonthago['消费项目'][i]),("拔除" in fourmonthago['消费项目'][i])]): # 做了拔牙或拔除的前提下
                if all([("畸" not in fourmonthago['诊断名称'][i]),("畸" not in fourmonthago['治疗计划'][i]),("畸" not in fourmonthago['消费项目'][i]),("畸" not in fourmonthago['科室'][i])]): #非正畸
                    if all([("阻生" not in fourmonthago['诊断名称'][i]),("阻生" not in fourmonthago['治疗计划'][i]),("阻生" not in fourmonthago['消费项目'][i])]): # 非阻生牙
                        if all([("埋伏" not in fourmonthago['诊断名称'][i]),("埋伏" not in fourmonthago['治疗计划'][i]),("埋伏" not in fourmonthago['消费项目'][i])]): # 非埋伏牙
                            if all([("智齿" not in fourmonthago['治疗计划'][i]),("智齿" not in fourmonthago['诊断名称'][i]),("智齿" not in fourmonthago['消费项目'][i])]): #非智齿
                                baya_idx.append(i)
                                
        
        set_baya = set(fourmonthago.loc[baya_idx,'关联号'].value_counts().index) # 在时间合格的人群中，进一步找出其他字段合格的人群
        final_call = list(self.call_og & set_baya) #找到与模型预测结果的交集

        #dataout = fourmonthago.loc[fourmonthago['关联号'].isin(final_call),:] ## 若需要数据，则在结果同时也返回该数据

        #sub_valid['关联号'] = sub_valid['关联号'].astype(str)
        #final_out = sub_valid[sub_valid['关联号'].isin(final_call)] #从原数据集中根据关联号筛出合格人员的所有信息

        print("子进程", p, "完成")


        return final_call
        
        
    def find_valid(self):
        #multiprocessing.set_start_method('spawn')
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), maxtasksperchild=1)
        result = []

        for p in range(len(self.pat_list)):
            result.append(pool.apply_async(self.valid_og, (p,self.valid_list[p],self.pat_list[p])))
    
        pool.close()
        pool.join() # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
        print("主进程终止")


        return [i.get() for i in result]

    
if __name__ == "__main__":
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)" ##必须保留
    #pool = multiprocessing.Pool()
    #pat_list = tuple([sub_pat1,sub_pat2,sub_pat3,sub_pat4])
    #imp_list = tuple([sub_imp1,sub_imp2,sub_imp3,sub_imp4])
    #early = datetime.date(2019, 1, 1)
    #interval = 365

    #for p in range(len(pat_list)):
        # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
     #   pool.apply_async(valid_implant, (p,pat_list[p], imp_list[p], early, interval,))

    #pool.close()
    #pool.join() # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
