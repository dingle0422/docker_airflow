
import multiprocessing
import datetime
import gc
#找出种植前的所有消费记录


class sub_implant(object):
    def __init__(self,pat_list,imp_list,early,interval):
        
        self.pat_list = pat_list
        self.imp_list = imp_list
        self.early = early
        self.interval = interval


    def valid_implant(self,p,sub_pat, sub_imp, earliest, interval):
        """
        sub_pat: 病人id子集
        sub_imp: 相应消费数据子集
        earliest: 时间截取下限，即时间窗口最早日期(必须以时间戳形式带入，如datetime.date(2019, 1, 1))
        interval：时间间隔、周期
        """
        print("子进程",p,'进入')
        sub_imp['implant'] = 0
        
        for i in sub_pat: #把每个种植过的患者筛出
            pat = sub_imp[sub_imp['关联号'].isin([i])]
            implant_dates = pat[pat['收费分类'].isin(['种植费'])]['date'].values

            first_implant = implant_dates.min() #找出该患者第一次种植牙的时间
            yearago = first_implant - datetime.timedelta(days = interval) #365

            #只取第一次种植之前一年的记录 ##########################20201104 
            if yearago > earliest: #根据数据时间范围自行修改 datetime.date(2019, 1, 1)
                sub_imp.loc[pat[(pat['date'].values < first_implant)&(pat['date'].values > yearago)].index, "implant"] = "selected"
            else:
                continue
        print("子进程",p,"完成")



        return sub_imp[sub_imp['implant'] == "selected"]

    
    
    
    def find_valid(self):
        #multiprocessing.set_start_method('spawn')
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), maxtasksperchild=1)
        result = []
        for p in range(len(self.pat_list)):
            result.append(pool.apply_async(self.valid_implant, (p,self.pat_list[p], self.imp_list[p], self.early, self.interval,)))


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
