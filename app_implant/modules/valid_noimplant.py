
import multiprocessing
import datetime
import gc
#找出种植前的所有消费记录


class sub_noimplant(object):
    def __init__(self,pat_list,imp_list,early,interval):
        
        self.pat_list = pat_list
        self.imp_list = imp_list
        self.early = early
        self.interval = interval

        
    def valid_noimplant(self,p,sub_PAT, sub_IMP, earliest, interval):
        """
        sub_PAT: 病人id子集
        sub_IMP: 相应消费数据子集
        earliest: 时间截取下限，即时间窗口最早日期(必须以时间戳形式带入，如datetime.date(2019, 1, 1))
        interval：时间间隔、周期
        """
        print("子进程",p,'进入')
        count = 0
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


        print("子进程",p,"完成")
        return sub_IMP[sub_IMP['noimplant'].values == "selected"]

    
    
    
    def find_valid(self):
        #multiprocessing.set_start_method('spawn')
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), maxtasksperchild=1)

        result = []
        for p in range(len(self.pat_list)):
            result.append(pool.apply_async(self.valid_noimplant, (p,self.pat_list[p], self.imp_list[p], self.early, self.interval,)))
    
        pool.close()
        pool.join() # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束


        print("主进程终止")
        return [i.get() for i in result]

    
if __name__ == "__main__":
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)" ##必须保留
    #pool = multiprocessing.Pool()
    #pat_list = tuple([sub_PAT1,sub_PAT2,sub_PAT3,sub_PAT4])
    #imp_list = tuple([sub_IMP1,sub_IMP2,sub_IMP3,sub_IMP4])
    #early = datetime.date(2019, 1, 1)
    #interval = 365

    #for p in range(len(pat_list)):
        # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
     #   pool.apply_async(valid_implant, (p,pat_list[p], imp_list[p], early, interval,))

    #pool.close()
    #pool.join() # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
