#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib as mpl
from sklearn.metrics import classification_report,confusion_matrix
import re
import collections
import pandas as pd

# In[4]:


# 混淆矩阵
def confusion_matrix_lucid(pred,test):
    '''
    lucid confusion matrix
    '''
    import pandas as pd
    print(classification_report(test,pred))
    cm = confusion_matrix(test,pred).ravel() # tn,fp,fn,tp
    return pd.DataFrame({"Actual_implant": [cm[3],cm[2]],"Actual_noimplant":[cm[1],cm[0]]}, index = ['predict_implant','predict_noimplant'])


# In[3]:


def diag_dist(data, top = 20):
    '''
    柱状图体现数据集中诊断名称的出现频数
    '''
    l = []
    for i in data['诊断名称']:
        l += [o for o in re.sub(r'[^\u4e00-\u9fff]', ' ', str(i)).split(" ") if (o != "")&(len(o) != 1)]
    uni_diag = set(l)
    
    count = collections.defaultdict(int)
    for i in list(data['诊断名称']):
        forsearch = set([o for o in re.sub(r'[^\u4e00-\u9fff]', ' ', str(i)).split(" ") if (o != "")&(len(o) != 1)])
        for o in uni_diag:
            if o in forsearch:
                count[o] += 1
    

    diag_plot =  sorted(count.items() , key = lambda x: x[1], reverse = True)    
    mpl.rcParams['font.sans-serif'] = ['KaiTi']
    mpl.rcParams['font.serif'] = ['KaiTi']
    font_set = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=15)

    plt.figure(figsize = (25,8))
    plt.title("诊断名称分布",fontsize = 20)
    x = [i[0] for i in diag_plot][:top]
    x.reverse()
    y = [i[1] for i in diag_plot][:top]
    y.reverse()
    plt.bar(x = 0, bottom = x, width = y, height=0.5, orientation="horizontal",color = 'g')
   # plt.bar([i[0] for i in diag_plot][:top], [i[1] for i in diag_plot][:top],color = 'darkgreen')
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.xlabel("频率",fontsize = 18)
    plt.ylabel("特征名称",fontsize= 18)
    return

