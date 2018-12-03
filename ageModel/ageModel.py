#-*-coding:utf-8 -*-
'''
Just for Test
假设n年之后，人数不变的情况下，n年之后的年龄结构是什么样
'''
import pandas as pd
#####进行年龄段的划分
def ageDivided(year_dict):
    new_dict={'20岁以下':0,'20-30':0,'30-40':0,'40-50':0,'50-60':0,'60-70':0,'70-80':0,'80-90':0,'90-100':0,'100-150':0}
    for age,number in year_dict.items():
        value = number
        if age in range(0, 20):
            new_dict['20岁以下']=new_dict['20岁以下']+value
        elif age in range(20, 30):
            new_dict['20-30'] = new_dict['20-30']+value
        elif age in range(30, 40):
            new_dict['30-40'] = new_dict['30-40']+value
        elif age in range(40, 50):
            new_dict['40-50'] = new_dict['40-50']+value
        elif age in range(50, 60):
            new_dict['50-60'] = new_dict['50-60']+value
        elif age in range(60, 70):
            new_dict['60-70'] = new_dict['60-70']+value
        elif age in range(70, 80):
            new_dict['70-80'] = new_dict['70-80']+value
        elif age in range(80, 90):
            new_dict['80-90'] = new_dict['80-90']+value
        elif age in range(90, 100):
            new_dict['90-100'] = new_dict['90-100']+value
        else:
            new_dict['100-150'] = new_dict['100-150']+value
    return new_dict

data=pd.read_csv("dataset/ageComputed.csv")
ageList=data['age']
numberList=data['number']
year_dict={}
n=1            ####表示n年之后
for i in range(0,len(ageList)):
    year_dict[ageList[i]]=numberList[i]

new_dict={}    #####n年之后的年龄结构
for key,value in year_dict.items():
    new_dict[int(key)+n]=value
####进行年龄段的划分
age_distribution={}
age_distribution=ageDivided(new_dict)
out=open("dataset/ageOfNyears.csv","w+")
for key,value in age_distribution.items():
    out.write("%s,%d\n"%(key,value))
out.close()



