#-*-coding:utf-8 -*-
'''
病人住院费用影响因素分析
'''
import numpy as np
from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score

from sklearn import ensemble

from sklearn import cross_validation

def train(clf,x_train,y_train,x_test,y_test):
    clf.fit(x_train,y_train)
    result = clf.predict(x_test)
    print (result)

data=open("dataset/Data2015.txt",'r',encoding='utf-8')
out=open("dataset/rf_result.txt",'w+')
keys=[]
values=[]
test=[]
###按行读取训练集文件
for line in data:
    key=line.strip("\n").split(',')
    keys.append(key[1:30])
    values.append(key[30])
training=np.array(keys)
training_set=training.reshape(training.shape[0],29)
label=np.array(values).ravel()         ##只提取类标签

####对训练集进行划分(7:3比例进行划分)
x_train, x_test, y_train, y_test =cross_validation.train_test_split(
    training_set,label, test_size=0.3, random_state=0)
rf_model= ensemble.RandomForestRegressor(n_estimators=90)
train(rf_model,x_train,y_train,x_test,y_test)
print (rf_model.feature_importances_)