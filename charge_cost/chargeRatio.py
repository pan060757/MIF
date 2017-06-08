#-*-coding:utf-8 -*-
'''
统计每年的缴费比例分布情况(这边只统计4%，9%，6%的情况)
'''

from pyspark import SparkContext
import string

####（(年份，缴费比例)，人次）
def preprocessing(line):
    try:
        wage=float(line[6])         ###缴费工资
        fees=float(line[7])         ###缴费金额
        if(wage!=0):
            ratio=round(fees/wage * 100, 0)
            if ratio==9 or ratio==6 or ratio==4:
                return ((line[2][0:4],str(ratio)),1)
            else:
                return ((line[2][0:4],'0'),1)
        else:
            return (str(99999),0)

    except Exception:
            return (str(999999), 0)

sc=SparkContext()
data=sc.textFile('/mif/data/mode_ac43_310.txt')
result=data.map(lambda line:line.encode("utf-8").split(','))\
    .filter(lambda line:line[3]=='310')\
    .map(preprocessing)\
    .filter(lambda (key,value):isinstance(key,str)==False)\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()

out=open("output/chargeRatio.csv",'w+')
####（(年份，缴费比例)，人次）
for (key,value) in result.collect():
    out.write("%s,%s,%d\n"%(key[0],key[1],value))
out.close()