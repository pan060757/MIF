#-*-coding:utf-8-*-
'''
获取脑梗死病人的就诊序号数据（2015年度住院费用总支出总高，统筹费用支出最高）
获取高血压病人的就诊序号数据（2015年度住院费用总支出第2高，统筹费用支出最2高）
获取腰椎间盘突出病人的就诊序号数据（2015年度住院人次最高，住院费用总支出和统筹费用支出也相对较高）
'''
from pyspark import SparkContext
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

sc=SparkContext()
hospital=sc.textFile('/mif/data_new/worker_hospital.txt')
####（就医序号）
ngs=hospital.map(lambda line:line.split(','))\
    .filter(lambda line:line[27]=='脑梗死'.decode("utf-8"))\
    .map(lambda line:(line[2],1))\
    .sortByKey()

####（就医序号）
gxy=hospital.map(lambda line:line.split(','))\
    .filter(lambda line:line[27]=='高血压'.decode("utf-8"))\
    .map(lambda line:(line[2],1))\
    .sortByKey()

####（就医序号）
yzjptc=hospital.map(lambda line:line.split(','))\
    .filter(lambda line:line[27]=='腰椎间盘突出'.decode("utf-8"))\
    .map(lambda line:(line[2],1))\
    .sortByKey()

####（就医序号）
out1=open('output/numberOfNGS.csv','w+')
for (key,value) in ngs.collect():
    out1.write("%s\n"%(key))
out1.close()

out2=open('output/numberOfGXY.csv','w+')
for (key,value) in gxy.collect():
    out2.write("%s\n"%(key))
out2.close()

out3=open('output/numberOfYZJPTC.csv','w+')
for (key,value) in yzjptc.collect():
    out3.write("%s\n"%(key))
out3.close()