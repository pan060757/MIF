#-*-coding:utf-8 -*-
'''
职工统筹账户收入情况和参保人数(根据每年的应缴类型)
'''
from pyspark import SparkContext
import string
###按年度统计每年的缴费人数
def precessing(line):
    line=line.encode('utf-8').split(',')
    for i in range(5,12):
        if(line[i]==""):
            line[i]='0'
    return line

def combine(line1,line2):
    line1=list(line1)
    line2=list(line2)
    line1.extend(line2)
    return line1

sc=SparkContext()
data=sc.textFile('/mif/data_new/mode_ac43_310.txt')
###((年份，个人编号,应缴类型),(划入统筹账户,划入个人账户，次数))
###((年份,应缴类型)(划入统筹账户,划入个人账户))
###(年份,(应缴类型,划入统筹账户,划入个人账户))
data=data.map(precessing)\
    .filter(lambda line:line[3]=='310')\
    .map(lambda line:((line[2][0:4],line[0],line[4]),(float(line[10]),float(line[11]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .map(lambda (key,value):((key[0],key[2]),(value[0],value[1])))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
    .map(lambda(key,value):(key[0],(key[1],value[0],value[1])))\
    .reduceByKey(combine)\
    .sortByKey()

###((年份,(应缴类型，统筹账户收入,个人账户收入))
out=open('output/workerChargeByType.csv','w+')
for (key,value) in data.collect():
    line = reduce(lambda a, b: "%s,%s" % (a, b), value).encode("utf-8")
    out.write("%s,%s\n" % (key,line))
out.close()

