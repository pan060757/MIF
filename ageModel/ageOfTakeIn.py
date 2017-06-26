#-*-coding:utf-8 -*-
'''
统计参保人员首次加入医疗保险的年龄所属年龄段
'''
from pyspark import SparkContext
import string
#####找到该参保人员首次参保的年龄
def firstYearOfTakeIn(value1,value2):
    return value1 if value1<value2 else value2     ###python三目表达式

###对于进行连接的数据进行处理，计算参保人员年龄信息
def processing_4((key,value)):
    try:
        current_year=value[0]
        birth_year=value[1][0][0:4]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        return (key,(current_year,age))
    except Exception:
        return (str(999999),0)

def ageDivided((key,value)):
    year =value[0]
    age =value[1]
    if age in range(0, 20):
        return (year + ',' + '0',1)
    elif age in range(20, 30):
        return (year + ',' + '1',1)
    elif age in range(30, 40):
        return (year + ',' + '2',1)
    elif age in range(40, 50):
        return (year + ',' + '3',1)
    elif age in range(50, 60):
        return (year + ',' + '4',1)
    elif age in range(60,70):
        return (year + ',' + '5',1)
    elif age in range(70, 80):
        return (year + ',' + '6',1)
    elif age in range(80, 90):
        return (year + ',' + '7',1)
    elif age in range(90, 100):
        return (year + ',' + '8',1)
    elif age in range(100, 150):
        return (year + ',' + '9',1)
    else:
        return (str(999999), 0)

###程序入口
sc=SparkContext()
data1=sc.textFile('/mif/data_new/mode_ac43_310.txt')
##每步map对应的键值对转化
#((number,year),1)
#(number,year)
data1=data1.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[3]=='310'and line[4]=='10')\
    .map(lambda line:((line[0],line[2][0:4]),1))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):(key[0],key[1]))\
    .reduceByKey(firstYearOfTakeIn)\
    .sortByKey()


####(number,(indentify,xingzhi,birthdate,sex))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))

data=data1.join(data2)


#(year+','+(年龄段)+','+sex,1)
#（year,(年龄段，number))
###统计每年每个年龄段的缴费人数(首次参保年龄所属年龄段)
result=data.map(processing_4)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .map(ageDivided)\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda(key,value):(len(key.split(','))>1))\
    .sortByKey()

####(年份,(年龄段，参保人数))
out=open('output/ageOfTakeIn.csv','w+')
for (key,value) in result.collect():
   out.write("%s,%d\n"%(key,value))
out.close()
