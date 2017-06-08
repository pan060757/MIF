#-*-coding:utf-8 -*-
'''
对退休人员的缴费比例进行分析
'''
from pyspark import SparkContext
import string

###(（缴费年份，个人编号,缴费比例），缴费次数）
def preprocessing(line):
    try:
        wage=float(line[6])         ###缴费工资
        fees=float(line[7])         ###缴费金额
        if(wage!=0):
            ratio=round(fees/wage * 100, 0)
            return ((line[2][0:4],line[0],str(ratio)),1)
        else:
            return (str(999999),0)
    except Exception:
            return (str(999999), 0)


####年龄计算和状态划分
###(个人编号,(缴费年份,缴费比例，缴费次数,年龄，性别))
def ageComputed((key,value)):
    try:
        current_year=value[0][0]
        birth_year=value[1][0][0:4]
        sex=value[1][1]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        if (sex == '1'):
            if (age < 60):
                return (key,(value[0][0],value[0][1],value[0][2],age,value[1][1],'0'))
            else:
                return (key,(value[0][0],value[0][1],value[0][2],age,value[1][1],'1'))
        else:
            if (age < 50):
                return (key,(value[0][0],value[0][1],value[0][2],age,value[1][1],'0'))
            else:
                return (key,(value[0][0],value[0][1],value[0][2],age,value[1][1],'1'))
    except Exception:
        return (str(999999),0)

####统计退休人员缴费比例
####((年份，缴费比例)，次数)
def retireRatio((key,value)):
    if value[5]=='1':
        return ((value[0],value[1]),1)
    else:
        return (str(999999),0)


sc=SparkContext()
data=sc.textFile('/mif/data/mode_ac43_310.txt')
###(（缴费年份，个人编号,缴费比例），缴费次数）
###(个人编号,(缴费年份,缴费比例，缴费次数）
data1=data.map(lambda line:line.encode("utf-8").split(",")) \
    .filter(lambda line: line[3] == '310') \
    .map(preprocessing) \
    .filter(lambda (key,value):isinstance(key,str)==False) \
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda (key,value):(key[1],(key[0],key[2],value)))

####((个人编号),(出生日期(yyyy-mm-dd)、性别))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))
#
# ###(个人编号,(缴费年份,缴费比例，缴费次数,年龄，性别）
# ####((年份，缴费比例)，次数)
#####((年份，(缴费比例，次数)
result=data1.join(data2)\
    .map(ageComputed)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .map(retireRatio)\
    .filter(lambda (key,value):(isinstance(key,str)==False))\
    .reduceByKey(lambda a,b:a+b) \
    .map(lambda (key,value):(key[0],(key[1],value)))\
    .sortByKey()

out=open("output/retireChargeRatio.csv","w+")
for (key,value) in result.collect():
    line = reduce(lambda a, b: "%s,%s" % (a, b), value).encode("utf-8")
    out.write("%s,%s\n" % (key, line))
out.close()