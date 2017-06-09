#-*coding:utf-8 -*-
'''
数据集准备（频繁项挖掘)
'''
import re

import datetime
from pyspark import SparkContext
import sys

sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")
#####住院数据预处理
###（年份,个人编号 1,(医院等级 5，住院天数，药品费 10,起付线 14,报销比例 15，统筹账户支付 17，入院日期，入院月份，出院病种编号 26(以编号前3位为准))))）
def hospitalProcessing(line):
    ### 医院等级的划分
    line=line.encode("utf-8").split(",")
    ### 医院等级的划分
    if (line[5] == '无等级'):
        line[5] = 'None'
    elif (line[5] == '一级'):
        line[5] = 'One'
    elif (line[5] == '二级'):
        line[5] = 'Two'
    elif (line[5] == '三级'):
        line[5] = 'Three'
    elif (line[5] == '社区'):
        line[5] = 'Four'
    else:
        line[5] = 'Five'
    for i in range(6,20):
        if line[i]=="":
            line[i]='0'
    if(line[21]!="" and line[22]!=""):       ###可能存在未记录出院时间和住院时间的住院记录
        inHospital = line[21]
        outHospital = line[22]
        s = inHospital.strip("").split('-')
        t = outHospital.strip("").split('-')
        s[1] = re.sub("\D", "", s[1])  ##提取其中数字部分
        t[1] = re.sub("\D", "", t[1])  ##提取其中数字部分
        if len(s[1]) < 2:
            s[1] = '0' + s[1]
        if len(s[0]) < 2:
            s[0] = '0' + s[0]
        if len(t[1]) < 2:
            t[1] = '0' + t[1]
        if len(t[0]) < 2:
            t[0] = '0' + t[0]
        d1 = datetime.datetime(int('20' + s[2]), int(s[1]), int(s[0]))
        d2 = datetime.datetime(int('20' + t[2]), int(t[1]), int(t[0]))
        days = (d2 - d1).days
        return (('20'+line[21][-2:],line[1]),(line[5],days,line[10],line[14],line[15],line[17],'20'+line[21][-2:],s[1],line[26][0:3]))
    else:
        return (str(999999),1)



###(年份,个人编号 0，年度工资 5)存在重复
def chargeProcessing(line):
    line = line.encode("utf-8").split(",")
    if line[2][0:4]>'2006' and line[2][0:2]<'2016' and line[3]=='310':
        if line[5]=="":
            line[5]='0'
        return((line[2][0:4],line[0],line[5]),1)
    else:
        return(str(999999),1)

####去重
def removeDupl(value1,value2):
    return value1

####计算年龄
#####(个人编号,(身份,性质，年龄,性别,月度工资，医院等级,住院天数,药品费，起付线，报销比例，入院日期，入院月份，出院病种,统筹账户支付))
def ageComputed((key, value)):
    if value[13]!="":
        currentDate = value[11]  ###入院日期
        birthDate = value[2][0:4]  ###出生日期
        age = int(currentDate) - int(birthDate)  ##计算住院时年龄
        sex=value[3]
        ####对参保人员身份进行处理
        if (sex == '1'):
            if (age >= 60):
               identity='retired'
            else:
               identity = 'working'
        else:
            if (age >= 50):
                identity = 'retired'
            else:
                identity = 'working'
        ####对所在单位数据进行处理
        workplace=value[1]
        if workplace=='10':
            workLevel='qy'
        elif workplace=='30':
            workLevel = 'jg'
        elif workplace=='50':
            workLevel = 'sydw'
        elif workplace=='99':
            workLevel = 'gt'
        elif workplace=='62':
            workLevel = 'hytc'
        else:
            workLevel='other'
        #####年龄进行分组
        if age in range(0, 20):
            ageGroup='age_0'
        elif age in range(20, 30):
            ageGroup='age_1'
        elif age in range(30, 40):
            ageGroup = 'age_2'
        elif age in range(40, 50):
            ageGroup = 'age_3'
        elif age in range(50, 60):
            ageGroup= 'age_4'
        elif age in range(60, 70):
            ageGroup= 'age_5'
        elif age in range(70, 80):
            ageGroup= 'age_6'
        elif age in range(80, 90):
            ageGroup= 'age_7'
        elif age in range(90, 100):
            ageGroup= 'age_8'
        elif age in range(100,150):
            ageGroup= 'age_9'
        else:
            ageGroup= 'age_0'
        #####对平均工资进行分组
        wage=float(value[4])
        if workerDict.has_key(currentDate):
            if(wage<workerDict[currentDate]):
                wageLevel='lower'           ####低水平收入者
            elif wage>=workerDict[currentDate] and wage<=3*workerDict[currentDate]:
                wageLevel='middle'           ####中等收入人群
            else:
                wageLevel='higher'           ####高收入人群
        else:
            return(str(999999),0)
        if value[3]=='1':
            sex='man'
        else:
            sex='woman'
        ####对单次住院天数进行离散化处理
        days=int(value[6])
        if days<7:
            dayslevel='l_o_w'        ###less than one week
        elif days>7 and days<=14:
            dayslevel = 'l_w_w'     ###less than two weeks
        elif days>14 and days<=21:
            dayslevel = 'l_t_w'     ###less than three weeks
        elif days>14 and days<=21:
            dayslevel = 'l_o_m'     ###less than one month
        else:
            dayslevel = 'm_o_m'     ###more than one month
        ######对单次统筹费用进行离散化处理
        groupfees=float(value[10])
        if(groupfees<3*wage):
            risk='low_risk'
        elif groupfees>=3*wage and groupfees<9*wage:
            risk = 'middle_risk'
        else:
            risk = 'high_risk'
        ####对月份数据进行处理
        month=value[12]
        if(month=='01'):
            monthLevel='Jan'
        elif(month=='02'):
            monthLevel='Feb'
        elif (month == '03'):
            monthLevel = 'Mar'
        elif (month == '04'):
            monthLevel = 'Apr'
        elif (month == '05'):
            monthLevel = 'May'
        elif (month == '06'):
            monthLevel = 'Jun'
        elif (month == '07'):
            monthLevel = 'Jul'
        elif (month == '08'):
            monthLevel = 'Aug'
        elif (month == '09'):
            monthLevel = 'Sep'
        elif (month == '10'):
            monthLevel = 'Oct'
        elif (month == '11'):
            monthLevel = 'Nov'
        else:
            monthLevel = 'Dem'
        return (key, (identity, workLevel,ageGroup,sex,wageLevel, value[5],dayslevel,
                      value[7],value[8],value[9],value[11],monthLevel,value[13],risk))
    else:
        return (str(999999),0)

####是否患有慢性病
def chroricProcessing((key,value)):
    if value[1]==None:
        return (key, (value[0][0], value[0][1], value[0][2],value[0][3],value[0][4],value[0][5],value[0][6],
                      value[0][7], value[0][8], value[0][9],'0',value[0][10],value[0][11],value[0][12],value[0][13]))
    else:
        return (key, (value[0][0], value[0][1], value[0][2], value[0][3], value[0][4], value[0][5], value[0][6],
                      value[0][7], value[0][8], value[0][9],'1', value[0][10],value[0][11],value[0][12],value[0][13]))

####((个人编号,医院等级),(住院人次))
####(个人编号,(医院等级，住院人次))
####读入职工住院数据
###（个人编号 1,(医院等级 5，住院天数,药品费 10,起付线 14,报销比例 15，统筹账户支付 17，出院病种编号 26
data = sc.textFile("/mif/data_new/worker_hospital.txt")
data=data.map(hospitalProcessing) \
    .filter(lambda (key, value): isinstance(value,int) == False) \
    .sortByKey()


#####读入职工缴费数据
######((年度,个人编号)，年度工资)

charge=sc.textFile("/mif/data_new/mode_ac43_310.txt")
charge=charge.map(chargeProcessing)\
    .filter(lambda (key,value):(isinstance(key,str)==False))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):((key[0],key[1]),key[2]))\
    .reduceByKey(removeDupl)\
    .sortByKey()

#####((年份，个人编号),(年度工资，医院等级 5，住院天数，药品费 10,起付线 14,报销比例 15，统筹账户支付 17，入院日期，入院月份,出院病种编号 26
#####(个人编号,(年度工资，医院等级 5，住院天数，药品费 10,起付线 14,报销比例 15，统筹账户支付 17，入院日期，入院月份,出院病种编号 26
hospitalCharge=data.join(charge)\
    .map(lambda (key,value):(key,(value[1],value[0][0],value[0][1],value[0][2],value[0][3],value
                             [0][4],value[0][5],value[0][6],value[0][7],value[0][8])))\
    .map(lambda (key,value):(key[1],(value[0],value[1],value[2],value[3],value[4],value
                             [5],value[6],value[7],value[8],value[9])))
###在职人员工资
workerDict={}
worker=open('workerwage.txt')
for line in worker:
    line=line.strip("\n").encode('utf-8').split(',')
    workerDict[line[0]]=float(line[1])

#####读入职工个人信息
worker=sc.textFile('/mif/mode_ac01_310.txt')
####(个人编号,(身份,性质，出生日期(yyyy-mm-dd),性别))
worker=worker.map(lambda line:line.encode("utf-8").split(","))\
    .map(lambda line:(line[1],(line[2:6])))\
    .sortByKey()

# #####(个人编号,(身份,性质，出生日期,性别,年度工资，医院等级,住院天数,药品费，起付线，报销比例，入院日期，入院月份,统筹账户支付，出院病种))
# #####(个人编号,(身份,性质，年龄,性别,年度工资，医院等级,住院天数,药品费，起付线，报销比例，出院病种，统筹账户支付))
workerHospitalCharge=hospitalCharge.join(worker) \
    .map(lambda (key,value):(key, (value[1][0], value[1][1],value[1][2], value[1][3],value[0][0], value[0][1],value[0][2],value[0][3],value[0][4],
                                   value[0][5],value[0][6],value[0][7],value[0][8],value[0][9])))\
    .map(ageComputed)\
    .filter(lambda (key, value): isinstance(value,int) == False)\
    .sortByKey()
#
# # #####读入职工慢性病登记信息
chroric=sc.textFile("/mif/data_new/worker_chroric_regist.txt")
chroric=chroric.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],'1'))\
    .sortByKey()

# #####(个人编号,(身份,性质，年龄,性别,年度工资，医院等级,住院天数,药品费，起付线，报销比例，出院病种，统筹账户支付))
# #####(个人编号,(身份,性质，年龄,性别,年度工资，医院等级,住院天数,药品费，起付线，报销比例，出院病种，是否患有慢性病,统筹账户支付))
result=workerHospitalCharge.leftOuterJoin(chroric)\
    .map(chroricProcessing)\
    .sortByKey()

#####(个人编号,(身份,性质，年龄,性别,年度工资，医院等级,住院天数，入院月份，出院病种，统筹费用支出))
out = open('fpm_all.txt', 'w+')
for (key,value) in result.collect():
    out.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(key,value[0],value[1],value[2],value[3],value[4],value[5],value[6],
                                                 value[12],value[13],value[14]))
out.close()
