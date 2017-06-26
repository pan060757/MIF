#-*-coding:utf-8-*-
'''
统计异常住院记录
'''
from pyspark import SparkContext
import sys

sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")
data=sc.textFile("file:///home/edu/mif/python/ss/mif/outlier/output/test.csv")
data=data.map(lambda line :line.split(","))
out = open('output/feesOutliers.csv', 'w+')
for line in data.collect():
  line= reduce(lambda a, b: "%s,%s"%(a,b),line).encode("utf-8")
  out.write("%s\n"%(line))
out.close()
