#-*- coding:utf-8 -*-
'''
频繁项集挖掘
'''
from pyspark import SparkContext
import sys
sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")

from pyspark.mllib.fpm import FPGrowth


data = sc.textFile("file:///home/edu/mif/python/ss/stastic_analysis/output/fpm_data.txt")
data = data.map(lambda line: line.encode("utf-8").split(','))\
       .map(lambda line:(line[0],(line[3:])))

data_bkt=data.map(lambda (k, v): v)
data_bkt.cache()
model = FPGrowth.train(data_bkt, 0.3)
minConfidence=0.1
fitems = model.freqItemsets().collect()
out = open('output/fpm_2015.txt', 'w+')
for itemset in fitems:
    line = reduce(lambda a, b: "%s\t%s"%(a,b), itemset.items).encode("utf-8")
    out.write("%d\t%s\n" % (itemset.freq,line))
out.close()
