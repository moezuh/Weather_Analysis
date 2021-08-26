import os
import subprocess
from datetime import datetime as dt
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import size
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder.master("yarn")\
        .config("spark.port.maxRetries", 100)\
        .config("spark.executor.instances", "6")\
        .config("spark.executor.cores", "4")\
        .config("spark.executor.memory", "8G")\
        .config("spark.driver.memory", "2G")\
        .config("spark.dynamicAllocation.enabled", "false")\
        .config("spark.yarn.queue", "Low")\
        .config("spark.port.maxRetries", 100)\
        .appName("New_Data_Reader_V0")\
        .getOrCreate()
file_path = "/data/weather/"

# Year 2010
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2010')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2010')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2010',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanData

# Year 2011
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2011')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2011')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2011',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)

# Year 2012
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2012')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2012')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2012',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)

# Year 2013
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2013')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2013')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2013',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)

# Year 2014
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2014')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2014')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2014',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)


# Year 2015
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2015')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2015')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2015',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)

# Year 2016
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2016')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2016')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2016',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)

# Year 2017
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2017')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2017')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2017',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)

# Year 2018
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2018')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2018')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2018',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)


# Year 2019
data = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path + '2019')
name = data.schema.names
name = str(name).strip("['']").split(' ')
names = []
for item in name:
    if len(item)>0:
        names.append(item)

rdd1 = data.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x.replace('*', ''))  # Remove asterisks from data
rdd5 = rdd4.map(lambda x: x[2: -2])
rdd5.saveAsTextFile('/user/ulhasamz/weather' + '2019')
newInData = spark.read.csv('/user/ulhasamz/weather' + '2019',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
cleanerData = cleanerData.union(cleanData)

rawData = cleanerData
rawData.createOrReplaceTempView("Data")
# Convert string columns into float and clean PRCP column
df = spark.sql("SELECT STN, YEARMODA, FLOAT (TEMP), FLOAT(DEWP) AS DEWP, SLP, FLOAT(STP) AS STP, VISIB,  WDSP, MXSPD, FLOAT(GUST) AS GUST, FLOAT(MAX) AS MAX, FLOAT(MIN) AS MIN, FLOAT(substring(PRCP, 0, 5)) AS PRCP, SNDP, FRSHTT FROM Data")


# Questions:
# Q1:
# Max
qmax = df.where((df.TEMP != '9999.9'))
year = 2010
amax = qmax.select(qmax.STN, qmax.YEARMODA, qmax.TEMP).where((qmax.YEARMODA.like("2010%"))).orderBy(qmax.TEMP.desc(), qmax.STN, qmax.YEARMODA).limit(1)
for i in range(1, 10):
	amax = amax.union(qmax.select(qmax.STN, qmax.YEARMODA, qmax.TEMP).where((qmax.YEARMODA.like(str(year + i) + "%"))).orderBy(qmax.TEMP.desc(), qmax.STN, qmax.YEARMODA).limit(1))
amax.show()

# Min
qmin = df.where((df.TEMP != '9999.9'))
year = 2010
amin = qmin.select(qmin.STN, qmin.YEARMODA, qmin.TEMP).where((qmin.YEARMODA.like("2010%"))).orderBy(qmin.TEMP, qmin.STN, qmin.YEARMODA).limit(1)
for i in range(1, 10):
	amin = amin.union(qmin.select(qmin.STN, qmin.YEARMODA, qmin.TEMP).where((qmin.YEARMODA.like(str(year + i) + "%"))).orderBy(qmin.TEMP, qmin.STN, qmin.YEARMODA).limit(1))
amin.show()

#Q2:
#Hottest day among all the years
qmax.select(qmax.STN, qmax.YEARMODA, qmax.TEMP).orderBy(qmax.TEMP.desc(), qmax.STN, qmax.YEARMODA).limit(1).show()

#Coldest day among all the years
qmin.select(qmin.STN, qmin.YEARMODA, qmin.TEMP).orderBy(qmin.TEMP, qmin.STN, qmin.YEARMODA).limit(1).show()

#Q3:
# Max Precipitation for 2015
p = df.where((df.PRCP != '99.99') & (df.YEARMODA.like('2015%')))
p.select(p.STN, p.YEARMODA, p.PRCP).orderBy(p.PRCP.desc(), p.STN, p.YEARMODA).limit(1).show()

# Min Precipitation for 2015
p.select(p.STN, p.YEARMODA, p.PRCP).orderBy(p.PRCP).limit(1).show()

#Q4:
# Percentage missing values of STP for 2019
n = df.where((df.STP == '99.99') & (df.YEARMODA.like('2019%'))).count() 
d = df.where((df.YEARMODA.like('2019%'))).count().show()
print(float(n)*100/float(d))


# Percentage missing values of STN for 2019
n = df.where((df.STN == '999999') & (df.YEARMODA.like('2019%'))).count()
d = df.where((df.YEARMODA.like('2019%'))).count()
print(float(n)*100/float(d))

#Q5:
# Station code with max wind gust for 2019
g = df.where((df.GUST != '999.9') & (df.YEARMODA.like('2019%')))
g.select(g.STN, g.YEARMODA, g.GUST).orderBy(g.GUST.desc(), g.STN, g.YEARMODA).limit(1).show()


