# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

data=[(1,"vinay","boddu","maths","hyderabad",{"age":26 ,"weight":60},[1,2,3],(1,"super")),
      (2,"divya","boddu","social","banglore",{"age":25,"weight":65},[3,4,5],(2,"best")),
      (3,"viru","sehwag","maths","hyderabad",{"age":26,"weight":66},[3,5,6],(3,"better"))]
schema=["sno","fname","lname","favsub","location","details","serails","essentials"]
schemaa=StructType([
  StructField("sno",IntegerType(),True),
  StructField("fname",StringType(),True),
  StructField("lname",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("location",StringType(),True),
  StructField("details",MapType(StringType(),IntegerType()),True),
  StructField("serails",ArrayType(IntegerType()),True),
  StructField("essentials",StructType([
    StructField("snoo",IntegerType(),True),
    StructField("behaviour",StringType(),True)
  ]),True)
])

data1=[(1,"vinay","boddu","projectbase","hyderabad",{"age":27 ,"weight":65},[1,2,3],(1,"super")),
      (4,"hooda","raj","social","banglore",{"age":25,"weight":65},[3,4,5],(2,"best"))]
schema1=["snoo","fname","lastname","favsubject","location","details","serails","essentials"]

df=spark.createDataFrame(data=data,schema=schemaa)
df.display()
# df1=spark.createDataFrame(data=data).toDF(*schema)
# rdd=spark.sparkContext.parallelize(data)
# df2=spark.createDataFrame(rdd).toDF(*schema)
# df3=rdd.toDF(schema)
# df4=rdd.toDF(schemaa)

dff=spark.createDataFrame(data=data1,schema=schema1)
dff.display()

# COMMAND ----------

##select
# df1=df.select("sno","fname","lname","location")
# df2=df.select(df["sno"],df["fname"],df["lname"])
# df3=df.select(df.sno,df.fname,df.lname,df.location)
# df4=df.select(col("sno"),col("fname"),col("lname"))
# df5=df.select("*")
# df6=df.select(df.columns)
# df7=df.select(*schema)
# df8=df.select([col for col in df.columns])
# df9=df.select("essentials.*")
# df10=df.select(df.colRegex("`^.*l.*`"))
# df10.display()

##add and update columns
# df1=df.withColumn("newcol",lit("super"))
# df2=df.withColumn("sno",col("sno")*10)
# df3=df.withColumn("sno",col("sno").cast("String"))
# df4=df.withColumn("newcol",concat_ws(" ","fname","lname"))
# df5=df.withColumn("newcol",when(col("fname")=="vinay","super")
#                           .when(col("fname")=="divya","best")
#                           .otherwise("better"))
# df5.display()

##rename nested and normal columns
# df1=df.withColumnRenamed("sno","newsno")
# schemaaa=StructType([
#   StructField("snoooo",IntegerType(),True),
#   StructField("behaviourrs",StringType(),True)
# ])
# df2=df.select(col("sno"),col("fname"),col("lname"),col("essentials").cast(schemaaa))
# df2.display()

##drop columns
# df1=df.drop("sno","fname","lname","location")
# lis=["sno","fname","lname","essentials"]
# df2=df.drop(*lis)
# df2.display()

##where or filter
# df1=df.filter(col("fname")=="vinay")
# df2=df.filter((col("sno")==1)| (col("fname")=="divya"))
# df3=df.filter(col("fname").startswith("vi"))
# df4=df.filter(col("fname").contains("y"))
# df5=df.filter(col("fname").isin("vinay","divya"))
# lis=["sno","vinay"]
# df6=df.filter(col("fname").isin(lis))
# df7=df.filter(col("fname").like("di%"))
# df7.display()

##when or otherwise
# df1=df.withColumn("newcol",when(col("fname")=="vinay","super")
#                           .when(col("fname")=="divya","best")
#                           .otherwise("better"))
# df2=df.select(col("sno"),col("fname"),col("lname"),col("location"),when(col("location")=="hyderabad","super")
#                                                                   .otherwise("best").alias("newcol"))
# df2.display()

##distinct in pyspark
# df1=df.select("location","lname").distinct()
# df2=df.select("location","lname").dropDuplicates(["location"])
# df2.display()

##pivot
# df1=df.groupby("location").pivot("lname").agg(sum("sno"),count("*"))
# lis=["vinay","divya","kumar"]
# df2=df.groupby("location","lname").pivot("fname",lis).agg(count("*"),sum("sno"),avg("sno"),max("sno"),min("sno"),mean("sno"))
# df2.display()

##ShortType(), IntegerType(), LongType(), StringType(), DateType(), TimestampType(), FloatType(), DoubleType(), DecimalType(), MapType(), ArrayType(), StructType()

##groupby in pyspark
# df1=df.groupby("location").agg(count("*"),sum("sno"),mean("sno"))
# df2=df.groupby("location","lname").agg(count("*"),sum("sno"),min("sno"))
# df2.display()

##sort
# df1=df.sort(col("location").desc())
# df2=df.sort(col("location").desc(),col("lname").desc())
# df2.display()

##Join Type
##Inner, leftouter, rightouter, fullouter, leftanti, leftsemi
# df1=df.join(dff,df.sno==dff.sno,"inner")
# df2=df.join(dff,["sno"],"leftouter")
# df3=df.join(dff,["sno"],"leftouter").select(df["fname"],dff["lname"],dff["location"])
# df3.display()

##union and unionall and unionbyname
# df1=df.select(df.columns[0:3]).union(dff.select(dff.columns[0:3])).distinct()
# df2=df.select(df.columns[0:5]).unionByName(dff.select(dff.columns[0:5]),True)
# df2.display()

##map() in pyspark
# rdd1=df.rdd.map(lambda x: (x["fname"].upper(),x["lname"],x["location"]))
# df1=rdd1.toDF(["firstname","lastname","location"])

# def func(x):
# return (x["fname"]+","+x["lname"],x["location"],x["favsub"])
# rdd2=df.rdd.map(lambda x: func(x))
# df2=rdd2.toDF(["fullname","location","favsubject"])

# def funct(y):
# return (y["fname"]+" "+y["lname"],)
# rdd3=df.rdd.map(lambda x: funct(x))
# df3=rdd3.toDF(["fullname"])
# df3.display()

##mapPartitions()
# rdd1=df.rdd.mapPartitions(lambda x: ((y["fname"]+" "+y["lname"],y["location"]) for y in x))
# df1=rdd1.toDF(["fullname","locations"])

# def reform(partitiondata):
#   for row in partitiondata:
#     yield [row.fname, row.lname, row.location.upper()]
# rdd2=df.rdd.mapPartitions(reform)
# df2=rdd2.toDF(["firstname","lastname","location"])

# def reformat(partition):
#   for coll in partition:
#     yield [coll["fname"]+" "+"supper",coll["lname"],coll["location"]]
# rdd3=df.rdd.mapPartitions(reformat)
# df3=rdd3.toDF(["fullname","lastname","location"])

# def reform(partitionsdata):
#   appenddata=[]
#   for row in partitionsdata:
#     firstname=row.fname
#     lastname=row.lname
#     appenddata.append([firstname,lastname])
#   return iter(appenddata)
  
# rdd4=df.rdd.mapPartitions(reform)
# df4=rdd4.toDF(["fname","lname"])
# df4.display()

##Persist and Cache()
##memory_only, memory_and_disk, disk_only
df1=df.cache()
# df2=df.persist(StorageLevel.MEMORY_ONLY)
# df3=df.persist(StorageLevel.MEMORY_AND_DISK)
# df4=df.persist(StorageLevel.DISK_ONLY)
# df4.display()

# COMMAND ----------

##ArrayType column in pyspark
##explode, split, array, array_contains
# df1=df.select("fname","lname",explode("serails"))
# df2=df.select(array("fname","lname").alias("fullname"),"location")
# df3=df.select("fname","lname",split("location","a").alias("locationarray"))
# df4=df2.select('location',explode("fullname"),array_contains("fullname","vinay"))
# df4.display()

##MapType column in pyspark
# rdd1=df.rdd.map(lambda x: (x.details["weight"],))
# df1=rdd1.toDF(["weight"])
# df2=df.select(df.fname, df.lname, df.details["weight"])
# ##map functions
# df3=df.select(df.fname, df.lname, explode(df.details))
# df4=df.select(df.fname, df.lname, map_keys(df.details))
# df5=df.select(df.fname, df.lname, map_values(df.details))
# df3.display()

##Flatten nested struct column


# COMMAND ----------

# MAGIC %md
# MAGIC ##Part -2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

data=[(1,"vinay","boddu","maths","hyderabad",{"age":27,"weight":60},[1,2,3],(1,"super")),
      (2,"divya","boddu","social","banglore",{"age":26,"weight":65},[3,4,5],(2,"better")),
      (3,"viru","sehwag","maths","hyderabad",{"age":27,"weight":70},[3,6,7],(3,"best"))]
schema=["sno","fname","lname","favsub","location","stats","serials","details"]
schemaa=StructType([
  StructField("sno",IntegerType(),True),
  StructField("fname",StringType(),True),
  StructField("lname",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("location",StringType(),True),
  StructField("stats",MapType(StringType(),IntegerType()),True),
  StructField("serials",ArrayType(IntegerType()),True),
  StructField("details",StructType([
    StructField("snoo",IntegerType(),True),
    StructField("quality",StringType(),True)
  ]),True)
])

df=spark.createDataFrame(data=data,schema=schemaa)
# rdd=spark.sparkContext.parallelize(data)
# df1=rdd.toDF(schema)
# df2=rdd.toDF(schemaa)
# df3=spark.createDataFrame(rdd).toDF(*schema)
# df4=spark.createDataFrame(data).toDF(*schema)
df.display()

data1=[(1,"vinay","boddu","science","chennai",{"age":27,"weight":60},[1,2,3],(1,"super")),
      (4,"rajesh","ram","social","thrivandram",{"age":26,"weight":65},[3,4,5],(2,"better"))]
schemaa1=StructType([
  StructField("sno",IntegerType(),True),
  StructField("firstname",StringType(),True),
  StructField("lastname",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("location",StringType(),True),
  StructField("stats",MapType(StringType(),IntegerType()),True),
  StructField("serials",ArrayType(IntegerType()),True),
  StructField("details",StructType([
    StructField("snoo",IntegerType(),True),
    StructField("quality",StringType(),True)
  ]),True)
])

dff=spark.createDataFrame(data=data1,schema=schemaa1)
dff.display()

# COMMAND ----------

##select
# df1=df.select("sno","fname","lname")
# df2=df.select(df["sno"],df["fname"],df["lname"])
# df3=df.select(df.sno,df.fname,df.lname)
# df4=df.select(col("sno"),col("fname"),col("location"))
# df5=df.select("*")
# df6=df.select(df.columns)
# df7=df.select([col for col in df.columns])
# df8=df.select(*schema)
# df9=df.select("details.*")
# df10=df.select(df.colRegex("`^.name*`"))
# df10.display()

##withcolumn
# df1=df.withColumn("sno",lit(None))
# df2=df.withColumn("newsno",lit("super"))
# df3=df.withColumn("sno",col("sno").cast("String"))
# df4=df.withColumn("sno",col("sno")*10)
# df5=df.withColumn("newsno",concat_ws(" ","fname","lname"))
# df6=df.withColumn("newsno",when(col("sno")==1,"super")
#                           .when(col("sno")==2,"better")
#                           .otherwise("best"))
# df6.display()

##renamed columns and nested column
# df1=df.withColumnRenamed("sno","newsno")
# schemaaa=StructType([
#   StructField("newsnoo",IntegerType(),True),
#   StructField("quality",StringType(),True)
# ])
# df2=df.select(col("fname"),col("lname"),col("details").cast(schemaaa))
# df2.display()

##drop
# df1=df.drop("sno","fname","lname")
# lis=["sno","fname"]
# df2=df.drop(*lis)
# df2.display()

##where or filter
# df1=df.filter(col("sno")==1)
# df2=df.filter((col("sno")==1)&(col("fname")=="vinay"))
# df3=df.filter(col("fname").startswith("vi"))
# df4=df.filter(col("fname").contains("y"))
# df5=df.filter(col("fname").isin("vinay","divya"))
# lis=["sno","vinay"]
# df6=df.filter(col("fname").isin(lis))
# df7=df.filter(col("fname").like("vi%"))
# df7.display()

##when or otherwise
# df1=df.withColumn("sno",when(col("sno")==1,"super")
#                        .otherwise("best"))
# df2=df.select(col("fname"),col("lname"),when(col("fname").isin("vinay","divya"),"super")
#                                        .otherwise("best").alias("newcol"))
# df2.display()

##distinct or dropduplicates
# df1=df.select("lname").distinct()
# df2=df.select("lname","location").dropDuplicates(["location"])
# df2.display()

##pivot
# df1=df.groupby("location").pivot("lname").agg(count("*"))
# lis=["boddu"]
# df2=df.groupby("location").pivot("lname",lis).count()
# df2.display()

##Datatypes
##ShortType(),IntegerType(), LongType(), DateType(), TimestampType(), FloatType(), DoubleType(), DecimalType(), MapType(), StringType(), ArrayType(), StructType()

##groupby
# df1=df.groupby("location").count()
# df2=df.groupby("location","lname").agg(count("*"),sum("sno"),avg("sno")).filter(sum("sno")>2)
# df2.display()

##sort
# df1=df.select("*").sort(col("location").desc())
# df2=df.select("*").sort(col("location").desc(),col("lname").desc())
# df2.display()

##join types
##inner, leftouter, rightouter, fullouter, leftanti, leftsemi
# df1=df.join(dff,["sno"],"inner")
# df2=df.join(dff,df.fname==dff.firstname,"leftouter")
# df3=df.join(dff,["sno"],"leftouter").select(df["sno"],df["fname"],df["lname"],dff["stats"])
# df3.display()

##union and union all
# df1=df.union(dff)
# df2=df.select("fname", "lname").union(dff.select("firstname","lastname"))
# df3=df.select(df.columns[0:5]).unionByName(dff.select(dff.columns[0:5]),True)
# df3.display()

##map
# rdd1=df.rdd.map(lambda x: (x["fname"].upper(),x["lname"].lower()))
# df1=rdd1.toDF(["firstname","lastname"])

# def func(x):
#   firstname=x.fname.upper()
#   sno=x.sno*100
#   return firstname, sno
# rdd2=df.rdd.map(lambda x: func(x))
# df2=rdd2.toDF(["fname","snoo"])
# df2.display()

##mappartitions
# rdd1=df.rdd.mapPartitions(lambda x: ((y["fname"].upper(), y["sno"]+100,y["lname"].lower()) for y in x))
# df1=rdd1.toDF(["fname","snoo"])

# def reform(par):
#   for y in par:
#     lastname=y.lname.upper()
#     firstname=y.fname.upper()
#     yield lastname, firstname
# rdd2=df.rdd.mapPartitions(reform)
# df2=rdd2.toDF(["lname","fname"])

# def reformat(par):
#   updatedata=[]
#   for y in par:
#     lastname=y.lname.upper()
#     firstname=y.fname.upper()
#     updatedata.append([lastname, firstname])
#   return iter(updatedata)
# rdd3=df.rdd.mapPartitions(reformat)
# df3=rdd3.toDF(["lname","fname"])
# df3.display()

##flatmap() in pyspark
# rdd1=df.rdd.flatMap(lambda x: (x["fname"].split("i")))
# rdd1.collect()

##persit and cache
# df1=df.cache()
# df2=df.select("fname","lname","location").persist(StorageLevel.DISK_ONLY)
# df2.display()

##arraytype columns
# df1=df.select("fname","lname","serials")
# df2=df.select("fname","lname","location",explode("serials"))
# df3=df.select("fname","lname",split("location","b"))
# df4=df.select("fname","lname",array("fname","lname").alias("full name"))
# df5=df4.select("fname","lname",array_contains("full name","vi"))
# df5.display()

##Maptype column
# df1=df.select("stats")
# df2=df.select("stats.weight")
# rdd3=df.rdd.map(lambda x: (x["fname"],x["lname"],x.stats["weight"]))
# df3=rdd3.toDF(["firstname","lastname","properties"])
# df4=df.select("fname","lname","stats.weight")
# df5=df.select(col("fname"),col("lname"),explode("stats"))
# df6=df.select(col("fname"),col("lname"),map_keys("stats"))
# df7=df.select(explode(map_keys("stats"))).distinct()
# df8=df7.rdd.map(lambda x:x[0])
# df9=df.select(col("fname"),map_values("stats"))
# df8.collect()

##flatten nested array column
# df1=df.select("fname","lname",flatten(flatten("serials")))
# df1.display()

##flatten nested struct column
There will be function to use

##explode array and map columns in pyspark


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

data=[(1,"vinay","boddu","maths","hyderabad",{"age":26,"weight":60},[1,2,3],(1,"better")),
      (2,"divya","boddu","social","banglore",{"age":25,"weight":65},[3,4,5],(2,"best")),
      (3,"viru","sehwag","maths","hyderabad",{"age":26,"weight":70},[5,6,7],(3,"quick"))]
schema=["sno","fname","lname","favsub","location","stats","serials","essentials"]
schemaa=StructType([
  StructField("sno",IntegerType(),True),
  StructField("fname",StringType(),True),
  StructField("lname",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("location",StringType(),True),
  StructField("stats",MapType(StringType(),IntegerType()),True),
  StructField("serials",ArrayType(IntegerType()),True),
  StructField("essentials",StructType([
    StructField("snoo",IntegerType(),True),
    StructField("qualities",StringType(),True)
  ]),True)
])

df=spark.createDataFrame(data=data,schema=schemaa)
df.display()
# rdd=spark.sparkContext.parallelize(data)
# df1=rdd.toDF(schema)
# df2=rdd.toDF(schemaa)
# df3=spark.createDataFrame(rdd).toDF(*schema)
# df4=spark.createDataFrame(data).toDF(*schema)
# df4.display()

data1=[(1,"vinay","boddu","science","hyderabad",{"age":26,"weight":60},[1,2,3],(1,"better")),
      (4,"raju","bhai","social","chennai",{"age":25,"weight":65},[3,4,5],(2,"best"))]
schemaa1=StructType([
  StructField("sno",IntegerType(),True),
  StructField("firstname",StringType(),True),
  StructField("lastname",StringType(),True),
  StructField("favsubject",StringType(),True),
  StructField("location",StringType(),True),
  StructField("stats",MapType(StringType(),IntegerType()),True),
  StructField("serials",ArrayType(IntegerType()),True),
  StructField("essentials",StructType([
    StructField("snoo",IntegerType(),True),
    StructField("qualities",StringType(),True)
  ]),True)
])
dff=spark.createDataFrame(data=data1,schema=schemaa1)
dff.display()

# COMMAND ----------

##select
# df1=df.select("sno","fname","lname")
# df2=df.select(df["sno"],df["fname"])
# df3=df.select(df.sno,df.fname, df.lname)
# df4=df.select(col("sno"),col("fname"),col("lname"))
# df5=df.select("*")
# df6=df.select(df.columns)
# df7=df.select([col for col in df.columns])
# df8=df.select(*schema)
# df9=df.select("essentials.*")
# df10=df.select(df.colRegex("`^.*s.*`"))
# df10.display()

##add and update columns using withColumn
# df1=df.withColumn("fname",upper(col("fname")))
# df2=df.withColumn("sno",col("sno")*10)
# df3=df.withColumn("sno",col("sno").cast("String"))
# df4=df.withColumn("newcol",lit("None"))
# df5=df.withColumn("newcol",concat_ws(" ","fname","lname"))
# df6=df.withColumn("newcol",when(col("sno")==1,"super")
#                           .otherwise("best"))
# df6.display()

##rename nested columns
# df1=df.withColumnRenamed("sno","newsno")
# schemaaa=StructType({
#   StructField("newsnoo",IntegerType(),True),
#   StructField("newqualities",StringType(),True)
# })
# df2=df.select(col("fname"),col("lname"),col("essentials").cast(schemaaa))
# df2.display()

##drop
# df1=df.drop("sno","fname","lname")
# lis=["sno","fname","lname"]
# df2=df.drop(*lis)
# df2.display()

##where or filter
# df1=df.filter(col("sno")==1)
# df2=df.filter("sno=2")
# df3=df.filter((col("sno")==3)&(col("fname")=="viru"))
# df4=df.filter(col("fname").startswith("vi"))
# df5=df.filter(col("fname").contains("y"))
# df6=df.filter(col("fname").isin("vinay","divya"))
# lis=["divya","viru"]
# df7=df.filter(col("fname").isin(lis))
# df8=df.filter(col("fname").like("vi%"))
# df8.display()

##when or otherwise
# df1=df.withColumn("col",when(col("sno")==1,"super")
#                        .otherwise("best"))
# df2=df.select(col("fname"),col("lname"),when(col("sno")==1,"best").otherwise("super").alias("newcol"))
# df2.display()

##distinct or dropDuplicates
# df1=df.select("lname").distinct()
# df2=df.select("lname","location").dropDuplicates(subset=["location"])
# df2.display()

##pivot table
# df1=df.groupby("location").pivot("lname").agg(count("*"))
# lis=["boddu","raj","gaami"]
# df2=df.groupby("location","fname").pivot("lname",lis).agg(count("*"),sum("lname"))
# df2.display()

##datatypes
##ShortType(), IntegerType(), LongType(), FloatType(), DoubleType(), DecimalType(), StringType(), DateType(), TimestampType(), StructType(), MapType(), ArrayType()

##groupby
# df1=df.groupby("location").agg(count("*"))
# df2=df.select("sno","location").groupby("location").agg(count("*"),sum("sno"),avg("sno"),max("sno"),min("sno"))
# df2.display()

##sort
# df1=df.sort(col("location").desc())
# df2=df.sort(col("location").desc(),col("lname").desc())
# df2.display()

##jointype
##inner, leftouter, rightouter, fullouter, leftanti, leftsemi
# df1=df.join(dff,df.fname==dff.firstname)
# df2=df.join(dff,["sno"],"leftouter").select(df["sno"],df["fname"],dff["firstname"],df["lname"],df["location"])
# df2.display()

##union and unionall
# df1=df.select(df.columns[0:5]).union(dff.select(dff.columns[0:5]))
# df2=df.select(df.columns[0:5]).unionByName(dff.select(dff.columns[0:5]),True)
# df2.display()

##map() in pyspark
# rdd1=df.rdd.map(lambda x: (x["fname"].upper(),x["lname"][0:3].lower()+x["lname"][3:].upper()))
# df1=rdd1.toDF(["fname","lastname"])
# def function(x):
#   firstname=x.fname.upper()
#   lastname=x.lname.lower()
#   return firstname, lastname
# rdd2=df.rdd.map(lambda x: function(x))
# df2=rdd2.toDF(["firstname","lastname"])
# df2.display()

##mapPartitions() in pyspark
rdd1=df.rdd.mapPartitions(lambda x: ((y["fname"].upper(),y["lname"][0:3].lower()+y["lname"][3:].upper())for y in x))
df1=rdd1.toDF(["firstname","lastname"])

def f(par):
  for el in par:
    firstname=el.fname
    lastname=el.lname[0:3].upper()+el.lname[3:].lower()
    yield firstname, lastname
rdd2=df.rdd.mapPartitions(f)
df2=rdd2.toDF(["fname","lname"])

# def f(par):
#   updatedata=[]
#   for i in par:
#     firstname=i.fname
#     lastname=i.lname[0:3].upper()+i.lname[3:].lower()
#     updatedata.append([firstname, lastname])
#   return iter(updatedata)
# rdd3=df.rdd.mapPartitions(f)
# df3=rdd3.toDF(["fname","lname"])
# df3.display()

##Persist and Cache
##MEMORY_ONLY, MEMORY_AND_DISK and DISK_ONLY
df1=df.select("sno","fname","lname").cache()
df2=df.select("sno","fname","lname","location").persist(StorageLevel.MEMORY_ONLY)
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##New practice

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql import Window,WindowSpec

# COMMAND ----------

data=[(1,"vinay","boddu","maths","hyderabad",[1,2,3],{"age":60,"weight":70},(1,"super")),
      (2,"divya","boddu","social","banglore",[3,4,5],{"age":27,"weight":65},(2,"better")),
      (3,"viru","sehwag","maths","hyderabad",[3,5,6,7],{"age":25,"weight":75},(3,"best"))]
schema=["sno","fname","lname","favsub","location","serials","details","qualities"]
schemaa=StructType([
  StructField("sno",IntegerType(),True),
  StructField("fname",StringType(),True),
  StructField("lname",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("location",StringType(),True),
  StructField("serials",ArrayType(IntegerType()),True),
  StructField("details",MapType(StringType(),IntegerType()),True),
  StructField("qualities",StructType([
    StructField("snoo",IntegerType(),True),
    StructField("behaviour",StringType(),True)
  ]),True)
])

df=spark.createDataFrame(data=data,schema=schemaa)
df.display()
# rdd=spark.sparkContext.parallelize(data)
# df1=rdd.toDF(schema)
# df2=rdd.toDF(schemaa)
# df3=spark.createDataFrame(data).toDF(*schema)
# df4=spark.createDataFrame(rdd).toDF(*schema)

data1=[(1,"vinay","boddu","science","hyderabad",[[1,2,3]],{"age":26,"weight":71},(((1,"better")))),
      (4,"virender","alluri","telugu","banglore",[[6,7,8]],{"age":27,"weight":65},(((3,"best"))))]
schemaa1 = StructType([
    StructField("sno", IntegerType(), True),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("favsubject", StringType(), True),
    StructField("location", StringType(), True),
    StructField("serials", ArrayType(ArrayType(IntegerType())), True),
    StructField("details", MapType(StringType(), IntegerType()), True),
    StructField("qualities",StructType([
      StructField("snoo",IntegerType(),True),
      StructField("behaviour",StringType(),True)
    ]),True)
])
dff=spark.createDataFrame(data=data1,schema=schemaa1)
dff.display()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,ShortType,StringType,ArrayType,MapType,FloatType,DecimalType,DoubleType,LongType
from pyspark.sql.functions import *

# COMMAND ----------

data=[(1,"vinay","boddu","hyderabad","maths",[1,2,3],{"age":21,"weight":60},(1,"aaron")),
      (2,"divya","boddu","chennai","science",[2,3,4],{"age":25,"weight":65},(2,"sydney")),
      (3,"viru","sehwag","hyderabad","maths",[3,4,5],{"age":26,"weight":67},(3,"rob"))]
schema=StructType([
  StructField("sno",ShortType(),True),
  StructField("fname",StringType(),True),
  StructField("lname",StringType(),True),
  StructField("location",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("snoo",ArrayType(IntegerType()),True),
  StructField("details",MapType(StringType(),IntegerType()),True),
  StructField("subject",StructType([
    StructField("subid",IntegerType(),True),
    StructField("fullname",StringType(),True)
  ]))
])
schemaa=["sno","fname","lname","location","favsub","snoo","details","subject"]
df=spark.createDataFrame(data=data,schema=schema)
df.display()
# rdd=spark.sparkContext.parallelize(data)
# df1=rdd.toDF(schemaa)
# df2=spark.createDataFrame(data).toDF(*schemaa)

data1=[(1,"vinay","boddu","hyderabad","maths",[1,3,4],{"age":25,"weight":65},(1,"aaron")),
      (1,"divya","boddu","chennai","science",[2,4,5],{"age":30,"weight":70},(2,"sydney"))]
schemaa1=["sno","fname","lname","location","favsub","snoo","details","subject"]
dff=spark.createDataFrame(data=data1,schema=schemaa1)
dff.display()

# COMMAND ----------

##select
# df1=df.select("sno","fname","location")
# df2=df.select(df.sno,df.fname,df.lname)
# df3=df.select(df["sno"],df["fname"],df["lname"])
# df4=df.select(col("fname"),col("lname"),col("location"))
# df5=df.select("*")
# df6=df.select(df.columns)
# df7=df.select(df.columns[0:6])
# df8=df.select([col for col in df.columns])
# df9=df.select(*schemaa)
# df10=df.select("subject.subid")
# df11=df.select(df.colRegex("`^l.*`"))
# df11.display()

##add and update columns using with column
# df1=df.withColumn("newcol",lit("hungry"))
# df2=df.withColumn("sno",col("sno")*10)
# df3=df.withColumn("sno",col("sno").cast("Integer"))
# df4=df.withColumn("fname",concat_ws(" ",col("fname"),col("lname")))
# df5=df.withColumn("newcol",when(col("fname")=="vinay","super")
#                           .when(col("fname")=="divya","underestimate")
#                           .otherwise("superbowl"))
# df5.display()


##rename nested columns
# df1=df.withColumnRenamed("sno","newsno") \
#       .withColumnRenamed("fname","firstname")
# newschemaa=StructType([
#   StructField("fullsubjectid",IntegerType(),True),
#   StructField("firstname",StringType(),True)
# ])
# df2=df.select("sno","fname","lname",col("subject").cast(newschemaa))
# df2.display()

##drop column
# df1=df.drop("sno","fname","lname","location")
# df2=df.drop("favsub","location","snoo")
# lis=["sno","fname","lname"]
# df3=df.drop(*lis)
# lis2=["location","lname","favsub"]
# df4=df.drop(*lis2)
# df4.display()

##filter or where
# df1=df.filter(col("fname")=="vinay")
# df2=df.filter((col("fname")=="vinay")&(col("favsub")=="maths"))
# df3=df.filter(col("fname").isin("vinay","divya"))
# lis=["vinay","divya"]
# df4=df.filter(col("fname").isin(lis))
# df5=df.filter(col("fname").like("%i_%"))
# df6=df.filter(col("fname").startswith("v"))
# df7=df.filter(col("fname").contains("i"))
# df8=df.filter("""
#               fname='vinay' and 
#               lname='boddu' and 
#               sno=1""")
# df8.display()

##when or otherwise
# df1=df.withColumn("newcol",when(col("fname").isin("vinay","divya"),"super")
#                           .otherwise("else"))
# df2=df.select("sno","fname","lname",when(col("fname").like("vi%"),"loundpana")
#                                    .otherwise("jinthana").alias("newcol"))
# df2.display()


##distinct or dropDuplicates
# df1=df.select("location","fname").distinct()
# df2=df.select("location","fname").dropDuplicates(subset=["location"])
# df2.display()


##pivot dataframe
# df1=df.groupby("location").pivot("fname").count()
# df2=df.groupby("location","lname").pivot("fname").agg(count("*"),sum("sno"))
# lis=["vinay","divya","sirish"]
# df3=df.groupby("location").pivot("fname",lis).count()
# df3.display()

##Datatypes
##ShortType(), IntegerType(), LongType(), StringType(), FloatType(), DoubleType(), DecimalType(), DateType(), TimestampType(), ArrayType(), MapType(), StructType()

##Groupby
# df1=df.groupby("location").count()
# df2=df.groupby("location").agg(count("*"))
# df3=df.groupby("location").agg(count("*").alias("total")).filter((col("total")>1)&(col("location")=="hyderabad"))
# df4=df.groupby("location","lname").agg(count("*"),sum("sno"),avg("sno"),min("sno"),max("sno"))
# df4.display()


##sort or orderby
# df1=df.select(df.columns[0:5]).sort(col("location").asc(),col("lname").asc())
# df1.display()

##jointypes
##inner, outer, left, right, leftanti, leftsemi and selfjoin
# df1=df.join(dff,["sno"],"inner")
# df2=df.join(dff,df.sno==dff.sno,"leftsemi").select("sno","fname","lname","location","details")
# df3=df.join(dff,df.sno==dff.sno,"rightouter")
# df3.display()

##union and unionall
# df1=df.select("fname","lname","location").union(dff.select("fname","lname","location")).distinct()
# df2=df.select("fname","lname","location").unionByName(dff.select("fname","lname"),True)
# df2.display()

##map() in pyspark
# rdd=df.rdd.map(lambda x:(x["sno"],x["fname"].upper()[0:2],x["lname"]))
# df1=rdd.toDF(["sno","fname","lname"])

# def func(x):
#   firstname=x.fname
#   lastname=x.lname.upper()
#   return firstname, lastname
# rdd1=df.rdd.map(lambda x: func(x))
# df2=rdd1.toDF(["firstname","lastname"])

# rdd3=df.rdd.map(lambda x: (x["fname"],x["lname"],x["location"]))
# df3=rdd3.toDF()

# def function(x):
#   loc=x.location.upper()
#   return loc,
# rdd4=df.rdd.map(lambda x: function(x))
# df4=rdd4.toDF()
# df4.display()

##mapPartitions() in pyspark
# rdd1=df.rdd.mapPartitions(lambda x: ((y["sno"],y["fname"],y["lname"],y["location"]) for y in x))
# df1=rdd1.toDF()

# rdd2=df.rdd.mapPartitions(lambda y: ((x["fname"],)for x in y))
# df2=rdd2.toDF()

# def func(x):
#   for y in x:
#     firstname=y.fname.upper()
#     yield[firstname]
# rdd3=df.rdd.mapPartitions(func)
# df3=rdd3.toDF()

# def reform(par):
#   for i in par:
#     firstname=i.fname
#     lastname=i.lname
#     yield["firstname","lastname"]
# rdd4=df.rdd.mapPartitions(reform)
# df4=rdd4.toDF()

# def reform(i):
#   lis=[]
#   for y in i:
#     firstname=y.fname.upper()
#     lastname=y.lname
#     lis.append([firstname, lastname])
#   return iter(lis)
# rdd5=df.rdd.mapPartitions(reform)
# df5=rdd5.toDF()

# def reform(y):
#   lis=[]
#   for i in y:
#     firstname=i.fname
#     location=i.location.upper()
#     lis.append([firstname, location])
#   return iter(lis)
# rdd6=df.rdd.mapPartitions(reform)
# df6=rdd6.toDF()
# df6.display()

##flatmap() in pyspark


# COMMAND ----------

# MAGIC %md
# MAGIC New Practice

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

data=[(1,"vinay","boddu","hyderabad","maths",[1,2,3,4],{"age":26,"weight":60},(1,"vinay")),
      (2,"divya","boddu","chennai","social",[3,4,5],{"age":24,"weight":55},(2,"divya")),
      (3,"viru","sehwag","hyderabad","maths",[5,6,7,8],{"age":29,"weight":73},(3,"viru"))]
schema=StructType([
  StructField("sno",IntegerType(),True),
  StructField("fname",StringType(),True),
  StructField("lname",StringType(),True),
  StructField("location",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("snoo",ArrayType(IntegerType()),True),
  StructField("details",MapType(StringType(),IntegerType()),True),
  StructField("info",StructType([
    StructField("sno",IntegerType(),True),
    StructField("name",StringType(),True)
  ]),True)
])
schemaa=["sno","fname","lname","location","favsub","snoo","details","info"]
df=spark.createDataFrame(data=data,schema=schema)
df.display()
# rdd=spark.sparkContext.parallelize(data)
# df1=rdd.toDF()
# df2=rdd.toDF(schemaa)
# df3=rdd.toDF(schema)
# df4=spark.createDataFrame(data).toDF(*schemaa)
# df5=spark.createDataFrame(rdd).toDF(*schemaa)
# df6=spark.createDataFrame(data,schemaa)

data1=[(1,"vinay","brijraj","lucknow","science",[[1,2,3,4]],{"age":26,"weight":60},(1,"bringraj")),
      (2,"divya","boddu","chennai","social",[[3,4,5]],{"age":24,"weight":55},(2,"divya"))]
schemaa1=["sno","fname","lname","location","favsub","snoo","details","info"]

dff=spark.createDataFrame(data=data1,schema=schemaa1)
dff.display()

# COMMAND ----------

##select
# df1=df.select("sno","fname","lname")
# df2=df.select(df["sno"],df["fname"],df["lname"],df["location"])
# df3=df.select(df.sno, df.fname, df.lname)
# df4=df.select(col("sno"),col("fname"))
# df5=df.select("*")
# df6=df.select(df.columns)
# df7=df.select([col for col in df.columns])
# df8=df.select(*schemaa)
# df9=df.select(df.colRegex("`^l.*`"))
# df10=df.select("details")
# df10.display()

##add and update columns
# df1=df.withColumn("newcol",lit("literal"))
# df2=df.withColumn("sno",col("sno").cast("String"))
# df3=df.withColumn("sno",col("sno")*10)
# df4=df.withColumn("fname",concat_ws(" ","fname","lname"))
# df5=df.withColumn("newcol",when(col("fname")=="vinay","super")
#                           .when(col("fname")=="divya","super super")
#                           .otherwise("adhi"))
# df5.display()

##rename nested columns
# df1=df.withColumnRenamed("sno","newsno")
# newschema=StructType([
#   StructField("newno",IntegerType(),True),
#   StructField("newname",StringType(),True)
# ])
# df2=df.select("sno",col("fname"),col("lname"),col("info").cast(newschema))
# df2.display()

##drop column
# df1=df.drop("sno","fname","lname")
# lis=["sno","fname","lname","location"]
# df2=df.drop(*lis)
# df2.display()

##where or filter
# df1=df.filter(col("fname")=="vinay")
# df2=df.filter((col("fname")=="vinay")&(col("location")=="hyderabad"))
# df3=df.filter(col("fname").isin("vinay","divya"))
# lis=["vinay","divya"]
# df4=df.filter(col("fname").isin(lis))
# df5=df.filter(col("fname").like("vi%"))
# df6=df.filter(col("fname").startswith("vi"))
# df7=df.filter(col("fname").contains("y"))
# df8=df.filter("fname='vinay' and lname='boddu'")
# df8.display()

##when or otherwise
# df1=df.withColumn("newcol",when(col("fname")=="vinay","super")
#                           .when(col("fname")=="divya","super super")
#                           .otherwise("ok"))
# df2=df.select("sno","fname","lname",when(col("lname")=="raj",None)
#                                    .when(col("lname").isin("boddu"),"super")
#                                    .otherwise(None).alias("newcol"))
# df2.display()

##collect in dataframe and rdd
# df1=df.select("*")
# df1.collect()
# rdd=spark.sparkContext.parallelize(data)
# rdd.collect()

##distinct
# df1=df.select("location","favsub").distinct()
# df2=df.select("location","favsub").drop_duplicates(["location"])
# df2.display()

##pivot a dataframe
# df1=df.groupby("location").pivot("fname").count()
# df2=df.groupby("location","lname").pivot("fname").agg(count("*"),sum("sno"),max("sno"))
# lis=["vinay","divya","raj"]
# df3=df.groupby("location","lname").pivot("fname",lis).agg(sum("sno"),max("sno"),min("sno"))
# df3.display()

##DataTypes
##ShortType(), IntegerType(), LongType(), StringType(), FloatType(), DoubleType(), DecimalType(), DateType(), TimestampType(), ArrayType(), MapType(), StructType()

##groupby
# df1=df.groupby("location").count()
# df2=df.groupby("location").agg(count("*"),sum("sno"),avg("sno"),max("sno"))
# df2.display()

##sort a dataframe
# df1=df.select(df.columns[1:5]).sort(col("location").asc())
# df2=df.select("*").sort(col("location").asc(), col("lname").asc())
# df2.display()

##Join Types
##inner, left, right, fullouter, leftanti, leftsemi
# df1=df.join(dff,df.sno==dff.sno,"inner")
# df2=df.join(dff,["sno"],"left").select(df["sno"],df["fname"],df["lname"],df["location"],dff["fname"],dff["lname"],dff["location"])
# df2.display()

##union and unionall
# df1=df.select("*").union(dff.select("*"))
# df2=df.select("location","lname").union(dff.select("location","lname"))
# df3=df.select("location","lname").unionByName(dff.select("location","fname","lname"),True)
# df3.display()

##map() in pyspark
# rdd1=df.rdd.map(lambda x: (x["fname"],x["lname"]+" "+"upper",x["location"]))
# df1=rdd1.toDF(["firstname","lastname","location"])

# rdd2=df.rdd.map(lambda y: (y["fname"]+"upper",))
# df2=rdd2.toDF()

# rdd1=df.rdd.map(lambda x: (*x,))
# df1=rdd1.toDF()

# def func(x):
#   full=x.fname
#   half=x.lname.upper()
#   return full, half
# rdd3=df.rdd.map(lambda x: func(x))
# df3=rdd3.toDF()

# def reform(y):
#   firstname=y.fname
#   lastname=y.lname
#   return firstname, lastname
# rdd4=df.rdd.map(reform)
# df4=rdd4.toDF()
# df4.display()

##mapPartitions() in pyspark
# rdd1=df.rdd.mapPartitions(lambda x: ((y["fname"],y["lname"],y["details"])for y in x))
# df1=rdd1.toDF()

# rdd2=df.rdd.mapPartitions(lambda y: ((x["fname"]+"upper",x["lname"].upper(),x["location"])for x in y))
# df2=rdd2.toDF()

# def reform(x):
#   lis=[]
#   for y in x:
#     firstname=y.fname.upper()
#     lastname=y.lname+" "+"upper"
#     lis.append([firstname, lastname])
#   return iter(lis)
# rdd3=df.rdd.mapPartitions(reform)
# df3=rdd3.toDF()

# def reform(x):
#   for y in x:
#     firstname=y.fname
#     lastname=y.lname
#     yield [firstname, lastname]
# rdd4=df.rdd.mapPartitions(reform)
# df4=rdd4.toDF()
# df4.display()

##flatMap() in pyspark
##it flattens the rdd or dataframe (array or maptype)
# rdd1=df.rdd.flatMap(lambda x: (x["fname"].split("n")))
# rdd1.collect()

##foreach() in pyspark
# def func(df):
#   print(df)
# df.foreach(func)

##foreachPartitions() in pyspark


# COMMAND ----------

##Persist and Cache()
##cahce is MEMORY_ONLY
# df1=df.cache()
##Persist is storage level and it contains MEMORY_ONLY, MEMORY_AND_DISK and DISK_ONLY.
##default is Memory and disk.
# df1=df.persist()
# df2=df.persist(StorageLevel.MEMORY_ONLY)
# df2.display()

##User defined function in pyspark, use UDF when it is most necessary. First create python function, then use spark sql to convert it into UDF and then use inside select or in withColumn
# def func(x):
#   fullname=x[0].upper()+x[1:]
#   return fullname
# convercase=udf(lambda x: func(x),StringType())
# df1=df.select("sno","fname",convercase("fname"),"lname","location")

# def full(x):
#   fullname=x[0].upper()+x[1:3].lower()+x[3:5].upper()
#   return fullname
# supercase=udf(lambda x: full(x),StringType())
# df2=df.withColumn("newcol",supercase("fname")).select("sno","fname",col("newcol").alias("newfname"),"lname","location")
# df2.display()

##arraytype column
#explode, split, array, array_contains
# df1=df.select("sno",col("fname"),col("lname"),explode("snoo").alias("newcol"))
# df2=df.withColumn("newcol",explode(col("snoo")))
# df3=df.select("sno",split("fname","i"))
# df4=df.select("sno","fname","lname",array("fname","lname"))
# df5=df.withColumn("newcol",array_contains(df.snoo,3))
# df4.display()

##Maptype column
##explode, map_keys("details"), map_values("details")
# rdd1=df.rdd.map(lambda x: (x.details["age"],))
# df1=rdd1.toDF()
# df2=df.withColumn("newcol",df.details.getItem("age"))
# df3=df.select("sno","fname","lname",df.details.getItem("weight"))
# df4=df.select("sno","fname","lname",df.details["age"])
# df5=df.select("sno","details.age")
# df6=df.select("sno","fname","lname",explode("details"))
# df7=df.select("sno","fname","lname",map_keys("details"))
# df8=df.select("sno","fname","lname","location",map_values("details"))
# df9=df.select("sno","fname","details")
# df9.display()

##flatten nested struct column
##there will be one function for this

##flatten nested arraycolumn
# df1=dff.select("sno","fname","lname",flatten("snoo"))
# df1.display()

##explode array and map columns


# COMMAND ----------

# MAGIC %md
# MAGIC New pyspark practice

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.window import Window

# COMMAND ----------

data=[(1,"vinay","boddu","hyderabad","maths",[1,None,3,],{"age":27,"weight":70},(1,"vinay")),
      (2,"divya","boddu","chennai","social",None,{"age":26,"weight":70},(2,"divya")),
      (3,"viru","sehwag","hyderabad","maths",[],{"age":25,"weight":68},(3,"viru"))]
schema=StructType([
  StructField("sno",IntegerType(),True),
  StructField("fname",StringType(),True),
  StructField("lname",StringType(),True),
  StructField("location",StringType(),True),
  StructField("favsub",StringType(),True),
  StructField("snoo",ArrayType(IntegerType()),True),
  StructField("details",MapType(StringType(),IntegerType()),True),
  StructField("info",StructType([
    StructField("sno",IntegerType(),True),
    StructField("name",StringType(),True)
  ]),True)
])
schemaa=["sno","fname","lname","location","favsub","snoo","details","info"]

df=spark.createDataFrame(data=data,schema=schema)
df.display()
# rdd=spark.sparkContext.parallelize(data)
# df1=rdd.toDF(schema) 
# df2=rdd.toDF(schemaa)
# df3=spark.createDataFrame(data).toDF(*schemaa)
# df4=spark.createDataFrame(rdd).toDF(*schemaa)

data1=[(1,"vinay","boddu","hyderabad","maths",[1,2,None,],{"age":None,"weight":80},(1,"vinay")),
      (2,"divya","boddu","chennai","social",None,{"age":26,"weight":70},(2,"divya"))]
schemaa1=["sno","fname","lname","location","favsub","snoo","details","info"]
dff=spark.createDataFrame(data=data1,schema=schemaa1)
dff.display()

# COMMAND ----------

##select in pyspark
# df1=df.select("sno","fname","lname")
# df2=df.select(df.sno, df.fname, df.lname)
# df3=df.select(df["sno"],df["fname"],df["lname"],df["location"])
# df4=df.select(col("fname"),col("lname"))
# df5=df.select("*")
# df6=df.select([col for col in df.columns])
# df7=df.select(df.columns)
# df8=df.select(*schemaa)
# df9=df.select(df.colRegex("`^l.*`"))
# df10=df.select(df.info.sno)
# df10.display()

##add and update column using withcolumn
# df1=df.withColumn("newcol",lit("super"))
# df2=df.withColumn("sno",col("sno")*10)
# df3=df.withColumn("newcol",concat_ws(" ","fname","lname"))
# df4=df.withColumn("newcol",when(col("fname")=="vinay","super")
#                           .when(col("fname")=="divya","double super"))
# df5=df.withColumn("sno",col("sno").cast(LongType()))
# df5.display()

##renamed nested column
# df1=df.withColumnRenamed("sno","newsno")
# newschema=StructType([
#   StructField("newsno",IntegerType(),True),
#   StructField("newname",StringType(),True)
# ])
# df2=df.select("sno","fname","lname",col("info").cast(newschema))
# df2.display()

##drop column
# df1=df.drop("sno","fname","lname")
# df2=df.drop(*df.columns[1:3])
# lis=["sno","fname","lname","location"]
# df3=df.drop(*lis)
# df3.display()

##where or filter
# df1=df.filter(col("fname")=="vinay")
# df2=df.filter((col("fname")=="vinay") &(col("lname")=="boddu"))
# df3=df.filter(col("fname").isin("vinay","divya"))
# lis=["vinay","viru"]
# df4=df.filter(col("fname").isin(lis))
# df5=df.filter(col("fname").startswith("vi"))
# df6=df.filter(col("fname").contains("v"))
# df7=df.filter("fname='vinay' and lname='boddu'")
# df7.display()

##when or otherwise
# df1=df.select("sno","fname","lname",when(((col("fname")=='vinay') & (col("lname")=='boddu')),"super")
#                                    .otherwise("double super").alias("newcol"))
# df2=df.withColumn("newcol",when(col("fname")=="vinay","super")
#                           .otherwise("double super"))
# df2.display()

##collect for dataframe and rdd
# df1=df.select(df.columns[1:5])
# rdd1=rdd
# rdd1.collect()

##distinct or dropDuplicates
# df1=df.select("lname","location").distinct()
# df2=df.select("fname","location").dropDuplicates(subset=["location"])
# lis=["location","lname"]
# df3=df.select(*lis).dropDuplicates(["location"])
# df3.display()

##pivot in dataframe
# df1=df.groupby("location").pivot("lname").count()
# df2=df.groupby("location").pivot("fname").agg(count("*"),sum("sno"),avg("sno"),min("sno"))
# lis=["vinay","shirish"]
# df3=df.groupby("location").pivot("fname",lis).agg(count("*"),sum("sno"))
# df3.display()

##datatypes
##ShortType(), IntegerType(), LongType(), StringType(), DecimalType(), FloatType(), DoubleType(), DateType(), TimestampType(), MapType(), ArrayType(), StructType()

##groupby
# df1=df.groupby("location").count()
# df2=df.groupby("location").agg(count("*"),sum("sno"),avg("sno"),min("sno"),max("sno"))
# df2.display()

##sort or order by in pyspark
# df1=df.select("sno","fname","lname","location","favsub").sort(col("location").asc())
# df2=df.select(df.columns[0:5]).sort(col("location").desc(),col("fname").desc())
# df2.display()

##join types
##inner, left join, right join, leftanti, leftsemi
# df1=df.join(dff,df.sno==dff.sno,"inner")
# df2=df.select("sno","fname","lname","location").join(dff.select("sno","fname","lname","location"),df.sno==dff.sno,"left")
# df3=df.join(dff,["sno"],"inner").select(df["sno"],df["fname"],df["lname"],dff["fname"],dff["lname"])
# df3.display()

##union and unionall
# df1=df.union(dff)
# df2=df.select("sno","fname","lname","location").union(dff.select("sno","fname","lname","location"))
# df3=df.select("sno","fname","location").unionByName(dff.select("sno","fname","lname"),True)
# df3.display()

##map() in pyspark
# rdd1=df.rdd.map(lambda x: (x["sno"],x["fname"].upper(),x["lname"].lower()))
# df1=rdd1.toDF()
# rdd2=df.rdd.map(lambda x: (x["sno"],x["fname"],x["lname"].lower()))
# df2=rdd2.toDF()
# rdd3=df.rdd.map(lambda x: (*x,))
# df3=rdd3.toDF()

# def func(x):
#   firstname=x.fname
#   lastname=x.lname
#   return firstname, lastname
# rdd4=df.rdd.map(lambda x: func(x))
# df4=rdd4.toDF()

# def func(y):
#   firstname=y.fname
#   return firstname,
# rdd5=df.rdd.map(func)
# df5=rdd5.toDF()
# df5.display()

##mapPartitions() in pypsark
# rdd1=df.rdd.mapPartitions(lambda x: ((y["sno"],y["fname"],y["lname"])for y in x))
# df1=rdd1.toDF()

# def reform(x):
#   for y in x:
#     firstname=y.fname.upper()
#     lastname=y.lname.lower()
#     yield [firstname, lastname]
# rdd2=df.rdd.mapPartitions(reform)
# df2=rdd2.toDF()

# def reform(par):
#   updateddata=[]
#   for ever in par:
#     firstname=ever.fname.upper()
#     lastname=ever.lname.upper()
#     updateddata.append([firstname, lastname])
#   return updateddata
# rdd3=df.rdd.mapPartitions(reform)
# df3=rdd3.toDF()
# df3.display()

##flatMap in pyspark
# rdd1=df.rdd.flatMap(lambda x: (x["fname"].split("i")))
# rdd1.collect()

# COMMAND ----------

##Persist and Cache() in pyspark
# df1=df.cache()
# df2=df.persist(StorageLevel.MEMORY_ONLY)
# df3=df.persist(StorageLevel.DISK_ONLY)
# df3.display()

##udf in pyspark
# def func(x):
#   return x.upper()
# varcol=udf(lambda x: func(x),StringType())
# df1=df.select("sno",varcol("fname"))

# def reform(x):
#   y=x[1:3].upper()+x[3:].lower()
#   return y
# newcol=udf(lambda x: reform(x), StringType())
# df2=df.select("sno",newcol("fname"))

# def newfunc(z):
#   return z[0:3].upper()+z[3:].lower()
# convertcase=udf(lambda z: newfunc(z),StringType())
# df3=df.withColumn("newcol",convertcase("fname"))
# df3.display()

##ArrayType() in pyspark
# df1=df.select("sno","fname","lname",explode("snoo"))
# df2=df.select("sno","fname","lname",split("fname","i"))
# df3=df.select("sno","fname","lname",array("fname","lname"))
# df4=df.select("sno","fname","lname",array_contains("snoo",2))
# df5=df.withColumn("newcol",explode("snoo"))
# df6=df.withColumn("newcol",split("fname","i"))
# df6.display()

##MapType() in pyspark
# df1=df.select("sno","fname","lname","details.age")
# df2=df.select("sno",df.details.age)
# df3=df.select("sno",df.details.getItem("age"))
# rdd4=df.rdd.map(lambda x: (x["sno"],x["fname"],x["lname"],x.details["age"]))
# df4=rdd4.toDF()
# df5=df.select("sno","fname","lname","sno",explode("details"))
# df6=df.withColumn("newcol",map_keys("details"))
# df7=df.withColumn("newcol2",map_values(df.details))
# df7.display()

##flatten nested struct column
##we have predefined function for this

##flatten nested array column
# df1=dff.select("sno","fname","lname",flatten("snoo"))
# df1.display()

##Explode array and map columns
##explode(), explode_outer(), posexplode() and posexplode_outer()
##explode will ignore null or empty values that our outside, if inside, then it will give same
# df1=df.select("sno","fname",explode("snoo"))
# df2=df.select("sno","fname","lname",explode_outer("snoo"))
# df3=df.select("sno","fname","lname",posexplode("snoo"))
# df4=df.select("sno","fname","lname",posexplode_outer("snoo"))
# df4.display()

##Sampling
##If you have larger dataset, you may need small sample to work with.
# df.sample(True, 0.09, 167)
##In the above, True means, we want different results with same sample, 0.09 is 9% and 167, if you repeat it, it will get same results.
# df1=df.sample(0.5,167)
# df2=df.sample(0.5,167)
# df3=df.sample(0.5,8765)
# df4=df.sample(True, 0.8,78)
# df4.display()

##Partitioning in pyspark
"""
Partitionby(), you can give one column or multiple columns, do not give column as partition which can create a lot of subdirectories, ex: partition on year and month but not date.
You can also use repartition() with partition, for ex: if we have 6 state, creating partitionby on state, will create 6 subdirectories, but when you use with repartition(2), it will create 2 part files for every state inside partition directory, total: 6 files.
You can also ignore data skew by using option(maxRecordsperFile=2), then it will create multiple part files and every file with only 2 records.
"""

##Functions in pyspark
##You can use sql functions using expr() API and calling them through a SQL expression string.
# df1=df.withColumn("newcol",expr("case when fname='vinay' then 'super' else 'normal' end"))

##String functions
##concat_ws,format_string,length, upper(col),lower(col),lpad(col, length, pad), rpad(col, length, pad), trin(col),rtrim
# df1=df.withColumn("newcol",concat_ws(" ","fname","lname"))
# df2=df.select("sno","fname",concat_ws(" ","fname","lname").alias("newname"))
# df3=df.select("sno","fname","lname",expr("concat(fname,' ',lname) "))
# df4=df.withColumn("newcol",format_string("%s %s","fname","lname"))
# df5=df.select("sno","fname","lname",length("fname"))
# df6=df.select("sno","fname",upper("fname"))
# df7=df.select("sno","fname","lname",lpad("fname",8,'r'))
# df8=df.withColumn("newcol",lpad("fname",6,'u'))
# df9=df.select("sno","fname",ltrim("lname"))
# df9.display()

##Date and Timestamp functions
##current_date(), current_timestamp(),date_format(col, format),year(col),month(col),dayofyear(col),dayofmonth(col),dayofweek(col), hour, minute(col),second(col), date_add(start, days),date_sub(start,days), add_months(start, days),to_timestamp(col),to_date(col)
# df1=df.select("sno",current_date())
# df2=df.withColumn("time",current_timestamp()).select("sno","fname","lname","time")
# df3=df.select("sno","fname",date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
# df4=df.select("sno","fname",dayofweek(current_date())) #dayofweek, dayofmonth, dayofyear, year, month
# df5=df.select("sno","fname","lname",second(current_timestamp())) ##hour(col),minute, second
# df6=df.select("sno",date_sub(current_date(),5)) ##date_add(start,days), date_sub(start,days)
# df7=df.select("sno","fname","lname",add_months(current_date(),2))
# df8=df.select("sno","fname",to_timestamp(current_date())) #to_timstamp(col), to_date(col)
# df8.display()

##array function in pyspark  (collection functions)
##array_contains(), slice(col, start, length),array_positions(col,index),element_at(col,extract),array_sort(col),array_remove(col,element), array_union(col1, col2),array_distinct(col),array_intersect(col1,col2),array_except(col1,col2),array_max(col),array_min(col),sort_array(col,asc or desc),reverse(col),
# df1=df.select("sno","fname","lname",array_contains("snoo",3))
# df2=df.select("sno","fname","lname",slice("snoo",2,4))
# df3=df.select("sno",array_position("snoo",2))
# df4=df.select("sno","fname","lname",element_at("snoo",1))
# df5=df.select("sno","fname","lname",array_sort("snoo"))
# df6=df.select("sno","fname",array_remove("snoo",3))
# df7=df.withColumn("newcol",array_distinct(array_sort("snoo")))
# df7.display()

##Map functions in pyspark
##Same as Map column functions in pyspark

##Sort Functions in pyspark
##same as sort in pyspark

##Aggregate functions
##We already looked into this

##Math functions
##abs(col),sqrt(col),ceil(col),factorial(col),floor(col),

##Window functions in pyspark
##Ranking functions: row_number(), rank(), dense_rank(), percent_rank(), ntile()
##analytic functions: cume_dist(), lead(), lag()
##aggregate functions: sum(), first(), last(), count(), avg(), min(), max()
# windowspec=Window.partitionBy("location").orderBy("fname")
# df1=df.select("sno","fname","lname","location",row_number().over(windowspec).alias("rownum"))
# windowspec=Window.partitionBy("location").orderBy("fname")
# df2=df.withColumn("newcol",dense_rank().over(windowspec))
# df2.display()

##analytic function
# windowspec=Window.partitionBy("location").orderBy("fname")
# df1=df.select("sno","fname","lname","location",lag("location",1,"super").over(windowspec))
# df1.display()

##aggregate functions
# windowspecagg=Window.partitionBy("location")
# df1=df.select("sno","fname","lname","location",count("*").over(windowspecagg),sum("sno").over(windowspecagg),avg("sno").over(windowspecagg))
# df1.display()

# COMMAND ----------


