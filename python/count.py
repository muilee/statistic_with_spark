from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://10.120.37.108/project.reactions") \
    .config("spark.mongodb.output.uri", "mongodb://10.120.37.108/fb_cleaned.test2018") \
    .getOrCreate()

def function(x):
	x = iter(x)
	temp = []
	for _ in range(40):
		temp.append(next(x))
	return temp


reactions_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.120.37.108/project.reactions").load()
reactions_rdd = reactions_df.rdd
reactions_temp1 = reactions_rdd.map(lambda x: (( x.post_id.split("_")[0] , x.person_id, x.name) , 1))
reactions_temp2 = reactions_temp1.reduceByKey(lambda x,y: x+y)

comments_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.120.37.108/project.comments").load()
comments_rdd = comments_df.rdd
comments_temp1 = comments_rdd.map(lambda x: (( x.politician_id , x.person_id, x.name) , (x.score, 1, 1) )  if x.score is not None else (( x.politician_id , x.person_id, x.name) , (0, 0, 1) ))
comments_temp2 = comments_temp1.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])  )
comments_temp3 = comments_temp2.map(lambda x: (x[0] , (x[1][0] / x[1][1], x[1][2]) ) if x[1][1] != 0 else (x[0] , (None, x[1][2]) ) )

person_rdd = reactions_temp2.fullOuterJoin(comments_temp3)

def transform(x):
	if x[1][0] is None:
		return (  (x[0][0], x[1][1][1]) ,  (x[0][1], x[0][2], x[1][0], x[1][1][0], x[1][1][1])  )
	elif x[1][1] is None:
		return (  (x[0][0], x[1][0]) ,  (x[0][1], x[0][2], x[1][0], x[1][1], x[1][1])  )
	else:
		return (  (x[0][0], x[1][0] + x[1][1][1]) ,  (x[0][1], x[0][2], x[1][0], x[1][1][0], x[1][1][1])  )

person_temp1 = person_rdd.map(transform)

person_temp2 = person_temp1.sortByKey(False)

person_temp3 = person_temp2.map(lambda x: (x[0][0] , (x[0][1], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4])    ) )


person_temp4 = person_temp3.groupByKey()

person_temp5 = person_temp4.mapValues(function)

person_temp6 = person_temp5.flatMapValues(lambda x: x)

person_temp7 = person_temp6.map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))

person_temp8 = spark.createDataFrame(person_temp7, ["politician_id", "total", "person_id", "name", "reactions", "avg_score", "comments"])

person_temp8.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","fb_cleaned").option("collection", "test2018").save()