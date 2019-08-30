#export PYTHONIOENCODING=utf8 #export no terminal antes de executar
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkContext, SparkConf
import time

MYspark = SparkSession \
    .builder \
    .appName("Media_Pipeline_App") \
    .config("spark.mongodb.input.uri", "mongodb://10.7.40.SEU_IP/trabFinal.Ages_Essencial") \
    .config("spark.mongodb.output.uri", "mongodb://10.7.40.SEU_IP/trabFinal.Media_Spark_Mongo") \
    .getOrCreate()

#sqlContext = SQLContext(MYspark) -nao precisa

pipeline =[
        {"$group":{"_id":{"country_name":"$country_name","year":"$year"},
                "Peso": {"$sum":{"$multiply":["$population","$age"]}},
                "Population": {"$sum":"$population"}}},
        {"$project": {"_id":0,"Pais":"$_id.country_name","Ano":"$_id.year",
                "Media":{"$cond":[ {"$eq":["$Population",0]}, 0, {"$divide":["$Peso","$Population"]} ]}
        }}
        ]

#"quality": { $cond: [ { $eq: [ "$downvotes", 0 ] }, "N/A", {"$divide":["$upvotes", "$downvotes"]} ] }
#"Media":{"$cond":[ {"$eq":["$Population",0]}, "N/A", {"$divide":["$Peso","$Population"]} ]


#1 -Read e Aggregation no Pipeline
start_time = time.time()
print("\n"+"1 - Leitura e Pipeline"+"\n")

dfPipe = MYspark.read.format("com.mongodb.spark.sql.DefaultSource").option("pipeline", pipeline).load()

time1 = time.time()-start_time

#1 -Leitura sem Pipeline
print("\n"+"1-Leitura Simples"+"\n")
time11 = time.time()
new_df = MYspark.read.format("com.mongodb.spark.sql.DefaultSource").load()
time12 = time.time()-time11

#2 -Calcula da Media
print("\n"+"2 - Achando a media"+"\n")
time21 = time.time()

new_df = new_df.select("*",(new_df.age * new_df.population).alias("peso"))

exprs = {'population':'sum','Peso':'sum'}
new_df = new_df.groupBy(["country_name","year"])\
        .agg(exprs)\
        .withColumnRenamed('SUM(population)','population')\
        .withColumnRenamed('SUM(Peso)','peso')

new_df = new_df.select('*',new_df.peso/new_df.population).withColumnRenamed('(peso / population)','Media')
#new_df.printSchema() ##verificar colunas - afeta tempo
new_df = new_df.select('country_name','year','population','Media')\
        .withColumnRenamed('country_name','Pais').withColumnRenamed('year','Ano')\
        .withColumnRenamed('population','Populacao')

time22=time.time()-time21


time31 = time.time()
#3 -Escrita
print("\n"+"3 - Escrita no Mongo"+"\n")
new_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()

time32 = time.time() - time31

print( "\n"+"FINALIZADO ! ! ! ! "+"\n" )

new_df.show() #demora um pouco

with open("Results/Results_Spark_Mongo_MEDIA.txt", "a") as myfile:
        tempos = "\n"+"Leitura e Pipeline:"+str(time1)\
        +"\n"+"Leitura Simples do Mongo: "+str(time12)\
        +"\n"+"Calculo da Media com Spark: "+str(time22)\
        +"\n"+"Escrita Mongo:"+str(time32)
        
		print tempos
        myfile.write(tempos)

	