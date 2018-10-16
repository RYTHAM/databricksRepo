// Databricks notebook source
// var applicationId="901c901f-6dee-4a76-b493-f487a4d25972"
// var applicationKey="8rGK2L0Ql4MSzpXupqfoQRcbJH+zNmulQfe6200RAEc="
// var tenantId="72f988bf-86f1-41af-91ab-2d7cd011db47"
// var source_url="adl://etltestrytham.azuredatalakestore.net/input.tsv"
// var sink_url="adl://etltestrytham.azuredatalakestore.net/output.tsv"

 spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
 spark.conf.set("dfs.adls.oauth2.client.id", "901c901f-6dee-4a76-b493-f487a4d25972")
 spark.conf.set("dfs.adls.oauth2.credential", "8rGK2L0Ql4MSzpXupqfoQRcbJH+zNmulQfe6200RAEc=")
 spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------


val df = spark.read
              .format("com.databricks.spark.csv")
              .option("delimiter","\t")
              .option("header", "true")
              .load("adl://etltestrytham.azuredatalakestore.net/input.tsv")
df.show()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

//df.registerTempTable("df")

// COMMAND ----------

--%sql show tables

--%sql select * from df

--%sql select hash("rytham",13)




// COMMAND ----------

import  org.apache.spark.sql.functions.{col,hash}
val newdf=df.withColumn("hash",hash(df.columns.map(col):_*))

// COMMAND ----------

newdf.show()
df.show()

// COMMAND ----------

//import  org.apache.spark.sql.functions.udf
//import scala.util.hashing.{MurmurHash3 =>MH3}

def getHash()= udf(
(id: String, name: String)=> MH3.stringHash((id+name))
)
val newdf2=df.withColumn("hash",getHash()($"id",$"name"))

//val data="check"
//val res=MH3.stringHash(data,MH3.stringSeed)
//val newdf2=df.withColumn("hash",res.toString())

df.show();

newdf2.show();

// COMMAND ----------

// newdf2.write
//       .format("com.databricks.spark.csv")
//       .option("delimiter","\t")  
//       .option("header", "true")
//       .save("adl://etltestrytham.azuredatalakestore.net/output.tsv")
newdf2.repartition(1).write.format("csv").option("header","true").save("adl://etltestrytham.azuredatalakestore.net/output.tsv")

//.coalesce(1).write()  if it is to be stored without partitioning


// COMMAND ----------

