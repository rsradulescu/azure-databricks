// Databricks notebook source
// DBTITLE 1,Create spark csv - with header
// create df with header from json
import org.apache.spark.sql.functions._
import spark.implicits._

var dataverse_export_path = "/mnt/dataverse-export/"
var df = spark.read.json(dataverse_export_path + "/model.json")

var df_attributes = df.select(explode(col("entities.attributes").getItem(0)).alias("attributes"))
var df_header = df_attributes.select("attributes.name") 

// COMMAND ----------

// create a list from first column to df
var headerList = df_header.select("name").map(r => r(0).asInstanceOf[String]).collect()

// read csv without headers
val dfWithoutHeader = spark.read.format("csv").option("inferSchema","true").load(dataverse_export_path + "account/2021-03.csv")

// set header from list.toSeq
val dfAccount = dfWithoutHeader.toDF(headerList.toSeq: _*)

display(dfAccount)

// COMMAND ----------

// DBTITLE 1,Avro - Kafka - Schema registry 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions._
import org.spark_project.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.{Schema, SchemaBuilder}
import org.spark_project.confluent.kafka.schemaregistry.client._
import org.apache.avro.Schema
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.read.confluent.SchemaManager
import java.io.File
import org.apache.spark.sql.types._

  
// Create SESSION for serialization to schema registry 
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//setting up spark configuration for the spark session
val conf: SparkConf = new SparkConf()
//kryo serializer for spark serialization/deserialization
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//using spark locally
conf.setMaster("local[*]")
val spark = SparkSession.builder.appName("Avro format with schema registry").config(conf).getOrCreate()

// config port and ip
val kafkaNodesString = "172.30.5.5:9092"
val schemaRegistryAddress = "http://172.30.5.5:8081"
val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryAddress, 1000)

//create avro aschema with created df
val avroSchema = AvroSchemaUtils.toAvroSchema(dfAccount)
val keySchema = AvroSchemaUtils.toAvroSchema(dfAccount, "Id") // df.Id = avro key

schemaRegistryClient.register("t-value", avroSchema)
schemaRegistryClient.register("t-key", keySchema)

//val input15 = spark
//  .readStream
//  .format("kafka")
//  .option("kafka.bootstrap.servers", kafkaNodesString)
//  .option("subscribe", "input15")
//  .load()

//val streamDF = input15.select(from_avro($"key", "t-key", schemaRegistryAddress), 
//                              from_avro($"value", "t-value", schemaRegistryAddress))

//display(streamDF)

// el value se envia con formato struct
val allColumns = struct(dfAccount.columns.head, dfAccount.columns.tail: _*)

dfAccount
  .select(to_avro('Id.cast("string").as('Id), lit("t-key"), schemaRegistryAddress, keySchema.toString).as('key),
          to_avro(allColumns, lit("t-value"), schemaRegistryAddress, avroSchema.toString).as('value))
  .write
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaNodesString)
  .option("topic", "input15")
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC Fuente: 
// MAGIC - https://docs.databricks.com/_static/notebooks/schema-registry-integration/index.html#avro-data-and-schema-registry.html
// MAGIC - https://shreeraman-ak.medium.com/spark-kafka-and-schema-registry-part-3-83d209a1046e
// MAGIC - https://github.com/AbsaOSS/ABRiS
// MAGIC - https://medium.com/@eswar.kumar/csv-data-streaming-using-kafka-schema-registry-and-avro-920a1de63016
// MAGIC - https://stackoverflow.com/questions/63142533/cannot-convert-catalyst-type-integertype-to-avro-type-null-int
// MAGIC - https://docs.databricks.com/spark/latest/structured-streaming/avro-dataframe.html
