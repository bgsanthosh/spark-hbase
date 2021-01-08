package org.cloudera.e2e.spark
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object KafkaHBaseWrite extends Logging {



  def main(args: Array[String]) {
    val ksourcetopic = "twitter"
    val ktargettopic = "tweet"
    val kborkers = "dex-mow-int-smm-broker2.dex-mow.svbr-nqvp.int.cldr.work:9093"

    val conf = new SparkConf()
      .setAppName("Spark Kafka Hbase Streaming")
      .set("spark.kafka.bootstrap.servers", kborkers)
      .set("spark.kafka.sasl.kerberos.service.name", "kafka")
      .set("spark.kafka.security.protocol", "SASL_SSL")
      .set("spark.kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .set("spark.kafka.ssl.truststore.password", "changeit")
      .set("spark.sql.streaming.checkpointLocation", s"/app/mount/checkpoint-temp-1")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    import spark.implicits._


    println("source topic: " + ksourcetopic)
    println("target topic: " + ktargettopic)
    println("brokers: " + kborkers)

    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "dex-mow-int-op-master0.dex-mow.svbr-nqvp.int.cldr.work")
    val hbaseContext = new HBaseContext(spark.sparkContext, hconf)

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "dex-mow-int-smm-broker2.dex-mow.svbr-nqvp.int.cldr.work:9093")
      .option("startingOffsets", "latest")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .option("kafka.ssl.truststore.password", "changeit")
      .option("subscribe", "twitter").load()

    val result1 = df.writeStream.format("org.apache.hadoop.hbase.spark")
      .option("hbase.columns.mapping",
        "name STRING :key," + "email STRING c:email")
      .option("hbase.table", "santhosh")
      .option("hbase.spark.use.hbasecontext", true).start()

    result1.awaitTermination()
  }

}