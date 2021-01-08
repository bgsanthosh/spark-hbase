package org.cloudera.e2e.spark

import java.sql.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._

import sys.process._
import scala.io.Source
import java.io.File
import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

case class Person(name: String, email: String, birthDate: Date, height: Float)

object HBaseRead {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkHBaseRead")
      .enableHiveSupport().getOrCreate()

    try {

      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "dex-dev-od-master6.dex-dev.xcu2-8y8x.dev.cldr.work")
      val hbaseContext = new HBaseContext(spark.sparkContext, conf)
      spark.sqlContext.read.format("org.apache.hadoop.hbase.spark")

      val sql = spark.sqlContext
      val df = sql.read.format("org.apache.hadoop.hbase.spark")
        .option("hbase.columns.mapping","name STRING :key,email STRING c:email, " + "birthDate DATE p:birthDate, height FLOAT p:height")
        .option("hbase.table","person")
        .load()

      df.show()


    } finally {
      spark.stop()


    }

  }
}
