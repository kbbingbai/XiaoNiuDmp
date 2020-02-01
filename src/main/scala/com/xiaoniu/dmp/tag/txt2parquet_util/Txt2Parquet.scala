package com.xiaoniu.dmp.tag.txt2parquet_util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * date  2020/1/28-9:05
  * author zhaishuai
  * Description:  由于新增加两个字段，所以重新写了该类，可以把txt转成parquet文件
  * 运行的结果：
  */
object Txt2Parquet {
  
  def main(args: Array[String]): Unit = {
    
    if(args.length != 2){
      println(
        """ com.xiaoniu.dmp.tag.Txt2Parquet
          |inputDir
          |ouputDir
        """.stripMargin)
    }
    
    val Array(inputDir,ouputDir) = args
    
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(Txt2Parquet.getClass.getSimpleName)
    val context = new SparkContext(conf)
    val sqlCon = new SQLContext(context)
    
    sqlCon.setConf("spark.sql.parquet.compression.codec","snappy")
  
    val result: RDD[Row] = context.textFile(inputDir).map(_.split("&&")).filter(_.length == 14).map(row => {
      Row(
        row(0).toInt,
        row(1),
        row(2),
        row(3),
        row(4).toInt,
        row(5),
        row(6).toDouble,
        row(7),
        row(8),
        row(9),
        row(10),
        row(11).toInt,
        row(12).toInt,
        row(13)
      )
    })
  
    val frame: DataFrame = sqlCon.createDataFrame(result,SchemaUtils.structType)
    
    frame.coalesce(1).write.parquet(ouputDir)
    
    context.stop()

  }
  
}
