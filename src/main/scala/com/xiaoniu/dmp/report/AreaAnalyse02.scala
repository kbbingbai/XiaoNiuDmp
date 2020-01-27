package com.xiaoniu.dmp.report

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * date  2020/1/19-23:18
  * author zhaishuai
  * Description:  用spark 算子的方式计算下述 指标
  *   一个人消费的总额,最高额，使用过哪几个ip（只统计个数）
  * 日志格式：000001&&张三&&500&&01&&05&&0194&&245.55&&2004-04-08 16:09:16&&拼多多便宜货&&3&&255.56.66.168&&618
  */
object AreaAnalyse02 {
  
  
  def main(args: Array[String]): Unit = {
    
    if(args.length !=2){
      """
        |com.xiaoniu.dmp.report.AreaAnalyse02
        |input
        |output
      """.stripMargin
      System.exit(0)
    }
    
    val Array(input,output) = args
    
    val conf: SparkConf = new SparkConf().
                            setMaster("local[2]").
                            setAppName(AreaAnalyse02.getClass.getName)
    
    val context = new SparkContext(conf)
    
    val sqlContext = new SQLContext(context)
    
    val frame: DataFrame = sqlContext.read.parquet(input)
  
  
    println(frame.printSchema())
    
    import org.apache.spark.sql.functions._
    val newFrame: DataFrame = frame.withColumn("id",col("id").cast(StringType))
    
    println(newFrame.printSchema())
    
    val single: RDD[(String,Double,String)] = newFrame.map(ele => {
      (ele.getString(0),ele.getDouble(5),ele.getString(9))
    })
    
    val result: RDD[(String, Iterable[(String, Double, String)])] = single.groupBy(_._1)
    
    // 如果输出目录为空则删除原目录
    val outputFile = new File(output)
    if(outputFile.exists()){
      FileUtils.deleteDirectory(outputFile)
    }

    val result02: RDD[(String, Double, Double, Int)] = result.map(ele => {
      (ele._1, ele._2.map(_._2).sum, ele._2.map(_._2).max, ele._2.map(_._3).size)
    })
    
    val unit: Unit = result02.coalesce(2).saveAsTextFile(output)
    
    context.stop()
    
  }
  
}
