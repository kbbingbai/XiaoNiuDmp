package com.xiaoniu.dmp.report

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * date  2020/1/19-16:02
  * author zhaishuai
  * Description:
  *   1 作者的统计要求：广告投放的地域分布统计，利用spark_sql的方式进行实现
  *   2 我的统计要求：一个人消费的总额,最高额，使用过哪几个ip（只统计个数）
  * 运行的结果：
  *
  *
  */
object AreaAnalyse01 {
  
  private val logger: Logger = Logger.getLogger(AreaAnalyse01.getClass)
  def main(args: Array[String]): Unit = {
  
    logger.debug(s"==获取的参数是：${args.mkString("&&")}==")
    
    if(args.length !=2 ){
      println(
        """
          |com.xiaoniu.dmp.report.AreaAnalyse01
          | input
          | output
        """.stripMargin)
        System.exit(0)
    }
    
    //1 接受参数
    val Array(input,output) = args
    
    // 2 创建context
    val conf: SparkConf = new SparkConf().setAppName(AreaAnalyse01.getClass.getName).setMaster("local[2]")
    val context = new SparkContext(conf)
    val sqlCon = new SQLContext(context)
    // 3 设置序列化方式
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    //4 读取parquet进行统计分析
    val frame: DataFrame = sqlCon.read.option("basePath","G:/05").parquet(input)
    
    println(frame.printSchema())
    
    // 如果输出目录为空则删除原目录
    val outputFile = new File(output)
    if(outputFile.exists()){
      FileUtils.deleteDirectory(outputFile)
    }
    
    frame.registerTempTable("log")
    val result: DataFrame = sqlCon.sql("select id, sum(price),max(price),count(ip) from log group by id")
    result.coalesce(1).write.json(output)
    
    context.stop;
    
  }
  
}
