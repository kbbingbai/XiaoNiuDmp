package com.xiaoniu.dmp.report

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * date  2020/1/20-17:09
  * author zhaishuai
  * Description:  我们的源数据当中民族都是相应的id,现提供一个民族的文本文件，源数据需要关联这个文本文件，把民族id转换成汉字
  * 运行的结果：
  */
object MediumReport01 {
  
  def main(args: Array[String]): Unit = {
    
    if(args.length !=3 ){
      println(
        """
          |com.xiaoniu.dmp.report.AreaAnalyse01
          | input
          | output
        """.stripMargin)
      System.exit(0)
    }
  
    //1 接受参数
    val Array(input,ruleInput,output) = args
  
    // 2 创建context
    val conf: SparkConf = new SparkConf().setAppName(MediumReport01.getClass.getName).setMaster("local[2]")
    val context = new SparkContext(conf)
  
    // 3 把民族信息收集到driver端
    val nationInfo: Map[String, String] = context.textFile(ruleInput).map(ele => {
      val splits: Array[String] = ele.split("[,]")
      (splits(1), splits(2))
    }).collect().toMap
    
    // 4 把数据广播出去
    val nationInfoBrocast: Broadcast[Map[String, String]] = context.broadcast(nationInfo)
  
    val result: RDD[(String, String)] = context.textFile(input).map(ele => {
      
      val splits: Array[String] = ele.split("&&")
      val id = splits(0)
      val nationId = splits(4)
      
      val nationName = nationInfoBrocast.value.getOrElse(nationId,"没有找到")
      
      (id,nationName)
    })
  
    // 如果输出目录为空则删除原目录
    val outputFile = new File(output)
    if(outputFile.exists()){
      FileUtils.deleteDirectory(outputFile)
    }
    result.saveAsTextFile(output)
    context.stop()
    
  }
}
