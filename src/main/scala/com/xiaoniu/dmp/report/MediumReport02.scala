package com.xiaoniu.dmp.report

import com.xiaoniu.dmp.util.JedisPools
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Description:  对MediumReport01 进行改造，使用redis实现与对MediumReport01 一样的效果
  * 运行的结果：
  */
object MediumReport02 {
  
  private val logger: Logger = Logger.getLogger(MediumReport02.getClass)
  
  def main(args: Array[String]): Unit = {
    
    if(args.length != 2 ){
      println(
        """
          |com.xiaoniu.dmp.report.MediumReport02
          | input
          | output
        """.stripMargin)
      System.exit(0)
    }
    
    //1 接受参数
    val Array(input,output) = args
  
    logger.info(s"接受的参数是：${input}-----${output}")
    
    // 2 创建context
    val conf: SparkConf = new SparkConf().setAppName(MediumReport02.getClass.getName).setMaster("yarn-client")
    val context = new SparkContext(conf)
    
    val result: RDD[(String, String)] = context.textFile(input).map(ele => {
      val jedis: Jedis = JedisPools.getJedis()
      logger.info(s"==== jedis的对象：${jedis}")
      val splits: Array[String] = ele.split("&&")
      val id = splits(0)
      val nationId = splits(4)
      
      val nationName: String = jedis.get(nationId)
      
      (id,nationName)
    })
    
   
    logger.info(s"====计算完了")
    result.saveAsTextFile(output)
    context.stop()
    
  }
}
