package com.xiaoniu.dmp.report

import com.xiaoniu.dmp.util.JedisPools
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis


/**
  * date  2020/1/22-17:00
  * author zhaishuai
  * Description: 把民族文本文件信息存入redis当中,我的redis安装在了linux（192.168.44.162）当中
  * 运行的结果：
  */
object NationInfoImportRedis {
  
  private val logger: Logger = Logger.getLogger(this.getClass.getName)
  
  def main(args: Array[String]): Unit = {
  
    // 较下参数
    if(args.length != 1){
      println(
        """
          |输入参数个数为1
          |输入参数用途是民族文本文件的路径
        """.stripMargin)
      System.exit(0)
    }
    
    val Array(input) = args
  
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
  
    val context = new SparkContext(conf)
  
    // 设置sparkContext的日志级别
    context.setLogLevel("ERROR")
    
    val readRecord: RDD[String] = context.textFile(input)
  
    logger.error(s"====context.defaultParallelism:${context.defaultParallelism}====")
  
    val status: collection.Map[String, (Long, Long)] = context.getExecutorMemoryStatus
    
    status.foreach(ele => logger.error(s"status:${ele.toString()}"))

    
    // 把spark读到的民族的信息放在redis里面
    readRecord.map(ele => {
      val splitArr: Array[String] = ele.split(",")
      (splitArr(1),splitArr(2))
    }).foreachPartition(iter =>{
      
      val jedis: Jedis = JedisPools.getJedis()
      logger.error("====初始化一个logger====")
      iter.foreach(single =>{
        jedis.set(single._1,single._2)
      })
      
      jedis.close()
      
    })
    
    context.stop()
    
  }
  
}
