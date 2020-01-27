package com.xiaoniu.dmp.report

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * date  2020/1/14-23:32
  * author zhaishuai
  * Description:  利用spark算子的方式进行省市的统计
  * 运行的结果：
  * 数据的格式：
  *   id      name                    age  sex cityid nationid  price reqdate appname 订单id ip delay
  *   000006&&刘备后代中国刘备后代中国&&503&&01&&15&&0113&&439.58&&1989-07-01 15:18:15&&鱼台旧货市场&&2&&255.56.66.213&&199
  */
object ProviceCityReport03 {
  
  
  
  private val logger: Logger = Logger.getLogger(ProviceCityReport03.getClass)
  
  def main(args: Array[String]): Unit = {
    
    if(args.length !=2){
      logger.info("参数不够")
      System.exit(0)
    }
    
    val Array(input,output) =args
    
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
    
    val context = new SparkContext(conf)
    
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    val outputFile = new File(output)
    if(outputFile.exists()){
      FileUtils.deleteDirectory(outputFile)
    }
    
    context.textFile(input).
                map(line => line.split("&&",-1))
                .map(ele => ((ele(0),ele(5)),1))
                .reduceByKey(_+_).sortBy(_._1._1)
              .map(ele => ele._1._1+","+ele._1._2+","+ele._2).saveAsTextFile(output)
    
    context.stop()
    
  }
  
}
