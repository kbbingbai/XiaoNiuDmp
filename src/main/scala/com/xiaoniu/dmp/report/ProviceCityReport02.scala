package com.xiaoniu.dmp.report

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * date  2020/1/11-9:35
  * author zhaishuai
  * Description: 用sparkSql的方式进行省市的统计，并保存到sql当中
  *   视频当中的需求是统计 省市的数据
  *   我统计的是 id(人) 市 的数据
  *
  *   需求一：基于parquet文件进行统计
  *   需求二：将统计出来的结果存在到mysql当中
  *
  * 运行的结果：
  */
object ProviceCityReport02 {

  private val logger: Logger = Logger.getLogger(ProviceCityReport02.getClass)
  
  def main(args: Array[String]): Unit = {
  
    logger.debug(s"==获取的参数是：${args.mkString("&&")}==")
    //  0 检查参数的个数
    if(args.length != 1){
      
      logger.debug(s"==参数个数错误，现在参数个数为${args.length}==")
      
      """
        |com.xiaoniu.dmp.report.ProviceCityReport
        |参数
        | inputPath  outputPath
      """.stripMargin
      sys.exit()
    }
    
    var Array(input) = args
    
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[4]")
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    
    val frame: DataFrame = sqlContext.read.parquet(input)
  
    logger.info(s"==${frame.printSchema()}==")
    
    frame.registerTempTable("log")
    
    val result: DataFrame = sqlContext.sql("select id,cityid,count(*) from log group by id,cityid order by id")
    
    
    // 加载配置文件  application.conf --> application.json  --> application.properties
    val load: Config = ConfigFactory.load()
    
    val properties = new Properties()
    
    properties.setProperty("user",load.getString("mysql.user"))
    properties.setProperty("password",load.getString("mysql.password"))
    
    
    result.write.mode(SaveMode.Ignore).jdbc(load.getString("mysql.url"),load.getString("mysql.table"),properties)
    
    context.stop()
    
  }



}
