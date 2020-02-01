package com.xiaoniu.dmp.tag.txt2parquet_util

import org.apache.spark.sql.types._

/**
  * date  2020/1/28-9:20
  * author zhaishuai
  * Description:  视频当中有打标签的需求，所以在生成日志的时候，
  *               暂时增加了两个字段，一个是idspacetype（广告位类型） idspacetypename（广告位名称）
  * 运行的结果：
  */
object SchemaUtils {
  
  val structType = StructType(
    Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", StringType, true),
      StructField("sex", StringType, true),
      StructField("nationid", IntegerType, true),
      StructField("cityid", StringType, true),
      StructField("price", DoubleType, true),
      StructField("requestDate", StringType, true),
      StructField("appname", StringType, true),
      StructField("orderid", StringType, true),
      StructField("ip", StringType, true),
      StructField("delay", IntegerType, true),
      StructField("idspacetype", IntegerType, true),
      StructField("idspacetypename", StringType, true)
    )
  )
  
  
}
