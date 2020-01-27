package com.xiaoniu.dmp.util

import org.apache.spark.sql.types._

/**
  * date  2019/12/31-0:19
  * author zhaishuai
  * Description: 
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
      StructField("cityid", IntegerType, true),
      StructField("price", DoubleType, true),
      StructField("requestDate", StringType, true),
      StructField("appname", StringType, true),
      StructField("orderid", StringType, true),
      StructField("ip", StringType, true),
      StructField("delay", IntegerType, true)
    )
  )
  
}
