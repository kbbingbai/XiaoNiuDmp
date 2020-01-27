package com.xiaoniu.dmp.etl

import com.xiaoniu.dmp.util.SchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * date  2019/12/29-16:11
  * author zhaishuai
  * Description:  把数据 用StructType 转成DataFrame 再写成snappy文件
  * 运行的结果：
  */
object Bz2ToParquet {
  
  def main(args: Array[String]): Unit = {
    
   //  0 检查参数的个数
    if(args.length != 2){
      """
        |com.xiaoniu.dmp.etl.Bz2ToParquet
        |参数
        | inputPath  outputPath
      """.stripMargin
      sys.exit()
    }
    
    // 1接收参数
    val Array(inputPath,outputPath) = args
  
    // 2 初始化sparkcontent
    val conf: SparkConf = new SparkConf().setAppName("Bz2ToParquet").setMaster("local[2]")
    val context = new SparkContext(conf)
    val sqlContent = new SQLContext(context)
    
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sqlContent.setConf("spark.sql.parquet.compression.codec", "snappy")
    
    // 3读取源文件
  
    val start = System.currentTimeMillis()
    val textFile: RDD[String] = context.textFile("G:\\20170101-1.txt")

    val result = textFile
      .filter(ele =>ele.contains("&&"))
      .map( row=> {
        Row(
          row.split("&&")(0).toInt,
          row.split("&&")(1),
          row.split("&&")(2),
          row.split("&&")(3),
          row.split("&&")(4).toInt,
          row.split("&&")(5).toInt,
          row.split("&&")(6).toDouble,
          row.split("&&")(7),
          row.split("&&")(8),
          row.split("&&")(9),
          row.split("&&")(10),
          row.split("&&")(11).toInt
        )
    })
    
    val dataFrame: DataFrame = sqlContent.createDataFrame(result,SchemaUtils.structType)

    dataFrame.write.parquet("G:\\02")
    
    println((System.currentTimeMillis() - start)/1000)

    context.stop()

  }
  
}
