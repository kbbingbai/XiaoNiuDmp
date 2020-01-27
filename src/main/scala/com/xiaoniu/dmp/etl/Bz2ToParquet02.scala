package com.xiaoniu.dmp.etl

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}



/**
  * date  2020/1/9-22:54
  * author zhaishuai
  * Description:  使用样例类的形式把数据转成parquet文件
  * 运行的结果：
  */
object Bz2ToParquet02 {
  
  def main(args: Array[String]): Unit = {
  
    // 2 初始化sparkcontent
    val conf: SparkConf = new SparkConf().setAppName("Bz2ToParquet").setMaster("local[2]")
    val context = new SparkContext(conf)
    val sqlContent = new SQLContext(context)
    
    //注意自定义类
    conf.registerKryoClasses(Array(classOf[Log2]))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    sqlContent.setConf("spark.sql.parquet.compression.codec", "snappy")
  
    // 3读取源文件
    val textFile: RDD[String] = context.textFile("G:\\20170101-1.txt")
  
    import sqlContent.implicits._
  
    val logRdd: DataFrame = textFile.map(_.split("&&")).filter(_.length > 10).map(Log2(_)).toDF()
    logRdd.write.partitionBy("name").parquet("G:\\05")
  
    context.stop()
  
  
  }
  
}



/**自定义类*/
class Log2(id: Int,
          name: String,
          age: String,
          sex: String,
          nationid: Int,
          cityid: Int,
          price: Double,
          requestDate: String,
          appname: String,
          orderid: String,
          ip: String,
          delay: Int
         ) extends Product with Serializable {
  /**
    * 利用脚标的方式，获取该对象的某个属性
    * */
  override def productElement(n: Int): Any = n match {
    
    case 0 => id
    case 1 => name
    case 2 => age
    case 3 => sex
    case 4 => nationid
    case 5 => cityid
    case 6 => price
    case 7 => requestDate
    case 8 => appname
    case 9 => orderid
    case 10 => ip
    case 11 => delay
    
  }
  
  //对象一共的属性个数
  override def productArity: Int = this.getClass.getFields.length
  
  //比较两个对象是否是同一个对象
  override def canEqual(that: Any): Boolean =that.isInstanceOf[Log2]
}

/**
  * 伴生对象
  * */
object Log2{
  def apply(arr: Array[String]): Log2 = new Log2(
    arr(0).toInt,
    arr(1),
    arr(2),
    arr(3),
    arr(4).toInt,
    arr(5).toInt,
    arr(6).toDouble,
    arr(7),
    arr(8),
    arr(9),
    arr(10),
    arr(11).toInt
  )
}