package com.xiaoniu.dmp.tag

import java.util
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.xiaoniu.dmp.tag.txt2parquet_util.{RedisUtil, Txt2Parquet}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * date  2020/1/27-22:18
  * author zhaishuai
  * Description:  给记录进行打标签
  * 运行的结果：
  */
object Tag2Cxt {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println(
        """ com.xiaoniu.dmp.tag.Tag2Cxt
          |inputDir
          |dict1
          |dict2
          |ouputDir
        """.stripMargin)
    }

    val Array(inputDir,ouputDir) = args

    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(Txt2Parquet.getClass.getSimpleName)
    val context = new SparkContext(conf)
    val sqlCon = new SQLContext(context)

    // 进行解析文件
    val frame: DataFrame = sqlCon.read.parquet(inputDir)

    // 把appname从redis里面取出来
    val appnameSet: util.Set[String] = RedisUtil.resource.smembers("appname")
    val appnameMap = appnameSet.toArray().map(ele =>(ele,ele)).toMap.asInstanceOf[Map[String,String]]

    // 把民族信息从mysql当中提取出来

    val config: Config = ConfigFactory.load()

    val properties = new Properties()
    properties.setProperty("user",config.getString("mysql.user"))
    properties.setProperty("password",config.getString("mysql.password"))

    val jdbcDataFrame: DataFrame = sqlCon.read.jdbc(config.getString("mysql.url"),config.getString("mysql.table.xiaoniucity"),properties)

    import sqlCon.implicits._

    val cityList: util.List[Row] = jdbcDataFrame
      .select($"code", $"name").collectAsList()

    val iterator: util.Iterator[Row] = cityList.iterator()

    var cityMap: Map[String, String] = Map[String,String]()
    while(iterator.hasNext){
      val row: Row = iterator.next()
      cityMap += row.getAs[String](0) ->row.getAs[String](1)
    }

    val appnameMapBroad: Broadcast[Map[String, String]] = context.broadcast[Map[String,String]](appnameMap)
    val cityMapBroad: Broadcast[Map[String, String]] = context.broadcast[Map[String,String]](cityMap)



    // 对购物时长小于 delay < 1000 且 price>'10.00' 的数据进行打标签，否则不打标签
    frame.where("delay<1000 and price > 10").map(row => {
      val tagsAds: Map[String, Int] = TagsAds.makeTags(row)
      val tagsArea: Map[String, Int] = TagArea.makeTags(row,cityMapBroad.value)
      val tagsSex: Map[String, Int] = TagSex.makeTags(row)
      val tagsApp: Map[String, Int] = TagsApp.makeTags(row,appnameMapBroad.value)

      val id: Integer = row.getAs[Integer](0)
      
      (id,(tagsAds++tagsArea++tagsSex++tagsApp).toList)
    }).reduceByKey((a,b) => {

      //List(("zs",1),("ls",1),("zs",1)) ==> groupByKey ==> Map("zs"->List(("zs",1),("zs",1)),"ls"->List(("ls",1)))
      // ==> mapValues 这个算子保持key不变，只改变value ==> Map("zs"->个数,....)
  
      //下面是第一种写法
            (a++b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
      
      // 下面是第二种写法
//      (a++b).groupBy(_._1).mapValues(_.foldLeft(0)((ele,ele01) =>{
//        ele+ele01._2
//      })).toList
  
      // 下面是第三种写法
//      (a ++ b).groupBy(_._1).map {
//        case (k,someTags) => (k,someTags.map(_._2).sum)
//      }.toList
    
    }).saveAsTextFile(ouputDir)


    context.stop()

  }

}
