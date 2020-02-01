package com.xiaoniu.dmp.tag

import org.apache.spark.sql.Row

/**
  * date  2020/1/28-13:47
  * author   地域标签，统计市的标签
  * Description: 
  * 运行的结果：
  */
object TagArea extends TagsTrait {
  /** *
    * 写一个打标签的方法
    *
    * @param args 参数
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    
    //下面是返回值
    var tagMap = Map[String,Int]()
  
    // 把Any类型的数据进行强转，转成Row的类型
    val row: Row = args(0).asInstanceOf[Row]
    
    val areaMap: Map[String,String] = args(1).asInstanceOf[Map[String,String]]
    
    val cityId: String = row.getAs[String]("cityid")
    
    if(areaMap.contains(cityId)){
      tagMap += "City"+areaMap.get(cityId).get -> 1
    }
  
    tagMap
    
  }
}
