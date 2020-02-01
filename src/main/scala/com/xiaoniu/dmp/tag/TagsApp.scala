package com.xiaoniu.dmp.tag

import org.apache.spark.sql.Row

/**
  * date  2020/1/28-11:12
  * author zhaishuai
  * Description: 给App名称进行打标签
  * 运行的结果：
  */
object TagsApp extends TagsTrait {
  /** *
    * 写一个打标签的方法
    * @param args 参数
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
  
    //下面是返回值
    var tagMap = Map[String,Int]()
  
    // 把Any类型的数据进行强转，转成Row的类型
    val row: Row = args(0).asInstanceOf[Row]
    
    val dictMap: Map[String,String] = args(1).asInstanceOf[Map[String,String]]
    
    val appName: String = row.getAs[String]("appname")
    
    dictMap.contains(appName) match {
      case true =>{
        tagMap += "APP"+dictMap.get(appName) -> 1
      }
    }
    
    tagMap
    
  }
}
