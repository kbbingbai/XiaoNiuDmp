package com.xiaoniu.dmp.tag

import org.apache.spark.sql.Row

/**
  * date  2020/1/28-13:41
  * author zhaishuai 性别标签
  * Description: 
  * 运行的结果：
  */
object TagSex extends TagsTrait {
  /** *
    * @param args 参数
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
  
    //下面是返回值
    var tagMap = Map[String,Int]()
  
    // 把Any类型的数据进行强转，转成Row的类型
    val row: Row = args(0).asInstanceOf[Row]
    
    val sex: String = row.getAs[String]("sex")

    tagMap += "Sex"+ sex -> 1
    
    tagMap

  }
  
}
