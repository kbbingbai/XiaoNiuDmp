package com.xiaoniu.dmp.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * date  2020/1/28-10:45
  * author zhaishuai 这个给广告位打标签，包含两个部分：1 广告的类型（banner 插屏 全屏） 2 广告的类型名称
  * Description: 
  * 运行的结果：
  */
object TagsAds extends TagsTrait {
  /***
    * 广告打标签的方法
    * @param args 参数
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
  
    //下面是返回值
    var tagMap = Map[String,Int]()
    
    // 把Any类型的数据进行强转，转成Row的类型
    val row: Row = args(0).asInstanceOf[Row]
    
    val idspacetype: Int = row.getAs[Int]("idspacetype")
    val idspacetypename: String = row.getAs[String]("idspacetypename")
    
    if(idspacetype<10){
      tagMap += "LC0"+idspacetype -> 1
    }else if(idspacetype > 0){
      tagMap += "LC"+idspacetype -> 1
    }
    
    
    if(StringUtils.isNotEmpty(idspacetypename)){
      tagMap += "LN" +idspacetypename -> 1
    }
  
    tagMap
    
  }
  
}
