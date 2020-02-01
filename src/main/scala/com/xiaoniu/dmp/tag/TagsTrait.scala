package com.xiaoniu.dmp.tag

/**
  * date  2020/1/28-10:42
  * author zhaishuai
  * Description: 打标签的一个接口，各个打标签的类，都实现这个接口
  * 运行的结果：
  */
trait TagsTrait {
  
  /***
    * 写一个打标签的方法
    * @param args 参数
    * @return
    */
  def makeTags(args:Any*):Map[String,Int]
  
}
