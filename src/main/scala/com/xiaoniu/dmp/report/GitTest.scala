package com.xiaoniu.dmp.report

/**
  * date  2020/1/27-16:03
  * author zhaishuai
  * Description: 
  * 运行的结果：
  */
object GitTest {
  
  def main(args: Array[String]): Unit = {
  
    val tuples = List(("zs",1),("ls",1),("zs",1))
    
    val stringToTuples: Map[String, List[(String, Int)]] = tuples.groupBy(ele =>ele._1)
  
    println(stringToTuples)
    
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(ele => {
      ele.foldLeft(0)(_ + _._2)
    })
    
    println(stringToInt)
    
  }
  
}
