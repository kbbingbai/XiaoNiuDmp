package com.xiaoniu.dmp.tag.txt2parquet_util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * date  2020/1/28-16:10
  * author zhaishuai  从redis里面进行取出数据
  * Description: 
  * 运行的结果：
  */
object RedisUtil {
  
   val pool = new JedisPool(new GenericObjectPoolConfig,"192.168.44.162",6379)
  
   val resource: Jedis = pool.getResource
  
}
