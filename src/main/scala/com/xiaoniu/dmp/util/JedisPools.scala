package com.xiaoniu.dmp.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * date  2020/1/22-17:03
  * author zhaishuai
  * Description: 工具类
  * 运行的结果：
  */
object JedisPools {
  
    
    //1 定义jedisPool对象
    val jedisPool = new JedisPool(new GenericObjectPoolConfig(),"192.168.44.162",6379)
    
    //2 得到Resource
    def getJedis():Jedis = jedisPool.getResource()
  
  
}
