package com.lugan.flink.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolUtils {
    public static JedisPoolConfig createJedisPoolConfig(){
        JedisPoolConfig poolConfig = new JedisPoolConfig();
         poolConfig.setMaxIdle(400);
         poolConfig.setMaxTotal(1200);
         poolConfig.setMinIdle(500);
         poolConfig.setMaxWaitMillis(1000*10);
         poolConfig.setTestOnBorrow(false);
        return poolConfig ;
    }
//    public static JedisPool getJedisPool1=new JedisPool(createJedisPoolConfig(),"192.168.25.148");
//    public static Jedis getJedis(){
//        return  getJedisPool1.getResource();
//    }
//    public static JedisPool getJedisPool(){
//        return getJedisPool1;
//    }
public static JedisPool getJedisPool(){
        return new JedisPool(createJedisPoolConfig(),"localhost");
    }
       public static Jedis getJedis(){
        return  getJedisPool().getResource();
    }

}
