package com.sparrowrecsys.online.datamanager;

import redis.clients.jedis.Jedis;

/**
 * RedisClient 类，用于创建和管理 Jedis 单例对象，提供对 Redis 的访问。
 * Jedis 是一个 Redis 客户端，提供了与 Redis 服务器交互的功能。
 */
public class RedisClient {
    // 单例的 Jedis 客户端对象
    private static volatile Jedis redisClient;
    
    // Redis 服务器的地址和端口
    final static String REDIS_END_POINT = "localhost";
    final static int REDIS_PORT = 6379;

    /**
     * 私有构造函数，初始化 Jedis 客户端。
     * 由于 RedisClient 是单例模式，该构造函数只会被调用一次。
     */
    private RedisClient(){
        // 创建与 Redis 服务器的连接
        redisClient = new Jedis(REDIS_END_POINT, REDIS_PORT);
    }

    /**
     * 获取 Jedis 客户端的单例实例。
     * 采用双重检查锁定（Double-Checked Locking）确保线程安全，并且避免不必要的同步开销。
     * 
     * @return Jedis 客户端实例
     */
    public static Jedis getInstance(){
        if (null == redisClient){
            synchronized (RedisClient.class){
                if (null == redisClient){
                    // 创建 Jedis 实例并连接到 Redis 服务器
                    redisClient = new Jedis(REDIS_END_POINT, REDIS_PORT);
                }
            }
        }
        // 返回 Jedis 实例
        return redisClient;
    }
}
