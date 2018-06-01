package com.nayouliang.redis;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foodinfo.base.SerializeUtils;
import com.foodinfo.base.SysConfig;
import com.foodinfo.enums.SysConfigEnum;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class RedisUtil {

    private static Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    //Redis服务器IP
    private static String ADDR =""; 

    //Redis的端口号
    private static int PORT = 0;

    //访问密码
    private static String AUTH = "";

    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static int MAX_ACTIVE = 0;

    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static int MAX_IDLE = 0;

    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int MAX_WAIT = 0;

    private static int TIMEOUT = 0;

    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static boolean TEST_ON_BORROW = true;

    private static JedisPool jedisPool = null;

    private static final int expireTime=10;//分布式锁超时
    /**
     * 分布式锁的添加参数
     */
    private static final Long RELEASE_SUCCESS = 1L;
    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "PX";
    /**
     * 初始化Redis连接池
     */
    static {
        try {
        	
        	ADDR = SysConfig.getConfig("redis.ip",SysConfigEnum.REDIS);
        	PORT = Integer.parseInt(SysConfig.getConfig("redis.port",SysConfigEnum.REDIS));
        	AUTH = SysConfig.getConfig("redis.password",SysConfigEnum.REDIS);
        	MAX_ACTIVE = Integer.parseInt(SysConfig.getConfig("redis.max_active",SysConfigEnum.REDIS));
        	MAX_IDLE =Integer.parseInt(SysConfig.getConfig("redis.max_idle",SysConfigEnum.REDIS));
        	MAX_WAIT =Integer.parseInt(SysConfig.getConfig("redis.max_wait",SysConfigEnum.REDIS));
        	TIMEOUT =Integer.parseInt(SysConfig.getConfig("redis.timeout",SysConfigEnum.REDIS));
        	
        	JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(MAX_ACTIVE);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxWaitMillis(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);
            jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT,AUTH);
        } catch (Exception e) {
            logger.error("redis 初始异常",e);
        }
    }

    /**
     * 获取Jedis实例
     * @return
     */
    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 释放jedis资源
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
        	jedis.close();
        }
    }

    /**
     * 获取redis键值-object
     * 
     * @param key
     * @return
     */
    public static Object getObject(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            byte[] bytes = jedis.get(key.getBytes());
            if(bytes!=null && bytes.length>0) {
                return SerializeUtils.deSerialize(bytes);
            }
        } catch (Exception e) {
            logger.error("getObject获取redis键值异常:key=" + key + " cause:" + e.getMessage());
        } finally {
            jedis.close();
        }
        return null;
    }

    /**
     * 设置redis键值-object
     * @param key
     * @param value
     * @param expiretime
     * @return
     */
    public static String setObject(String key, Object value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.set(key.getBytes(), SerializeUtils.serialize(value));
        } catch (Exception e) {
            logger.error("setObject设置redis键值异常:key=" + key + " value=" + value + " cause:" + e.getMessage());
            return null;
        } finally {
            if(jedis != null)
            {
                jedis.close();
            }
        }
    }

    public static String setObject(String key, Object value,int expiretime) {
        String result = "";
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            result = jedis.set(key.getBytes(), SerializeUtils.serialize(value));
            if(result.equals("OK")) {
                jedis.expire(key.getBytes(), expiretime);
            }
            return result;
        } catch (Exception e) {
            logger.error("setObject设置redis键值异常:key=" + key + " value=" + value + " cause:" + e.getMessage());
        } finally {
            if(jedis != null)
            {
                jedis.close();
            }
        }
        return result;
    }

    /**
     * 删除key
     */
    public static Long delkeyObject(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.del(key.getBytes());
        }catch(Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            if(jedis != null)
            {
                jedis.close();
            }
        }
    }
    /**
     * 是否存在
     * @param key
     * @return
     */
    public static Boolean existsObject(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.exists(key.getBytes());
        }catch(Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            if(jedis != null)
            {
                jedis.close();
            }
        }
    }
    
    /**
     * 还剩多长时间
     * @param key
     * @return
     */
    public static long ttlDate(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.ttl(key);
        }catch(Exception e) {
            e.printStackTrace();
            return 0;
        }finally{
            if(jedis != null)
            {
                jedis.close();
            }
        }
    }
    
    /**
     * 尝试获取分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @param expireTime 超期时间
     * @return 是否获取成功
     */
    public static boolean tryGetDistributedLock(String lockKey, String requestId) {

    	Jedis jedis = null;
    	
        try {
            jedis = jedisPool.getResource();
            
            String result = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);

            if (LOCK_SUCCESS.equals(result)) {
                return true;
            }
            
            return false;
        }catch(Exception e) {
            logger.error("分布式锁失败",e);
            return false;
        }finally{
            if(jedis != null)
            {
                jedis.close();
            }
        }
    	
    	
        

    }

    
    
    
    /**
     * 释放分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @return 是否释放成功
     */
    public static boolean releaseDistributedLock(String lockKey, String requestId) {

    	Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));

            if (RELEASE_SUCCESS.equals(result)) {
                return true;
            }
            return false;
        }catch(Exception e) {
            logger.error("分布式锁失败",e);
            return false;
        }finally{
            if(jedis != null)
            {
                jedis.close();
            }
        }
        

    }
}