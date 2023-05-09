package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @description:
 * @author: yangzhitong
 * @time: 2023/5/9 0:10
 */
@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        // 设置逻辑过期对象
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData), time, timeUnit);
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix,
                                          ID id, Class<R> type,
                                          Function<ID, R> dbFallback,
                                          Long time, TimeUnit timeUnit) {
        String key = keyPrefix.concat(String.valueOf(id));
        // 1.从redis中去查
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判空(有数据的时候才是true，""就是false)
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 命中的是否是空值（此时如果是""）
        if (!ObjectUtils.isEmpty(json)) {
            // 返回错误信息
            return null;
        }
        // 4.不存在查库
        R r = dbFallback.apply(id);
        if (ObjectUtils.isEmpty(r)) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 5.存在存入redis
        this.set(key, r, time, timeUnit);
        // 根据情况返回 shop
        return r;
    }


    public <R, ID> R queryWithLogicalExpire(String keyPrefix,
                                            ID id,
                                            Class<R> type,
                                            Function<ID, R> dbFallback,
                                            Long time, TimeUnit timeUnit) {
        String key = keyPrefix.concat(String.valueOf(id));
        // 1.从redis中去查
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判空(有商铺数据的时候才是true，""就是false)
        if (StrUtil.isBlank(json)) {
            // 不存在直接返回null
            return null;
        }
        // 3.命中反序列化对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 4.1 未过期 直接返回
            return r;
        }
        // 4.2 已过期 缓存重建
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        // 5 缓存重建
        // 5.1 获取互斥锁
        // 5.2 判断是否成功
        if (isLock) {
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                // 3.存在，直接返回
                redisData = JSONUtil.toBean(json, RedisData.class);
                return JSONUtil.toBean((JSONObject) redisData.getData(), type);
            }
            // 5.3 成功，开启独立线程实现缓存重建
            CompletableFuture.runAsync(() -> {
                try {
                    // 查数据库
                    R r1 = dbFallback.apply(id);
                    // 写redis
                    this.setWithLogicalExpire(key, r1, time, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(key);
                }
            });
        }
        // 5.4 返回过期的信息
        return r;
    }


    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        Boolean flag = stringRedisTemplate.delete(key);
    }

}
