package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MILLISECONDS);
        //逻辑过期解决缓存击穿
        Shop shop1 = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MILLISECONDS);

        if (shop == null) {
            return Result.fail("Shop not found");
        }
        // 根据情况返回 shop
        return Result.ok(shop);
    }

    /**
     * 互斥锁（缓存雪崩）
     *
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY.concat(String.valueOf(id));
        // 1.从redis中去查
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判空(有商铺数据的时候才是true，""就是false)
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            Result.ok(JSONUtil.toBean(shopJson, Shop.class));
        }
        // 命中的是否是空值（此时如果是""）
        if (shopJson != null) {
            // 返回错误信息
            return null;
        }
        // 4.实现缓存重建
        // 4.1 获取互斥锁
        Shop shop = null;
        try {
            boolean isLock = this.tryLock(LOCK_SHOP_KEY.concat(String.valueOf(id)));
            // 4.2 判断获取是否成功
            if (!isLock) {
                // 4.3 失败，则休眠并重试
                Thread.sleep(50);
                return this.queryWithMutex(id);
            }
            // 4.4 获取锁成功再次检查redis缓存是否存在，做doubleCheck 如果存在则无需重建缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                // 3.存在，直接返回
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            // 命中的是否是空值（此时如果是""）
            if (shopJson != null) {
                // 返回错误信息
                return null;
            }
            // 4.5 成功，根据id查询数据库
            shop = this.getById(id);
            if (ObjectUtils.isEmpty(shop)) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 5.存在存入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
            // 6.释放互斥锁
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            this.unLock(LOCK_SHOP_KEY);
        }
        // 根据情况返回 shop
        return shop;
    }

    /**
     * 缓存穿透案例
     *
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY.concat(String.valueOf(id));
        // 1.从redis中去查
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判空(有商铺数据的时候才是true，""就是false)
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 命中的是否是空值（此时如果是""）
        if (shopJson != null) {
            // 返回错误信息
            return null;
        }
        // 4.不存在查库
        Shop shop = this.getById(id);
        if (ObjectUtils.isEmpty(shop)) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 5.存在存入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 根据情况返回 shop
        return shop;
    }


    /**
     * 缓存逻辑过期
     *
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY.concat(String.valueOf(id));
        // 1.从redis中去查
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判空(有商铺数据的时候才是true，""就是false)
        if (StrUtil.isBlank(shopJson)) {
            // 不存在直接返回null
            return null;
        }
        // 3.命中反序列化对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 4.1 未过期 直接返回
            return shop;
        }
        // 4.2 已过期 缓存重建
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        // 5 缓存重建
        // 5.1 获取互斥锁
        // 5.2 判断是否成功
        if (isLock) {
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                // 3.存在，直接返回
                redisData = JSONUtil.toBean(shopJson, RedisData.class);
                return JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
            }
            // 5.3 成功，开启独立线程实现缓存重建
            CompletableFuture.runAsync(() -> {
                try {
                    // 重建缓存
                    this.saveShopToRedis(id, CACHE_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(key);
                }
            });
        }
        // 5.4 返回过期的信息
        return shop;
    }


    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        Boolean flag = stringRedisTemplate.delete(key);
    }


    public void saveShopToRedis(Long id, Long expireTime) {
        // 1.查询店铺数据
        Shop shop = this.getById(id);
        // 2.封装逻辑过期 写入redis
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        // 3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("Shop id is null");
        }
        CompletableFuture.runAsync(() -> this.updateById(shop));
        String key = CACHE_SHOP_KEY.concat(String.valueOf(id));
        stringRedisTemplate.delete(key);
        return Result.ok();
    }
}
