package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {

        List<String> shopTypeList = stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, -1);
        List<ShopType> typeList = null;
        if (!CollectionUtils.isEmpty(shopTypeList)) {
            for (String shopType : shopTypeList) {
                typeList.add(JSONUtil.toBean(shopType, ShopType.class));
            }
            return Result.ok(typeList);
        }
        typeList = this.query().orderByAsc("sort").list();
        stringRedisTemplate.opsForList().leftPushAll(CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}
