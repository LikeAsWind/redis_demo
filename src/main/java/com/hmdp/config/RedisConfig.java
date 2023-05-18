package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;

/**
 * @description:
 * @author: yangzhitong
 * @time: 2023/5/10 22:25
 */
@Configurable
public class RedisConfig {

    @Bean
    public RedissonClient redissonClient() {
        // 配置类
        Config config = new Config();
        // 添加redis地址，这是单节点，config.useClusterServers()添加集群地址
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setPassword("root");

        return Redisson.create(config);
    }
}
