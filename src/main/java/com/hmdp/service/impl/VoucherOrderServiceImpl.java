package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 初始化提高性能
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    @PostConstruct //当前类初始化之后执行
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR =
            Executors.newSingleThreadExecutor();


    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息获取是否成功
                    if (CollectionUtils.isEmpty(list)) {
                        //2.1如果获取失败，没有消息，继续下一次循环
                        continue;
                    }
                    // 2.2解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.如果获取成功，创建
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1.获取PendingList中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功
                    if (CollectionUtils.isEmpty(list)) {
                        //2.1如果获取失败，说明PendingList没有异常消息，结束循环
                        break;
                    }
                    // 2.2解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.如果获取成功，创建
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理PendingList订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

   /* private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                } finally {

                }

            }
        }*/

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            //获取用户
            Long userId = voucherOrder.getUserId();
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            boolean isLock = lock.tryLock();
            if (!isLock) {
                //失败 返回错误或重试
                log.error("不允许重复下单");
                return;
            }
            try {
                proxy.createVoucherOrder(voucherOrder);
            } catch (IllegalStateException e) {
                throw new RuntimeException(e);
            } finally {
                // 释放锁
                lock.unlock();
            }
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单Id
        long order = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(order)
        );
        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1不为0，没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
       /* // 2.2为0，有购买资格，把下单的信息保存到阻塞队列中

        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        // 2.3放入阻塞队列
        orderTasks.add(voucherOrder);*/
        // 拿到事务代理的对象才可以 （this拿到的是取当前对象）
        proxy = (IVoucherOrderService) AopContext.currentProxy(); // 这样获取当前对象的代理对象
        return Result.ok(order);
    }

    private IVoucherOrderService proxy;


    @Override
    public Result seckillVoucher1(Long voucherId) {
        //1.查询优惠卷
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //未开始
            return Result.fail("秒杀未开始！");
        }
        //3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            //已经结束
            return Result.fail("秒杀已经结束！");
        }
        //4.判断库存是否充足
        Integer stock = voucher.getStock();
        if (stock < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }
        Long userId = UserHolder.getUser().getId();
        /*// 先获取锁（事务提交之后再去释放锁）这样才能确保线程安全
        synchronized (userId.toString().intern()) {
            // 拿到事务代理的对象才可以 （this拿到的是取当前对象）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy(); // 这样获取当前对象的代理对象
            return Result.ok(proxy.createVoucherOrder(voucherId));
        }*/

        /*// 创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean isLock = lock.tryLock(5L);
        if (!isLock) {
            //失败 返回错误或重试
            return Result.fail("一个人只允许下一单");
        }
        try {
            // 拿到事务代理的对象才可以 （this拿到的是取当前对象）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy(); // 这样获取当前对象的代理对象
            return Result.ok(proxy.createVoucherOrder(voucherId));
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放锁
            lock.unlock();
        }*/
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            //失败 返回错误或重试
            return Result.fail("一个人只允许下一单");
        }
        try {
            // 拿到事务代理的对象才可以 （this拿到的是取当前对象）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy(); // 这样获取当前对象的代理对象
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(userId);
            voucherOrder.setVoucherId(voucherId);
            proxy.createVoucherOrder(voucherOrder);
            return Result.ok();
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放锁
            lock.unlock();
        }

    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 根据优惠价id和用户id查询订单 一人一单
        Long userId = voucherOrder.getId();

        // intern 确保当前用户值一样的时候 锁的是一个用户
        // 存在 已经下过单 返回异常
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            // 存在
            log.error("已经够购买过了！");
            return;
        }
        //5.不存在（）扣减库存 （乐观锁 适合更新数据的时候用）
        boolean sunccess = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!sunccess) {
            // 扣减不足
            log.error("库存不足！");
            return;
        }
        this.save(voucherOrder);
    }

    @Transactional
    public Result createVoucherOrder1(Long voucherId) {
        // 根据优惠价id和用户id查询订单 一人一单
        Long userId = UserHolder.getUser().getId();

        // intern 确保当前用户值一样的时候 锁的是一个用户
        // 存在 已经下过单 返回异常
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            // 存在
            return Result.fail("已经够购买过了！");
        }
        //5.不存在（）扣减库存 （乐观锁 适合更新数据的时候用）
        boolean sunccess = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0).update();
        if (!sunccess) {
            // 扣减不足
            return Result.fail("库存不足！");
        }
        //7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.2.用户id
        voucherOrder.setUserId(userId);
        //6.3.代金券id
        voucherOrder.setVoucherId(voucherId);
        //7.返回订单id
        this.save(voucherOrder);
        return Result.ok(orderId);
    }
}
