package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

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

    @Override
    public Result seckillVoucher(Long voucherId) {
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

        // 创建锁对象
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
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
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
