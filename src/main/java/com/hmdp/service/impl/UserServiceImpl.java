package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不符合 返回错误
            return Result.fail("手机号格式错误");
        }
        // 3.生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 4.保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY.concat(phone), code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        session.setAttribute("code", code);
        // 5.发送
        log.debug("code= {}", code);
        // 6.返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        String formCode = loginForm.getCode();
        String sessionCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY.concat(phone));
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不符合 返回错误
            return Result.fail("手机号格式错误");
        }
        // 3.校验验证码
        if (sessionCode == null || !Objects.equals(sessionCode, formCode)) {
            return Result.fail("验证码错误");
        }
        // 4.一致，根据手机号查询用户
        User user = this.query().eq("phone", phone).one();
        // 5.判断用户是否存在
        if (ObjectUtils.isEmpty(user)) {
            // 创建用户
            user = createUserWithPhone(phone);
        }
        // 6.存在
        // 生成token令牌
        String token = UUID.randomUUID().toString(true);
        // 将User转化hash
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 7.保存用户信息到redis
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY.concat(token),
                BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create().setIgnoreNullValue(true)
                        .setFieldValueEditor((name, value) -> String.valueOf(value))));
        // 设置有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY.concat(token), LOGIN_USER_TTL, TimeUnit.MILLISECONDS);
        // 返回token
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX.concat(RandomUtil.randomString(10)));
        this.save(user);
        return user;
    }
}
