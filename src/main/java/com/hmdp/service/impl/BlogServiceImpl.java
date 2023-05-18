package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(record -> {
            queryBlogUser(record);
            isBlogLiked(record);
        });
        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {
        // 1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        // 2.查询有关的用户
        queryBlogUser(blog);
        // 3.查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (ObjectUtils.isEmpty(user)) {
            // 用户未登录 不处理点赞
            return;
        }
        Long id = user.getId();
        //判断是否点赞
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY, id);
        if (score != null) {
            blog.setIsLike(Boolean.TRUE);
        }

    }

    @Override
    public Result likeBlog(Long id) {
        //1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        //2.判断是否点赞
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY, userId.toString());
        //3.未点赞
        if (score == null) {
            //3.1数据库+1
            boolean isSucces = update().setSql("liked = liked + 1").eq("id", id).update();
            //3.2保存用户到redis的set集合
            if (isSucces) {
                stringRedisTemplate.opsForZSet().add(BLOG_LIKED_KEY, userId.toString(), System.currentTimeMillis());
            }
        } else {
            //4.已点赞
            //4.1数据库点赞减一
            boolean isSucces = update().setSql("liked = liked - 1").eq("id", id).update();
            //3.2保存用户到redis的set集合
            if (isSucces) {
                //4.2把用户从redis的set集合移除
                stringRedisTemplate.opsForZSet().remove(BLOG_LIKED_KEY, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 查询top5的点赞用户 zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY, 0, 4);
        if (CollectionUtils.isEmpty(top5)) {
            return Result.ok();
        }
        // 解析出其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 根据id查用户
        List<User> users = userService.query().in("id", ids)
                .last("ORDER BY FIELD(id," + idStr + ")").list();
        List<UserDTO> userDTOS = BeanUtil.copyToList(users, UserDTO.class);
        return Result.ok(userDTOS);
    }
}
