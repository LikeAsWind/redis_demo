package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

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
        Long userId = UserHolder.getUser().getId();
        //判断是否点赞
        Boolean isLike = stringRedisTemplate.opsForSet().isMember(BLOG_LIKED_KEY, userId.toString());
        blog.setIsLike(BooleanUtil.isTrue(isLike));
    }

    @Override
    public Result likeBlog(Long id) {
        //1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        //2.判断是否点赞
        Boolean isLike = stringRedisTemplate.opsForSet().isMember(BLOG_LIKED_KEY, userId.toString());
        //3.未点赞
        if (BooleanUtil.isFalse(isLike)) {
            //3.1数据库+1
            boolean isSucces = update().setSql("liked = liked + 1").eq("id", id).update();
            //3.2保存用户到redis的set集合
            if (isSucces) {
                stringRedisTemplate.opsForSet().add(BLOG_LIKED_KEY, userId.toString());
            }
        } else {
            //4.已点赞
            //4.1数据库点赞减一
            boolean isSucces = update().setSql("liked = liked - 1").eq("id", id).update();
            //3.2保存用户到redis的set集合
            if (isSucces) {
                //4.2把用户从redis的set集合移除
                stringRedisTemplate.opsForSet().remove(BLOG_LIKED_KEY, userId.toString());
            }
        }
        return Result.ok();
    }
}
