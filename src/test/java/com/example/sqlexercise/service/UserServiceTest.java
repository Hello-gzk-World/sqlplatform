package com.example.sqlexercise.service;

import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.po.PassRecord;
import com.example.sqlexercise.vo.ResponseVO;
import com.example.sqlexercise.vo.SignVO;
import com.example.sqlexercise.vo.UserVO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@SpringBootTest
class UserServiceTest {

    @Autowired
    private UserService userService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    @Transactional
    void signUp() {
        String name = "testUserName";
        String email = "testEmail";
        String code = "testCode";
        stringRedisTemplate.opsForValue().set(Constants.RedisKey.CODE_KEY_PREFIX + email, code);
        String password = "testPassword";
        String passwordConfirmation = "testPassword";
        UserVO userVO = new UserVO();
        userVO.setName(name);
        userVO.setEmail(email);
        userVO.setCode(code);
        userVO.setPassword(password);
        userVO.setPasswordConfirmation(passwordConfirmation);
        ResponseVO responseVO = userService.signUp(userVO);
        Assertions.assertTrue(responseVO.isSuccess());
        stringRedisTemplate.opsForValue().get(Constants.RedisKey.CODE_KEY_PREFIX + email);
        stringRedisTemplate.delete(Constants.RedisKey.CODE_KEY_PREFIX + email);
    }

    @Test
    void signIn() {
        String password = "123456";
        SignVO signVO = new SignVO();
        signVO.setEmail(ConstantsOfTest.USER_EMAIL);
        signVO.setPassword(password);
        ResponseVO responseVO = userService.signIn(signVO);
        Assertions.assertTrue(responseVO.isSuccess());
    }

    @Test
    void getRecords() {
        List<PassRecord> res = userService.getRecords(ConstantsOfTest.USER_ID);
        Assertions.assertNotNull(res);
    }

    @Test
    void getUserInfo() {
        UserVO res = userService.getUserInfo(ConstantsOfTest.USER_ID);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    @Transactional
    void modifyInfo() {
        UserVO userVO = new UserVO();
        userVO.setEmail(ConstantsOfTest.USER_EMAIL);
        userVO.setPassword("modifyPassword");
        userVO.setName("modifyUserName");
        ResponseVO responseVO = userService.modifyInfo(userVO);
        Assertions.assertTrue(responseVO.isSuccess());
    }

    @Test
    void getStars() {
        Object res = userService.getStars(ConstantsOfTest.USER_ID);
        Assertions.assertNotNull(res);
    }

    @Test
    void getStatistic() {
        Object res = userService.getStatistic(ConstantsOfTest.USER_ID);
        Assertions.assertNotNull(res);
    }

    @Test
    void getRecentSubmits() {
        Object res = userService.getRecentSubmits(ConstantsOfTest.USER_ID, 20);
        Assertions.assertNotNull(res);
    }

    @Test
    @Transactional
    void resetPassword() {
        String email = ConstantsOfTest.USER_EMAIL;
        String code = "testCode";
        stringRedisTemplate.opsForValue().set(Constants.RedisKey.CODE_KEY_PREFIX + email, code);
        String password = "testPassword";
        String passwordConfirmation = "testPassword";
        UserVO userVO = new UserVO();
        userVO.setEmail(email);
        userVO.setCode(code);
        userVO.setPassword(password);
        userVO.setPasswordConfirmation(passwordConfirmation);
        ResponseVO responseVO = userService.resetPassword(userVO);
        Assertions.assertTrue(responseVO.isSuccess());
        stringRedisTemplate.opsForValue().get(Constants.RedisKey.CODE_KEY_PREFIX + email);
        stringRedisTemplate.delete(Constants.RedisKey.CODE_KEY_PREFIX + email);
    }
}
