package com.example.sqlexercise.serviceImpl;

import com.example.sqlexercise.data.*;
import com.example.sqlexercise.lib.PasswordHash;
import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.po.PassRecord;
import com.example.sqlexercise.po.User;
import com.example.sqlexercise.service.UserService;
import com.example.sqlexercise.vo.ResponseVO;
import com.example.sqlexercise.vo.SignVO;
import com.example.sqlexercise.vo.UserVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j(topic = "com.example.sqlexercise.serviceImpl.UserServiceImpl")
@Service
public class UserServiceImpl implements UserService {

    private UserMapper userMapper;
    private PassRecordMapper passRecordMapper;
    private QuestionStateMapper questionStateMapper;
    private BatchMapper batchMapper;
    private SubQuestionMapper subQuestionMapper;
    private QuestionTagsMapper questionTagsMapper;

    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    public UserServiceImpl(UserMapper userMapper, PassRecordMapper passRecordMapper, QuestionStateMapper questionStateMapper, BatchMapper batchMapper, SubQuestionMapper subQuestionMapper, QuestionTagsMapper questionTagsMapper, StringRedisTemplate stringRedisTemplate) {
        this.userMapper = userMapper;
        this.passRecordMapper = passRecordMapper;
        this.questionStateMapper = questionStateMapper;
        this.batchMapper = batchMapper;
        this.subQuestionMapper = subQuestionMapper;
        this.questionTagsMapper = questionTagsMapper;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public ResponseVO signUp(UserVO userVO) {
        //验证码校验
        String email = userVO.getEmail();
        String rightCode = stringRedisTemplate.opsForValue().get(Constants.RedisKey.CODE_KEY_PREFIX + email);
        if (rightCode == null) {
            log.info("验证码已过期");
            return ResponseVO.failure(Constants.Message.CODE_EXPIRED);
        }
        if (!rightCode.equals(userVO.getCode())) {
            log.info("验证码错误");
            return ResponseVO.failure(Constants.Message.CODE_WRONG);
        }
        if (!userVO.getPassword().equals(userVO.getPasswordConfirmation())) {
            log.info("两次输入的密码不一致");
            return ResponseVO.failure(Constants.Message.PASSWORD_CONFIRMATION_WRONG);
        }
        if (userMapper.selectByEmail(email) != null) {
            log.info("该邮箱已注册");
            return ResponseVO.failure(Constants.Message.EMAIL_EXISTED);
        }
        User user = new User();
        BeanUtils.copyProperties(userVO, user);
        user.setId(UUID.randomUUID().toString());
        user.setPasswordHash(PasswordHash.getPasswordHash(userVO.getPassword()));
        user.setCreatedAt(new Date());
        user.setUpdatedAt(new Date());
        user.setRole("user");
        userMapper.insert(user);

        return ResponseVO.success(Constants.Message.SIGNUP_SUCCEED);
    }

    @Override
    public ResponseVO signIn(SignVO signVO) {
        User user = userMapper.selectByEmail(signVO.getEmail());
        if (user == null) {
            log.info("该邮箱 " + signVO.getEmail() + " 未注册");
            return ResponseVO.failure(Constants.Message.EMAIL_NOT_EXISTED);
        }
        boolean isMatch = PasswordHash.comparePassword(signVO.getPassword(), user.getPasswordHash());
        if (isMatch) {
            log.info(user.getEmail() + " 登录成功");
            return ResponseVO.success(user);
        } else {
            log.info("邮箱或密码错误");
            return ResponseVO.failure(Constants.Message.EMAIL_OR_PASSWORD_WRONG);
        }
    }

    @Override
    public List<PassRecord> getRecords(String userId) {
        return passRecordMapper.selectByUserId(userId);
    }

    @Override
    public UserVO getUserInfo(String userId) {
        User user = userMapper.selectById(userId);
        UserVO userVO = new UserVO();
        BeanUtils.copyProperties(user, userVO);
        return userVO;
    }

    @Override
    public ResponseVO modifyInfo(UserVO userVO) {
        // 创建PO对象
        User user = new User();
        BeanUtils.copyProperties(userVO, user);
        String password = userVO.getPassword();
        if (password != null && !password.equals("")) {
            user.setPasswordHash(PasswordHash.getPasswordHash(password));
        }
        String name = userVO.getName();
        if (name != null && !name.equals("")) {
            user.setName(name);
        }
        user.setUpdatedAt(new Date());

        // 执行数据库更新操作
        int res = userMapper.update(user);
        if (res == 1) {
            return ResponseVO.success(Constants.Message.MODIFY_USER_INFO_SUCCEED);
        } else {
            return ResponseVO.failure(Constants.Message.MODIFY_USER_INFO_FAILED);
        }
    }

    /**
     * 获取某一用户收藏的题目列表
     */
    @Override
    public Object getStars(String userId) {
        List<Map<String, Object>> res = questionStateMapper.selectStars(userId);
        // 遍历列表中的每一个小题，添加标签数据
        for (Map<String, Object> subQuestionInfo : res) {
            // 获取当前小题的subId
            Integer subId = (Integer) subQuestionInfo.get("subId");
            // 查数据库，获取该小题的标签，结果集合中为标签编号
            List<Integer> tagTypes = questionTagsMapper.selectTagBySubId(subId);
            // 将标签编号转换为标签名
            List<String> tagNames = Constants.QuestionTag.tagTypeListToNameList(tagTypes);
            // 将标签信息添加进当前小题map中
            subQuestionInfo.put("tags", tagNames);
        }
        return res;
    }

    /**
     * 获取用户做题统计数据，包括已通过题目数、提交未通过题目数、未开始题目数、提交总次数、提交通过次数、提交通过率
     */
    @Override
    public Object getStatistic(String userId) {
        Map<String, Object> res = new HashMap<>();

        // 查subQuestion总数
        int totalNumOfSubQuestion = subQuestionMapper.countTotal();
        res.put("totalNumOfSubQuestion", totalNumOfSubQuestion);

        // 查该用户已通过题目数
        int passedQuestionNum = questionStateMapper.selectPassedByUserId(userId);
        res.put("passedQuestionNum", passedQuestionNum);

        // 查提交未通过题目数
        int submittedButNotPassQuestionNum = questionStateMapper.selectSubmittedButNotPassByUserId(userId);
        res.put("submittedButNotPassQuestionNum", submittedButNotPassQuestionNum);

        // 计算未开始题目数
        int notStartedQuestionNum = totalNumOfSubQuestion - passedQuestionNum - submittedButNotPassQuestionNum;
        res.put("notStartedQuestionNum", notStartedQuestionNum);

        // 查用户提交总次数
        int submitTimes = batchMapper.selectSubmitTimesByUserId(userId);
        res.put("submitTimes", submitTimes);

        // 查用户提交通过次数
        int passTimes = passRecordMapper.selectPassTimesByUserId(userId);
        res.put("passTimes", passTimes);

        // 计算用户提交通过率
        String passRate = String.format("%.2f", (double) passTimes / (double) submitTimes * 100) + '%';
        res.put("passRate", passRate);

        // 查每个标签分别有多少道小题
        Map<Integer, Long> totalSubQuestionNumOfEachTag = new HashMap<>();
        for (Map<String, Object> item : questionTagsMapper.selectTotalSubQuestionNumOfEachTag()) {
            totalSubQuestionNumOfEachTag.put((Integer) item.get("tag"), (Long) item.get("totalSubQuestionNum"));
        }

        // 查当前用户在每个标签对应的小题中通过了多少道
        Map<Integer, Long> passedSubQuestionNumOfEachTag = new HashMap<>();
        for (Map<String, Object> item : questionTagsMapper.selectPassedSubQuestionNumOfEachTag(userId)) {
            passedSubQuestionNumOfEachTag.put((Integer) item.get("tag"), (Long) item.get("passedNum"));
        }

        // 整理为标签雷达图所需数据结构，并添加进结果集中
        // 例，[
        //         {"标签1": {"该标签小题总数": xx, "该用户在该标签小题中通过数": xx}},
        //         {"标签2": {"该标签小题总数": xx, "该用户在该标签小题中通过数": xx}},
        //         ...
        //     ]
        List<Map<String, Map<String, Object>>> tagRadar = new ArrayList<>();
        for (int tagType = 0; tagType < Constants.QuestionTag.TOTAL_TAG_NUM; tagType++) {
            String tagName = Constants.QuestionTag.tagTypeToName(tagType);
            Map<String, Object> curTagStatistic = new HashMap<>();
            curTagStatistic.put("totalNum", totalSubQuestionNumOfEachTag.getOrDefault(tagType, 0L));
            curTagStatistic.put("passedNum", passedSubQuestionNumOfEachTag.getOrDefault(tagType, 0L));
            Map<String, Map<String, Object>> curTag = new HashMap<>();
            curTag.put(tagName, curTagStatistic);
            tagRadar.add(curTag);
        }
        res.put("tagRadar", tagRadar);

        return res;
    }

    /**
     * 获取用户最近 n 条提交记录
     */
    @Override
    public Object getRecentSubmits(String userId, int n) {
        List<Map<String, Object>> res = batchMapper.selectRecentSubmits(userId, n);
        // 由于提交记录也是以小题为单位，故此处查询标签信息并添加进结果集的逻辑与getStars()方法中相同
        // 遍历列表中的每一条记录，添加标签数据
        for (Map<String, Object> recordInfo : res) {
            // 获取当前记录的subId
            Integer subId = (Integer) recordInfo.get("subId");
            // 查数据库，获取该小题的标签，结果集合中为标签编号
            List<Integer> tagTypes = questionTagsMapper.selectTagBySubId(subId);
            // 将标签编号转换为标签名
            List<String> tagNames = Constants.QuestionTag.tagTypeListToNameList(tagTypes);
            // 将标签信息添加进当前小题map中
            recordInfo.put("tags", tagNames);
        }
        return res;
    }

    /**
     * 根据邮箱验证码重置密码
     * @param userVO 只包含email, code, password, passwordConfirmation四个参数
     */
    @Override
    public ResponseVO resetPassword(UserVO userVO) {
        //验证码校验
        String email = userVO.getEmail();
        String rightCode = stringRedisTemplate.opsForValue().get(Constants.RedisKey.CODE_KEY_PREFIX + email);
        if (rightCode == null) {
            log.info("验证码已过期");
            return ResponseVO.failure(Constants.Message.CODE_EXPIRED);
        }
        if (!rightCode.equals(userVO.getCode())) {
            log.info("验证码错误");
            return ResponseVO.failure(Constants.Message.CODE_WRONG);
        }
        if (!userVO.getPassword().equals(userVO.getPasswordConfirmation())) {
            log.info("两次输入的密码不一致");
            return ResponseVO.failure(Constants.Message.PASSWORD_CONFIRMATION_WRONG);
        }
        if (userMapper.selectByEmail(email) == null) {
            log.info("该邮箱未注册");
            return ResponseVO.failure(Constants.Message.EMAIL_NOT_EXISTED);
        }
        ResponseVO responseVO = modifyInfo(userVO);
        if (responseVO.isSuccess()) {
            return ResponseVO.success(Constants.Message.RESET_PASSWORD_SUCCEED);
        } else {
            return ResponseVO.success(Constants.Message.RESET_PASSWORD_FAILED);
        }
    }
}
