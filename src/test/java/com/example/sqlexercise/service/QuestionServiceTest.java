package com.example.sqlexercise.service;

import com.example.sqlexercise.data.QuestionStateMapper;
import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.po.QuestionState;
import com.example.sqlexercise.po.SubQuestion;
import com.example.sqlexercise.vo.DraftVO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootTest
class QuestionServiceTest {

    @Autowired
    private QuestionService questionService;

    @Autowired
    private QuestionStateMapper questionStateMapper;

    @Test
    void getSchemaNameByMainId_1() {
        int mainId = 1;
        String res = questionService.getSchemaNameByMainId(mainId);
        String expected = "main_1";
        Assertions.assertEquals(expected, res);
    }

    @Test
    void getSchemaNameByMainId_2() {
        int mainId = 9999;
        String res = questionService.getSchemaNameByMainId(mainId);
        String expected = "该mainId不存在";
        Assertions.assertEquals(expected, res);
    }

    @Test
    void getSchemaConstructorByMainId() {
        int mainId = 1;
        String res = questionService.getSchemaConstructorByMainId(mainId);
        Assertions.assertNotNull(res);
    }

    @Test
    void getSubQuestionByMainId() {
        int mainId = 1;
        List<Map<String, Object>> res = questionService.getSubQuestionByMainId(mainId);
        Assertions.assertNotNull(res);
    }

    @Test
    void getSubQuestionBySubId() {
        int subId = 10;
        SubQuestion res = questionService.getSubQuestionBySubId(subId);
        Assertions.assertNotNull(res);
    }

    @Test
    void getMainQuestionByMainId() {
        int mainId = 1;
        Map<String, Object> res = questionService.getMainQuestionByMainId(mainId);
        Assertions.assertNotNull(res);
    }

    @Test
    void getMainQuestionsByPage() {
        int pageSize = 5;
        int page = 1;
        List<Map<String, Object>> res = questionService.getMainQuestionsByPage(ConstantsOfTest.USER_ID, pageSize, page);
        Assertions.assertNotNull(res);
    }

    @Test
    void getMainQuestionsByPageFilterByTags() {
        int pageSize = 5;
        int page = 1;
        List<Map<String, Object>> res =
                questionService.getMainQuestionsByPageFilterByTags(ConstantsOfTest.USER_ID, pageSize, page,
                        Arrays.asList("时间和日期", "字符串"));
        Assertions.assertNotNull(res);
    }

    @Test
    @Transactional
    void saveDraft() {
        int mainId = 1;
        int subId = 10;
        String draft = "select * from draftTest;";
        Date saveTime = new Date();
        DraftVO draftVO = new DraftVO(ConstantsOfTest.USER_ID, mainId, subId, draft, saveTime);
        String res = questionService.saveDraft(draftVO);
        Assertions.assertEquals(Constants.Message.SAVE_DRAFT_SUCCEED, res);
    }

    @Test
    void getDraft() {
        int mainId = 1;
        int subId = 10;
        DraftVO res = questionService.getDraft(ConstantsOfTest.USER_ID, mainId, subId);
        Assertions.assertNotNull(res);
    }

    @Test
    @Transactional
    void star() {
        int mainId = 1;
        int subId = 10;
        String res = questionService.star(ConstantsOfTest.USER_ID, mainId, subId);
        // 校验返回结果
        Assertions.assertEquals(Constants.Message.STAR_SUCCEED, res);
        QuestionState state = questionStateMapper.select(ConstantsOfTest.USER_ID, mainId, subId);
        // 校验数据库中状态
        Assertions.assertEquals(true, state.getIsStarred());
    }

    @Test
    @Transactional
    void unStar() {
        int mainId = 1;
        int subId = 10;
        String res = questionService.unStar(ConstantsOfTest.USER_ID, mainId, subId);
        // 校验返回结果
        Assertions.assertEquals(Constants.Message.UNSTAR_SUCCEED, res);
        QuestionState state = questionStateMapper.select(ConstantsOfTest.USER_ID, mainId, subId);
        // 校验数据库中状态
        Assertions.assertEquals(false, state.getIsStarred());
    }

    @Test
    void getIsStarredAndStateOf() {
        int mainId = 1;
        int subId = 10;
        Object res = questionService.getIsStarredAndStateOf(ConstantsOfTest.USER_ID, mainId, subId);
        Assertions.assertNotNull(res);
    }

    @Test
    void getSubmitRecord() {
        int mainId = 1;
        int subId = 10;
        List<Object> res = questionService.getSubmitRecord(ConstantsOfTest.USER_ID, mainId, subId);
        Assertions.assertNotNull(res);
    }

}