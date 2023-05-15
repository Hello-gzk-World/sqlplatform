package com.example.sqlexercise.service;

import com.example.sqlexercise.data.SubQuestionMapper;
import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.po.SubQuestion;
import com.example.sqlexercise.vo.BatchVO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@SpringBootTest
class BatchServiceTest {

    private final BatchService batchService;
    private final SubQuestionMapper subQuestionMapper;

    @Autowired
    BatchServiceTest(BatchService batchService, SubQuestionMapper subQuestionMapper){
        this.batchService = batchService;
        this.subQuestionMapper = subQuestionMapper;
    }

    @Test
    @Transactional
    @Disabled
    void processBatch_all() {
        List<SubQuestion> subQuestions = subQuestionMapper.selectAll();
        for (SubQuestion subQuestion : subQuestions) {
            BatchVO batchVO = new BatchVO();
            batchVO.setBatchText(subQuestion.getAnswer());
            batchVO.setMainId(subQuestion.getMainId());
            batchVO.setSubId(subQuestion.getId());
            batchVO.setDriver("mysql");
            batchVO.setUserId(ConstantsOfTest.USER_ID);
            String s = batchService.processBatch(batchVO, Constants.ProcessSqlMode.RUN);
            Assertions.assertEquals(Constants.Message.PASSED, s);
        }
    }

    @Test
    void processBatchAsync() throws Exception {
//        System.out.println(batchService.testMQ());
//        BatchVO batchVO = new BatchVO();
//        batchVO.setBatchText("select w.eno \n" +
//                "from works w\n" +
//                "group by w.eno\n" +
//                "having sum(hours) > 1000;");
//        batchVO.setDriver("mysql");
//        batchVO.setMainId(1);
//        batchVO.setUserId(ConstantsOfTest.USER_ID);
//        batchVO.setSubId(10);
//        batchService.processBatch(batchVO, Constants.ProcessSqlMode.RUN);
    }

    @Test
    @Transactional
    public void processBatch_1(){
        BatchVO batchVO = new BatchVO();
        batchVO.setBatchText("select w.eno \n" +
                "from works w\n" +
                "group by w.eno\n" +
                "having sum(hours) > 1000;");
        batchVO.setDriver("mysql");
        batchVO.setMainId(1);
        batchVO.setUserId(ConstantsOfTest.USER_ID);
        batchVO.setSubId(10);
        batchService.processBatch(batchVO, Constants.ProcessSqlMode.RUN);
    }

    @Test
    @Transactional
    public void processBatch_2(){
        BatchVO batchVO = new BatchVO();
        batchVO.setBatchText("select s.sname from sailors s\n" +
                "where s.age > 35 and not exists (\n" +
                "\tselect * \n" +
                "\tfrom reserves r join boats b\n" +
                "\ton r.bid = b.bid \n" +
                "\twhere  \n" +
                "\tr.sid = s.sid and \n" +
                "\tb.color = 'RED' and\n" +
                "\t(r.reserve_date between '2020-09-01' and '2020-09-30')\n" +
                ");");
        batchVO.setDriver("mysql");
        batchVO.setMainId(2);
        batchVO.setUserId(ConstantsOfTest.USER_ID);
        batchVO.setSubId(14);
        batchService.processBatch(batchVO, Constants.ProcessSqlMode.RUN);
    }

    @Test
    @Transactional
    public void processBatch_3(){
        BatchVO batchVO = new BatchVO();
        batchVO.setBatchText("select w.eno \n" +
                "from works w\n" +
                "group by w.eno\n" +
                "having sum(hours) > 1000;");
        batchVO.setDriver("oceanbase");
        batchVO.setMainId(1);
        batchVO.setUserId(ConstantsOfTest.USER_ID);
        batchVO.setSubId(10);
        batchService.processBatch(batchVO, Constants.ProcessSqlMode.RUN);
    }
}
