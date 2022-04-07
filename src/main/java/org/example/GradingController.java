package org.example;

import org.example.util.Constants;
import org.example.util.data.GetQuestionInfo;
import org.example.util.data.OperateAnswerSet;
import org.example.util.data.OperateScoreRecord;
import org.example.util.vo.AddToAnswerSetVO;
import org.example.util.vo.AnalysisVO;
import org.example.util.vo.GetScoreVO;
import org.example.util.vo.UpsertScoreRecordVO;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author shenyichen
 * @date 2022/3/28
 **/
@RestController
@RequestMapping("/api/score")
public class GradingController {

    @PostMapping("/getScore")
    public float getScore(@RequestBody GetScoreVO data) {
        System.out.println("get score...");
        return getScore(data.getMainId(), data.getSubId(), data.getStudentSql(), data.getMaxScore(), Constants.dbType);
    }

    public float getScore(Integer mainId, Integer subId, String studentSql, float maxScore, String dbType) {
        List<String> answers = OperateAnswerSet.getAnswers(mainId, subId);
        List<String> envSqls = GetQuestionInfo.getEnvSqls(mainId);
        PartialMarking partialMarking = new PartialMarking(dbType, envSqls);
        return partialMarking.partialMarkForAnswerSet(answers, studentSql, maxScore);
    }

    @PostMapping("/getHints")
    public AnalysisVO getHints(@RequestBody GetScoreVO data) {
        System.out.println("get hints...");
        List<String> hints = getHints(data.getMainId(), data.getSubId(), data.getStudentSql(), Constants.dbType);
        hints = hints.stream().distinct().collect(Collectors.toList());
        System.out.println(String.join(";", hints));
        return new AnalysisVO(String.join(";", hints));
    }

    public List<String> getHints(Integer mainId, Integer subId, String studentSql, String dbType) {
        List<String> answers = OperateAnswerSet.getAnswers(mainId, subId);
        List<String> envSqls = GetQuestionInfo.getEnvSqls(mainId);
        PartialMarking partialMarking = new PartialMarking(dbType, envSqls);
        return partialMarking.getHintsForAnswerSet(answers, studentSql);
    }

//    @PostMapping("/addToAnswerSet")
//    public void addToAnswerSet(@RequestBody AddToAnswerSetVO data) {
//        System.out.println("add to answer set");
//        addToAnswerSet(data.getMainId(), data.getSubId(), data.getInstrSql(), Constants.dbType);
//    }
//
//    public void addToAnswerSet(Integer mainId, Integer subId, String instrSql, String dbType) {
//        float score = getScore(mainId, subId, instrSql, 100.0f, dbType);
//        if (score < 100.0f) {
//            OperateAnswerSet.addAnswer(mainId, subId, instrSql);
//        }
//    }
//
//    @PostMapping("/upsertScoreRecord")
//    public void upsertScoreRecord(@RequestBody UpsertScoreRecordVO data) {
//        System.out.println("upsert score record");
//        OperateScoreRecord.upsertScoreRecord(data);
//    }

}
