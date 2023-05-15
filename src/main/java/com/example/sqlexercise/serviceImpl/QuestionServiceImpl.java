package com.example.sqlexercise.serviceImpl;

import com.example.sqlexercise.data.*;
import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.po.*;
import com.example.sqlexercise.service.QuestionService;
import com.example.sqlexercise.vo.DraftVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Slf4j(topic = "com.example.sqlexercise.serviceImpl.QuestionServiceImpl")
@Service
public class QuestionServiceImpl implements QuestionService {

    private SubQuestionMapper subQuestionMapper;
    private MainQuestionMapper mainQuestionMapper;
    private QuestionStateMapper questionStateMapper;
    private DraftMapper draftMapper;
    private BatchMapper batchMapper;
    private QuestionTagsMapper questionTagsMapper;
    private QuestionRelatedTableInfoMapper questionRelatedTableInfoMapper;

    @Autowired
    public QuestionServiceImpl(SubQuestionMapper subQuestionMapper,
                               MainQuestionMapper mainQuestionMapper,
                               QuestionStateMapper questionStateMapper, DraftMapper draftMapper, BatchMapper batchMapper, QuestionTagsMapper questionTagsMapper, QuestionRelatedTableInfoMapper questionRelatedTableInfoMapper) {
        this.subQuestionMapper = subQuestionMapper;
        this.mainQuestionMapper = mainQuestionMapper;
        this.questionStateMapper = questionStateMapper;
        this.draftMapper = draftMapper;
        this.batchMapper = batchMapper;
        this.questionTagsMapper = questionTagsMapper;
        this.questionRelatedTableInfoMapper = questionRelatedTableInfoMapper;
    }

    @Override
    public String getSchemaNameByMainId(int mainId) {
        if (mainQuestionMapper.selectById(mainId) == null) {
            log.info("Info of main question " + mainId + " is not found!");
            return "该mainId不存在";
        }
        return "main_" + mainId;
    }

    @Override
    public String getSchemaConstructorByMainId(int mainId) {
        MainQuestion mainQuestion = mainQuestionMapper.selectById(mainId);
        if (mainQuestion == null) {
            log.info("Info of main question " + mainId + " is not found!");
        }
        try {
            if (!System.getProperty("os.name").toLowerCase().startsWith("linux")) {
                Path dbPath = Paths.get("src/main/resources/" + mainQuestion.getDbPath());
                if (!Files.exists(dbPath)) {
                    log.info("File of main question" + mainId + "is not found!");
                }
                return Files.readString(dbPath);
            } else {
                InputStream inputStream = this.getClass().getResourceAsStream("/BOOT-INF/classes/" + mainQuestion.getDbPath());
                String content = readStream(inputStream);
                inputStream.close();
                return content;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "IOException occurred!";
        }
    }

    private static String readStream(InputStream inStream) throws Exception {
        ByteArrayOutputStream outSteam = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len = -1;
        while ((len = inStream.read(buffer)) != -1) {
            outSteam.write(buffer, 0, len);
        }
        outSteam.close();
        inStream.close();
        return outSteam.toString();
    }

    /**
     * 获取某一大题下所有小题信息
     */
    @Override
    public List<Map<String, Object>> getSubQuestionByMainId(int mainId) {
        List<Map<String, Object>> res = subQuestionMapper.selectByMainId(mainId);
        // 遍历列表中的每一个小题，添加标签数据
        for (Map<String, Object> subQuestionInfo : res) {
            // 获取当前小题的subId
            Integer subId = (Integer) subQuestionInfo.get("id");
            // 查数据库，获取该小题的标签，结果集合中为标签编号
            List<Integer> tagTypes = questionTagsMapper.selectTagBySubId(subId);
            // 将标签编号转换为标签名
            List<String> tagNames = Constants.QuestionTag.tagTypeListToNameList(tagTypes);
            // 将标签信息添加进当前小题map中
            subQuestionInfo.put("tags", tagNames);
        }
        return res;
    }

    @Override
    public SubQuestion getSubQuestionBySubId(int subId) {
        return subQuestionMapper.selectBySubId(subId);
    }

    /**
     * 获取某一大题信息
     */
    @Override
    public Map<String, Object> getMainQuestionByMainId(int mainId) {
        MainQuestion mainQuestion = mainQuestionMapper.selectById(mainId);
        Map<String, Object> res = new HashMap<>();
        res.put("id", mainQuestion.getId());
        res.put("title", mainQuestion.getTitle());
        res.put("description", mainQuestion.getDescription());
        res.put("subCount", mainQuestion.getSubCount());

        // 查数据库，获取该大题下所有小题的标签，结果集合中为标签编号
        List<Integer> tagTypes = questionTagsMapper.selectTagByMainId(mainId);
        // 将标签编号转换为标签名
        List<String> tagNames = Constants.QuestionTag.tagTypeListToNameList(tagTypes);
        // 将标签信息添加进结果map中
        res.put("tags", tagNames);

        // 查数据库，获取该大题相关表信息
        List<Map<String, Object>> tableInfoByMainId = questionRelatedTableInfoMapper.selectTableInfoByMainId(mainId);
        // 遍历整理结果集
        Map<String, List<String>> tableInfoMap = new HashMap<>();
        for (Map<String, Object> info : tableInfoByMainId) {
            String tableName = (String) info.get("tableName");
            List<String> columnList = tableInfoMap.get(tableName);
            if (columnList == null) {
                columnList = new ArrayList<>();
            }
            columnList.add((String) info.get("columnName"));
            tableInfoMap.put(tableName, columnList);
        }
        List<Map<String, Object>> relatedTableInfo = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : tableInfoMap.entrySet()) {
            Map<String, Object> tmp = new HashMap<>();
            tmp.put("table", entry.getKey());
            tmp.put("columns", entry.getValue());
            relatedTableInfo.add(tmp);
        }
        // 将相关表信息添加进结果map中
        res.put("relatedTableInfo", relatedTableInfo);

        return res;
    }

    /**
     * 分页查询mainQuestion
     *
     * @param userId    userId
     * @param pageSize  每页数据量
     * @param page      页数
     */
    @Override
    public List<Map<String, Object>> getMainQuestionsByPage(String userId, int pageSize, int page) {
        int from = (page - 1) * pageSize;
        List<Map<String, Object>> res = new ArrayList<>();
        // 获取大题总数
        Integer total = mainQuestionMapper.countTotal();
        Map<String, Object> map = new HashMap<>();
        map.put("totalMainQuestionNum", total);
        res.add(map);
        // 查一页大题信息
        List<Map<String, Object>> questions = mainQuestionMapper.selectByPage(userId, from, pageSize);
        // 遍历每一个大题信息，添加标签数据
        for (Map<String, Object> mainQuestionInfo : questions) {
            // 获取当前小题的subId
            Integer mainId = (Integer) mainQuestionInfo.get("mainId");
            // 查数据库，获取该小题的标签，结果集合中为标签编号
            List<Integer> tagTypes = questionTagsMapper.selectTagByMainId(mainId);
            // 将标签编号转换为标签名
            List<String> tagNames = Constants.QuestionTag.tagTypeListToNameList(tagTypes);
            // 将标签信息添加进当前小题map中
            mainQuestionInfo.put("tags", tagNames);
        }
        res.addAll(questions);
        return res;
    }

    /**
     * 根据标签筛选mainQuestion，并分页返回
     *
     * @param userId    userId
     * @param page      页数
     * @param pageSize  每页数据量
     * @param tags      筛选条件，标签列表
     */
    @Override
    public List<Map<String, Object>> getMainQuestionsByPageFilterByTags(String userId, Integer pageSize, Integer page, List<String> tags) {
        int from = (page - 1) * pageSize;
        List<Map<String, Object>> res = new ArrayList<>();
        // 将前端传来的标签名称列表转换为标签类型编号列表
        List<Integer> tagTypeList = Constants.QuestionTag.tagNameListToTypeList(tags);
        // 获取包含这些标签的大题总数
        Integer total = mainQuestionMapper.countTotalAfterFilter(tagTypeList);
        Map<String, Object> map = new HashMap<>();
        map.put("totalMainQuestionNumAfterFilter", total);
        res.add(map);
        // 根据标签，查一页大题信息
        List<Map<String, Object>> questions = mainQuestionMapper.selectByPageFilterByTags(userId, from, pageSize, tagTypeList);
        // 遍历每一个大题信息，添加标签数据
        for (Map<String, Object> mainQuestionInfo : questions) {
            // 获取当前小题的subId
            Integer mainId = (Integer) mainQuestionInfo.get("mainId");
            // 查数据库，获取该小题的标签，结果集合中为标签编号
            List<Integer> tagTypes = questionTagsMapper.selectTagByMainId(mainId);
            // 将标签编号转换为标签名
            List<String> tagNames = Constants.QuestionTag.tagTypeListToNameList(tagTypes);
            // 将标签信息添加进当前小题map中
            mainQuestionInfo.put("tags", tagNames);
        }
        res.addAll(questions);
        return res;
    }

    /**
     * 保存用户做题草稿
     */
    @Override
    public String saveDraft(DraftVO draftVO) {
        Draft draft = draftMapper.select(draftVO.getUserId(), draftVO.getMainId(), draftVO.getSubId());
        boolean firstSave = draft == null;
        draft = new Draft();
        org.springframework.beans.BeanUtils.copyProperties(draftVO, draft);
        draft.setSaveTime(new Date());
        if (firstSave) {
            draftMapper.insert(draft);
        } else {
            draftMapper.update(draft);
        }
        return Constants.Message.SAVE_DRAFT_SUCCEED;
    }

    /**
     * 获取用户保存的做题草稿
     */
    @Override
    public DraftVO getDraft(String userId, Integer mainId, Integer subId) {
        Draft draft = draftMapper.select(userId, mainId, subId);
        if (draft == null) {
            return null;
        }
        DraftVO draftVO = new DraftVO();
        org.springframework.beans.BeanUtils.copyProperties(draft, draftVO);

        return draftVO;
    }

    /**
     * 修改一个subQuestion的状态为 已收藏
     */
    @Override
    public String star(String userId, Integer mainId, Integer subId) {
        // 查询数据库中是否已经有该题目针对该用户的状态记录
        QuestionState questionState = questionStateMapper.select(userId, mainId, subId);
        if (questionState == null) {
            // 若没有，则insert
            questionState = new QuestionState(userId, mainId, subId, true, Constants.QuestionState.NOT_STARTED);
            questionStateMapper.insert(questionState);
        } else {
            // 若有，则update，is_starred字段更新为1，即已收藏
            questionStateMapper.updateIsStarred(userId, mainId, subId, true);
        }
        return Constants.Message.STAR_SUCCEED;
    }

    /**
     * 修改一个subQuestion的状态为 未收藏
     */
    @Override
    public String unStar(String userId, Integer mainId, Integer subId) {
        questionStateMapper.updateIsStarred(userId, mainId, subId, false);
        return Constants.Message.UNSTAR_SUCCEED;
    }

    /**
     * 获取某用户对某一个subQuestion是否收藏、是否通过
     */
    @Override
    public Object getIsStarredAndStateOf(String userId, Integer mainId, Integer subId) {
        Map<String, Object> res = questionStateMapper.selectIsStarredAndStateOf(userId, mainId, subId);
        if (res == null) {
            res = new HashMap<>();
            res.put("isStarred", "未收藏");
            res.put("state", "未开始");
        }
        return res;
    }

    /**
     * 查询某用户对于某subQuestion的提交记录，包含通过和未通过的，且返回的记录列表按时间先后排序，越近的提交记录越靠前
     */
    @Override
    public List<Object> getSubmitRecord(String userId, Integer mainId, Integer subId) {
        List<Object> submitRecord = batchMapper.selectSubmitRecord(userId, mainId, subId);
        return submitRecord;
    }

}
