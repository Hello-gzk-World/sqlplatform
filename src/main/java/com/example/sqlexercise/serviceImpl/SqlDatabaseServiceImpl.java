package com.example.sqlexercise.serviceImpl;

import com.alibaba.fastjson.JSON;
import com.example.sqlexercise.driver.relational.RelationalClient;
import com.example.sqlexercise.driver.relational.MysqlClient;
import com.example.sqlexercise.lib.ResultOfTask;
import com.example.sqlexercise.lib.SqlDatabase;
import com.example.sqlexercise.lib.SqlDatabasePool;
import com.example.sqlexercise.po.SubQuestion;
import com.example.sqlexercise.service.QuestionService;
import com.example.sqlexercise.service.SqlDatabaseService;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class SqlDatabaseServiceImpl implements SqlDatabaseService {

    private final SqlDatabasePool sqlDatabasePool;
    private final QuestionService questionService;
    private final RedisTemplate<Integer, Object> redisTemplate;
    private final Cache<Integer, Object> caffeineCache;

    @Autowired
    public SqlDatabaseServiceImpl(QuestionService questionService, SqlDatabasePool sqlDatabasePool,
                                  RedisTemplate<Integer, Object> redisTemplate,
                                  @Qualifier("localCacheManager") Cache<Integer, Object> caffeineCache) {
        this.questionService = questionService;
        this.sqlDatabasePool = sqlDatabasePool;
        this.redisTemplate = redisTemplate;
        this.caffeineCache = caffeineCache;
    }

    @Override
    public Object runSqlTask(int mainId, String driver, String sqlText, Map options) {
        int maxRetryTimes = Integer.parseInt(options.getOrDefault("maxRetryTimes", 1).toString());
        boolean forEach = Boolean.parseBoolean(options.getOrDefault("forEach", false).toString());
        boolean skipPre = Boolean.parseBoolean(options.getOrDefault("skipPre", false).toString());
        boolean skipPost = Boolean.parseBoolean(options.getOrDefault("skipPost", false).toString());
        String schemaName = questionService.getSchemaNameByMainId(mainId);
        String schemaConstructor = questionService.getSchemaConstructorByMainId(mainId);
        if (forEach) {
            ArrayList<SqlDatabase> sqlDatabaseArrayList = sqlDatabasePool.getSqlDatabaseList(schemaName, driver);
            ArrayList<ResultOfTask> results = new ArrayList<>();
            for (SqlDatabase sqlDatabase : sqlDatabaseArrayList) {
                if (!skipPre) {
                    sqlDatabase.pretask(schemaConstructor);
                }
                results.add(sqlDatabase.task(sqlText, maxRetryTimes));
                if (!skipPost) {
                    sqlDatabase.posttask();
                }
            }
            return results;
        } else {
            SqlDatabase sqlDatabase = sqlDatabasePool.pickSqlDatabase(schemaName, driver);
            if (!skipPre) {
                sqlDatabase.pretask(schemaConstructor);
            }
            ResultOfTask result = sqlDatabase.task(sqlText, maxRetryTimes);
            if (!skipPost) {
                sqlDatabase.posttask();
            }
            return result;
        }
    }

    @Override
//    @Cacheable("standardAnswer")
    public ResultOfTask getStandardAnswer(int subId, String driver) {
        // 1.先从Caffeine缓存中读取
        Object o = caffeineCache.getIfPresent(subId);
        if (Objects.nonNull(o)) {
            System.out.println("从Caffeine中查询到数据...");
            return (ResultOfTask) o;
        }

        // 2.如果缓存中不存在，则从Redis缓存中查找
        String jsonString = (String) redisTemplate.opsForValue().get(subId);
        ResultOfTask result = JSON.parseObject(jsonString, ResultOfTask.class);
        if (Objects.nonNull(result)) {
            System.out.println("从Redis中查询到数据...");

            // 保存Caffeine缓存
            caffeineCache.put(subId, result);
            return result;
        }

        // 3.如果Redis缓存中不存在，则从数据库中查询
        SubQuestion subQuestion = questionService.getSubQuestionBySubId(subId);
        int mainId = subQuestion.getMainId();
        String sqlText = subQuestion.getAnswer();
        boolean ordered = subQuestion.isOrdered();
        Map options = new HashMap();
        options.put("skipPost", true);
        result = (ResultOfTask) this.runSqlTask(mainId, driver, sqlText, options);
        result.ordered = ordered;

        if (Objects.nonNull(result)) {
            // 保存Caffeine缓存
            caffeineCache.put(subId, result);

            // 保存Redis缓存,20s后过期
            redisTemplate.opsForValue().set(subId, JSON.toJSONString(result), 15, TimeUnit.MINUTES);
        }
        System.out.println("从数据库中查询到数据...");
        return result;
    }

    @Override
    public ResultOfTask runTaskOfGettingSchemaInfo(int mainId, String driver) {
        String schemaName = questionService.getSchemaNameByMainId(mainId);
        RelationalClient client = null;
        if (driver.equals("mysql")) {
            client = new MysqlClient();
        }
        String sqlText = client.getSchemaSql(schemaName);
        Map options = new HashMap();
        options.put("skipPre", true);
        options.put("skipPost", true);
        return (ResultOfTask) this.runSqlTask(mainId, driver, sqlText, options);
    }

    @Override
    public ArrayList<ResultOfTask> runTaskOfUpdatingSchemaInfo(int mainId, String driver) {
        String schemaName = questionService.getSchemaNameByMainId(mainId);
        String schemaConstructor = questionService.getSchemaConstructorByMainId(mainId);
        RelationalClient client = null;
        if (driver.equals("mysql")) {
            client = new MysqlClient();
        }
        String sqlText = client.initSchemaSql(schemaName) + schemaConstructor;
        Map options = new HashMap();
        options.put("forEach", true);
        options.put("skipPre", true);
        options.put("skipPost", true);
        return (ArrayList<ResultOfTask>) this.runSqlTask(mainId, driver, sqlText, options);
    }

    @Override
    public ArrayList<ResultOfTask> runTaskOfCleaningSchema(int mainId, String driver) {
        String schemaName = questionService.getSchemaNameByMainId(mainId);
        RelationalClient client = null;
        if (driver.equals("mysql")) {
            client = new MysqlClient();
        }
        String sqlText = client.cleanSchemaSql(schemaName);
        Map options = new HashMap();
        options.put("forEach", true);
        options.put("skipPre", true);
        options.put("skipPost", true);
        return (ArrayList<ResultOfTask>) this.runSqlTask(mainId, driver, sqlText, options);
    }

}
