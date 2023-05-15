package com.example.sqlexercise.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * 常量类
 */
public class Constants {

    /**
     * Redis中key的前缀常量
     */
    public static class RedisKey {
        /**
         * 验证码前缀
         */
        public static final String CODE_KEY_PREFIX = "CODE:";
    }

    /**
     * 执行用户写的sql的模式，运行/提交
     */
    public static class ProcessSqlMode {
        public static final String RUN = "RUN";
        public static final String SUBMIT = "SUBMIT";
    }

    /**
     * Question状态值，未开始、提交未通过、已通过
     */
    public static class QuestionState {
        public static final int NOT_STARTED = 0;
        public static final int SUBMITTED_BUT_NOT_PASS = 1;
        public static final int PASSED = 2;
    }

    /**
     * Docker相关
     */
    public static class DockerRelated {
        /**
         * MySQL docker image     的名称、版本号
         * docker container 的默认端口
         */
        public static final String MYSQL_IMAGE_NAME = "mysql";
        public static final String MYSQL_IMAGE_TAG = "5.7";
        public static final String MYSQL_IMAGE = MYSQL_IMAGE_NAME + ":" + MYSQL_IMAGE_TAG;
        public static final int MYSQL_CONTAINER_DEFAULT_PORT = 3310;
        /**
         * Redis docker image     的名称、版本号
         * docker container 的默认端口
         */
        public static final String REDIS_IMAGE_NAME = "redis";
        public static final String REDIS_IMAGE_TAG = "7.0";
        public static final String REDIS_IMAGE = REDIS_IMAGE_NAME + ":" + REDIS_IMAGE_TAG;
        public static final int REDIS_CONTAINER_DEFAULT_PORT = 6379;
        /**
         * OceanBase docker image
         * oceanbase-ce 的默认端口
         * warning: 在docker中启动OceanBase-ce需要至少8G以上的内存
         */
        public static final String OCEANBASE_IMAGE_NAME = "oceanbase/oceanbase-ce";
        public static final String OCEANBASE_IMAGE_TAG = "4.0.0.0";
        public static final String OCEANBASE_IMAGE = OCEANBASE_IMAGE_NAME + ":" + OCEANBASE_IMAGE_TAG;
        public static final int OCEANBASE_CONTAINER_DEFAULT_PORT = 2881;
        public static final String OCEANBASE = "oceanbase";

        /**
         * openGauss docker image(官方镜像用不了)
         * openGauss默认端口
         */
        public static final String OPENGAUSS_IMAGE_NAME = "enmotech/opengauss";
        public static final String OPENGAUSS_IMAGE_TAG = "latest";
        public static final String OPENGAUSS_IMAGE = OPENGAUSS_IMAGE_NAME + ":" + OPENGAUSS_IMAGE_TAG;
        public static final int OPENGAUSS_CONTAINER_DEFAULT_PORT = 5432;
        public static final String OPENGAUSS = "openGauss";
    }

    /**
     * 返回给前端的提示信息
     */
    public static class Message {
        /**
         * 验证码校验相关
         */
        public static final String CODE_EXPIRED = "验证码已过期";
        public static final String CODE_WRONG = "验证码错误";
        /**
         * 密码校验相关
         */
        public static final String PASSWORD_CONFIRMATION_WRONG = "两次输入的密码不一致";
        public static final String EMAIL_OR_PASSWORD_WRONG = "邮箱或密码错误";
        /**
         * 邮箱校验相关
         */
        public static final String EMAIL_EXISTED = "该邮箱已注册";
        public static final String EMAIL_NOT_EXISTED = "该邮箱未注册";
        /**
         * 成功失败相关
         */
        public static final String SIGNUP_SUCCEED = "注册成功";
        public static final String MODIFY_USER_INFO_SUCCEED = "修改个人信息成功";
        public static final String MODIFY_USER_INFO_FAILED = "修改个人信息失败";
        public static final String RESET_PASSWORD_SUCCEED = "重置密码成功";
        public static final String RESET_PASSWORD_FAILED = "重置密码失败";
        public static final String CODE_SEND_SUCCEED = "发送成功";
        public static final String SAVE_DRAFT_SUCCEED = "保存成功";
        public static final String STAR_SUCCEED = "已收藏";
        public static final String UNSTAR_SUCCEED = "取消收藏";
        /**
         * SQL运行相关
         */
        public static final String PASSED = "Passed";   // SQL运行通过
        public static final String NOT_PASSED = "Didn't pass";   // SQL运行未通过
    }

    /**
     * Question标签相关
     */
    public static class QuestionTag {
        // 标签总数
        public static final int TOTAL_TAG_NUM = 11;

        public static final String DATE_AND_TIME = "时间和日期";
        public static final String STRING = "字符串";
        public static final String NUMBER = "数值";
        public static final String SET = "集合";
        public static final String AGGREGATE = "聚合函数";
        public static final String LIKE_QUERY = "模糊查询";
        public static final String ORDER = "排序";
        public static final String GROUP = "分组";
        public static final String MULTI_TABLE_CONNECTION = "多表连接";
        public static final String SUB_QUERY = "子查询";
        public static final String CONNECTION_JUDGE = "条件判断";

        public static final String UNDEFINE_TAG = "未知标签";

        /**
         * 把数据库中存储的标签类型编号转换为标签名称
         */
        public static String tagTypeToName(int tagType) {
            switch (tagType) {
                case 0: return DATE_AND_TIME;
                case 1: return STRING;
                case 2: return NUMBER;
                case 3: return SET;
                case 4: return AGGREGATE;
                case 5: return LIKE_QUERY;
                case 6: return ORDER;
                case 7: return GROUP;
                case 8: return MULTI_TABLE_CONNECTION;
                case 9: return SUB_QUERY;
                case 10: return CONNECTION_JUDGE;
                default: return UNDEFINE_TAG;
            }
        }

        /**
         * 标签名称转换为对应类型编号
         */
        public static int tagNameToType(String tagName) {
            switch (tagName) {
                case DATE_AND_TIME: return 0;
                case STRING: return 1;
                case NUMBER: return 2;
                case SET: return 3;
                case AGGREGATE: return 4;
                case LIKE_QUERY: return 5;
                case ORDER: return 6;
                case GROUP: return 7;
                case MULTI_TABLE_CONNECTION: return 8;
                case SUB_QUERY: return 9;
                case CONNECTION_JUDGE: return 10;
                default: return -1;
            }
        }

        /**
         * 将标签类型编号列表转换为标签名称列表
         */
        public static List<String> tagTypeListToNameList(List<Integer> tagTypes) {
            List<String> tagNames = new ArrayList<>();
            for (Integer tagType : tagTypes) {
                tagNames.add(tagTypeToName(tagType));
            }
            return tagNames;
        }

        /**
         * 将标签名称列表转换为标签类型编号列表
         */
        public static List<Integer> tagNameListToTypeList(List<String> tagNames) {
            List<Integer> tagTypes = new ArrayList<>();
            for (String tagName : tagNames) {
                tagTypes.add(tagNameToType(tagName));
            }
            return tagTypes;
        }
    }

}
