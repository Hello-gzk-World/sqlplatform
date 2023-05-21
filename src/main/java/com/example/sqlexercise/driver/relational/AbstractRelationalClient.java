package com.example.sqlexercise.driver.relational;

/**
 * @program: sqlplatform
 * @description:
 * @author: Mr.K
 * @create: 2023-05-09 01:46
 **/
public abstract class AbstractRelationalClient implements RelationalClient {
    @Override
    public String initSchemaSql(String database){
        return "CREATE DATABASE IF NOT EXISTS "+database+";\nUse "+database+";\n";
    }

    @Override
    public String cleanSchemaSql(String database){
        return "DROP DATABASE IF EXISTS "+database;
    }
}
