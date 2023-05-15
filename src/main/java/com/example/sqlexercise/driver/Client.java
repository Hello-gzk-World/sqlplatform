package com.example.sqlexercise.driver;

import com.example.sqlexercise.lib.ResultOfTask;
import com.example.sqlexercise.lib.SqlDatabaseConfig;

public interface Client {
    void init(SqlDatabaseConfig config);

    void destroy();

    ResultOfTask runQuery(String query);

    String getSchemaSql(String database);

    String initSchemaSql(String database);

    String cleanSchemaSql(String database);

    boolean createUser(String sqlText);

    void createTable(String sqlText);
}
