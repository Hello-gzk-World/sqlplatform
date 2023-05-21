package com.example.sqlexercise.driver.relational;

import com.example.sqlexercise.lib.ResultOfTask;
import com.example.sqlexercise.lib.SqlDatabaseConfig;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.PooledConnection;
import java.sql.*;
import java.util.ArrayList;

@Slf4j(topic = "com.example.sqlexercise.driver.JDBC.MysqlClient")
public class MysqlClient extends AbstractRelationalClient {

    private MysqlConnectionPoolDataSource poolDataSource;


    public static void main(String[] args) {
        String driver = "org.postgresql.Driver";
        String sourceURL = "jdbc:postgresql://124.71.132.75:15432/db_tpcc";
        String userName = "gzk";
        String password = "Secretpassword@123";
        try {
            // 1. 加载驱动程序
            Class.forName(driver);

            // 内部连接池
            final HikariDataSource dataSource = new HikariDataSource();
            dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/");
            dataSource.setUsername("root");
            dataSource.setPassword(password);
            dataSource.setMaximumPoolSize(10);
//            dataSource.addDataSourceProperty("user", "gaussdb");
//            dataSource.addDataSourceProperty("password", password);
//            dataSource.addDataSourceProperty("databaseName","kkk");
            dataSource.setAutoCommit(false);
            Connection conn = dataSource.getConnection();


            // 3. 创建表
            String database="test";
            String initSchemaSql =  "CREATE DATABASE IF NOT EXISTS "+database+";\nUse "+database+";\n";


            String sql = "create table test(id int, name varchar);";
            Statement statement = conn.createStatement();
            statement.execute(sql);

//             4. 插入数据，预编译SQL,减少SQL执行，
            String insertSql = "insert into test values (?, ?)";
            PreparedStatement ps = conn.prepareStatement(insertSql);
            ps.setInt(1, 1111);
            ps.setString(2, "test1111");
            ps.execute();

            // 5. 查询结果集
            String selectSql = "select * from test";
            PreparedStatement psSelect = conn.prepareStatement(selectSql);
            ResultSet rs = psSelect.executeQuery();
            while (rs.next()) {
                System.out.println("id = " + rs.getInt(1));
                System.out.println("name = " + rs.getString(2));
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init(SqlDatabaseConfig config){
        this.poolDataSource = new MysqlConnectionPoolDataSource();
        this.poolDataSource.setServerName(config.host);
        this.poolDataSource.setDatabaseName(config.tags.get("schemaName").toString());
        this.poolDataSource.setPort(config.port);
        this.poolDataSource.setUser(config.username);
        this.poolDataSource.setPassword(config.password);
        try {
            this.poolDataSource.setUseSSL(true);
            this.poolDataSource.setConnectTimeout(5 * 60);
            this.poolDataSource.setAllowMultiQueries(true);
            this.poolDataSource.setInteractiveClient(true);
            this.poolDataSource.setServerTimezone("UTC");
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        this.poolDataSource = null;
    }

    @Override
    public ResultOfTask runQuery(String query) {
        ResultOfTask resultOfTask = new ResultOfTask();
        try {
            PooledConnection connections = poolDataSource.getPooledConnection();
            Connection connection = connections.getConnection();
            Statement statement = connection.createStatement();
            //限制查询时间在2s以内
            statement.setQueryTimeout(2);
            ResultSet rs = statement.executeQuery(query);
            int columnCount = rs.getMetaData().getColumnCount();
            while(rs.next()) {
                ArrayList<String> row = new ArrayList<>();
                for(int i=1;i<=columnCount;i++){
                    row.add(rs.getString(i));
                }
                resultOfTask.sheet.add(row);
            }
            rs.close();
            statement.close();
            connection.close();
        }catch(SQLTimeoutException e){
            e.printStackTrace();
            resultOfTask.error = "SQLTimeoutException!";
        }catch (SQLSyntaxErrorException e){
            e.printStackTrace();
            resultOfTask.error = "SQLSyntaxErrorException!";
        }catch (SQLException e){
            e.printStackTrace();
            resultOfTask.error = "SQLException!";
        }
        return resultOfTask;
    }

    @Override
    public String getSchemaSql(String database){
        String whereSql;
        if(database!=null){
            whereSql = "WHERE t.table_schema = '" + database + "'";
        }else{
            whereSql = "WHERE t.table_schema NOT IN (\n'mysql',\n'performance_schema',\n'information_schema'\n)";
        }
        return "SELECT\nt.table_schema,\nt.table_name,\nc.column_name,\nc.data_type\nFROM\nINFORMATION_SCHEMA.TABLES t\n" +
                "JOIN INFORMATION_SCHEMA.COLUMNS c ON t.table_schema = c.table_schema AND t.table_name = c.table_name\n"+
                whereSql + "\nORDER BY\nt.table_schema,\nt.table_name,\nc.ordinal_position";
    }


    @Override
    public void createTable(String sqlText){
        Connection connection = null;
        Statement statement = null;
        try{
            PooledConnection connections = poolDataSource.getPooledConnection();
            connection = connections.getConnection();
            statement = connection.createStatement();
            statement.execute(sqlText);
            statement.close();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public boolean createUser(String sqlText){
        try {
            PooledConnection connections = poolDataSource.getPooledConnection();
            Connection connection = connections.getConnection();
            Statement statement = connection.createStatement();
            //执行多条语句并返回第一条语句的结果;
            //true表示返回结果集为resultSet
            //false表示返回结果是更新计数或没有结果
            boolean rs = statement.execute(sqlText);
            while(true) {
                if (rs) {
                    log.info("An error occurred when create user");
                    return false;
                } else {
                    //rs为false，且updateCount=-1时,所有结果已取出
                    if(statement.getUpdateCount()==-1){
                        break;
                    }
                    rs = statement.getMoreResults();
                }
            }
            statement.close();
            connection.close();
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
}
