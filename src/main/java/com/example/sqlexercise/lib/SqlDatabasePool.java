package com.example.sqlexercise.lib;

import com.example.sqlexercise.config.DockerConfig;
import com.example.sqlexercise.constant.DriverEnum;
import com.fasterxml.uuid.Generators;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Scope;

import java.util.*;

@Scope("singleton")
public class SqlDatabasePool {

    private int count;
    private ArrayList<DriverEnum> drivers;
    private Map<String, ItemOfSqlDatabaseMap> sqlDatabaseMap;
    private int maxRows;
    private ArrayList<DockerServer> dockerServers;
    private ArrayList<String> servers;  // 存dockerServers中各dockerServer的id

    private final String NAMESPACE_URL = "6ba7b811-9dad-11d1-80b4-00c04fd430c8";

    // 构造函数
    public SqlDatabasePool(DockerConfig dockerConfig, ArrayList<DriverEnum> drivers,
                           Map<String, ItemOfSqlDatabaseMap> sqlDatabaseMap) {
        this.drivers = drivers;
        this.sqlDatabaseMap = sqlDatabaseMap;
        this.dockerServers = dockerConfig.getDockerServers();
        this.maxRows = 500;
        ArrayList<String> servers = new ArrayList<>();
        for (DockerServer dockerServer : dockerServers) {
            servers.add(dockerServer.getId());
        }
        this.servers = servers;
    }

    // 构造函数
    public SqlDatabasePool(DockerConfig dockerConfig, ArrayList<DriverEnum> drivers) {
        this.drivers = drivers;
        this.sqlDatabaseMap = new HashMap<>();
        this.dockerServers = dockerConfig.getDockerServers();
        this.maxRows = 500;
        ArrayList<String> servers = new ArrayList<>();
        for (DockerServer dockerServer : dockerServers) {
            servers.add(dockerServer.getId());
        }
        this.servers = servers;
    }

    /**
     * 构造数据库实例表中某个schema的初始结构
     *
     * @param schemaName
     * @return
     */
    private ItemOfSqlDatabaseMap createItemOfMap(@NotNull String schemaName) {
        //create uuidv5 namespace
        UUID namespace = Generators.nameBasedGenerator(UUID.fromString(NAMESPACE_URL)).generate("sqlexercise");
        ItemOfSqlDatabaseMap inRoot = null;
        if (!schemaName.isEmpty()) {
            inRoot = getItemOfMap("");
        }
        ItemOfSqlDatabaseMap inSchema = new ItemOfSqlDatabaseMap();
        for (DriverEnum driverEnum : this.drivers) {
            Map inDriver = new HashMap<String, ArrayList<SqlDatabase>>();
            for (String server : this.servers) {
                ArrayList<SqlDatabase> inServer = new ArrayList<>();
                for (DockerServer e : this.dockerServers) {
                    if (e.getId().equals(server)) {
                        for (int i = 0; i < e.getContainer(); i++) {
                            SqlDatabaseConfig config = new SqlDatabaseConfig();
                            config.tags = new HashMap<>();
                            config.tags.put("schemaName", schemaName);
                            config.tags.put("driver", driverEnum.getName());
                            config.tags.put("server", server);
                            config.tags.put("index", i);
                            config.host = e.getHost();
                            // todo 重构代码
                            config.port=driverEnum.getPort()+i;
                            if(schemaName.isEmpty()){
                                config.username="root";
                            }
                            else{
                                config.username="sqlexercise";
                                config.password = "X"+Generators.nameBasedGenerator(namespace).generate(driverEnum.getName() + "-" + server + "-" + i).toString();
                            }
                            config.maxRows = this.maxRows;

                            SqlDatabase root = null;
                            if (!schemaName.isEmpty()) {
                                root = inRoot.itemOfSqlDatabaseMap.get(driverEnum.getName()).get(server).get(i);
                            }
                            inServer.add(new SqlDatabase(config, root));
                        }
                        break;
                    }
                }
                inDriver.put(server, inServer);
            }
            inSchema.itemOfSqlDatabaseMap.put(driverEnum.getName(), inDriver);
        }
        this.sqlDatabaseMap.put(schemaName, inSchema);
        return inSchema;
    }

    /**
     * 根据数据库driver类型进行定制化参数设置
     */
    private SqlDatabaseConfig createSqlDatabaseConfig(String driver, String schemaName){
        return null;
    }

    /**
     * 获取数据库实例表中某个schema的结构
     *
     * @param schemaName
     * @return
     */
    private ItemOfSqlDatabaseMap getItemOfMap(String schemaName) {
        // 若不存在某个 Schema 的结构，则先进行构造
        if (!this.sqlDatabaseMap.containsKey(schemaName)) {
            // 此处先赋值为构造的异步任务，方法内部会再自动赋值为构造的结果
            this.sqlDatabaseMap.put(schemaName, createItemOfMap(schemaName));
        }
        // 不论此处是异步任务还是结果，最终都将统一给出异步结果
        return this.sqlDatabaseMap.get(schemaName);
    }

    /**
     * 获取数据库实例
     *
     * @param schemaName
     * @param driver
     * @param server
     * @param index
     * @return
     */
    public SqlDatabase getSqlDatabase(String schemaName, String driver, String server, int index) {
        ItemOfSqlDatabaseMap item = getItemOfMap(schemaName);
        return item.itemOfSqlDatabaseMap.get(driver).get(server).get(index);
    }

    /**
     * 获取数据库实例列表
     *
     * @param schemaName
     * @param driver
     * @return
     */
    public ArrayList<SqlDatabase> getSqlDatabaseList(String schemaName, String driver) {
        ItemOfSqlDatabaseMap item = getItemOfMap(schemaName);
        ArrayList<SqlDatabase> sqlDatabases = new ArrayList<>();
        for (String server : this.servers) {
            sqlDatabases.addAll(item.itemOfSqlDatabaseMap.get(driver).get(server));
        }
        return sqlDatabases;
    }

    /**
     * 从可用数据库实例中选择一个
     * 选择基于Round Robin
     *
     * @param schemaName
     * @param driver
     * @return
     */
    public SqlDatabase pickSqlDatabase(String schemaName, String driver) {
        SqlDatabase sqlDatabase = null;
        ArrayList<SqlDatabase> sqlDatabases = getSqlDatabaseList(schemaName, driver);
        int length = sqlDatabases.size();
        while (sqlDatabase == null || sqlDatabase.lockedQueries > 0) {
            if (sqlDatabase != null) {
                sqlDatabase.unlock();
            }
            sqlDatabase = sqlDatabases.get(this.count % length);
            this.count++;
        }
        return sqlDatabase;
    }

    public ArrayList<DockerServer> getDockerServers() {
        return dockerServers;
    }
}
