package com.example.sqlexercise;

import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.lib.*;
import com.example.sqlexercise.serviceImpl.MyAsyncService;
import com.fasterxml.uuid.Generators;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Image;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Slf4j(topic = "com.example.sqlexercise.CreateEnvForSqlExercise")
@Component
public class CreateEnvForSqlExercise implements ApplicationRunner {

    private final SqlDatabasePool pool;
    private final MyAsyncService myAsyncService;
    private final String NAMESPACE_URL = "6ba7b811-9dad-11d1-80b4-00c04fd430c8";
    private final String SALT = "sqlexercise";

    @Autowired
    public CreateEnvForSqlExercise(SqlDatabasePool pool, MyAsyncService myAsyncService) {
        this.pool = pool;
        this.myAsyncService = myAsyncService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("ApplicationRunner接口" );
        /*
        SqlDatabaseConfig config = new SqlDatabaseConfig();
        config.tags = new HashMap<>();
        config.tags.put("schemaName", "test");
        config.tags.put("driver", "mysql");
        config.tags.put("server", "1");
        config.tags.put("index", 1);
        SqlDatabase sqlDatabase = new SqlDatabase(config);
        sqlDatabase.testConnect();
        */
        //检查大题数据文件夹，里面包含schema的建表语句
        if (!System.getProperty("os.name").startsWith("Linux")) {
            // 若不是Linux系统，则说明是在本地开发环境中，即Windows或MacOS，所以在resources文件夹下读取
            try {
                Path dbPath = Paths.get("src/main/resources/examDataFiles");
                if (!Files.exists(dbPath)) {    // 不存在则创建该文件夹
                    Files.createDirectory(dbPath);
                } else {
                    log.info("examDataFiles exists.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 若是Linux系统，则说明以jar包形式部署到了生产环境中
            // do nothing, 因为会确保部署时所需文件被一并上传
        }
        // 配置远端服务器的Docker，初始化sql的运行环境
        createEnv(false);
        // Container mysql-1-0 uses password 77d3bc2d-d446-5b79-b64e-95cfa00dfd5c
        // Container mysql-1-1 uses password 33b60f2b-1dbb-5fe0-81a0-98252b6414bb
        // Container redis-1-0 uses password d5a32b3e-c535-58ba-aa30-57a677fbee74
    }

    /**
     * 创建运行所需的docker容器
     *
     * @param recreate 是否要重新创建容器
     */
    private void createEnv(boolean recreate) throws Exception {
        ArrayList<DockerServer> dockerServerArrayList = this.pool.getDockerServers();
        for (DockerServer dockerServer : dockerServerArrayList) {
            SqlServer server = new SqlServer(dockerServer.getHost());
            dockerServer.connectDocker();
            // 检查 MySQL 镜像
            Image image = dockerServer.getDockerImageByReference(Constants.DockerRelated.MYSQL_IMAGE);
            if (image == null) {
                dockerServer.pullImageByRepository(Constants.DockerRelated.MYSQL_IMAGE_NAME, Constants.DockerRelated.MYSQL_IMAGE_TAG);
            }
            log.info("After checking, image " + Constants.DockerRelated.MYSQL_IMAGE + " exists.");
            image = dockerServer.getDockerImageByReference(Constants.DockerRelated.OCEANBASE_IMAGE);
            if (image == null) {
                dockerServer.pullImageByRepository(Constants.DockerRelated.OCEANBASE_IMAGE_NAME, Constants.DockerRelated.OCEANBASE_IMAGE_TAG);
            }
            log.info("After checking, image " + Constants.DockerRelated.OCEANBASE_IMAGE + " exists.");
            // 检查openGauss镜像
            image = dockerServer.getDockerImageByReference(Constants.DockerRelated.OPENGAUSS_IMAGE);
            if (image == null) {
                dockerServer.pullImageByRepository(Constants.DockerRelated.OPENGAUSS_IMAGE_NAME, Constants.DockerRelated.OPENGAUSS_IMAGE_TAG);
            }
            log.info("After checking, image " + Constants.DockerRelated.OPENGAUSS_IMAGE + " exists.");

            // 若生成容器信息
            UUID namespace = Generators.nameBasedGenerator(UUID.fromString(NAMESPACE_URL)).generate(SALT);
            List<DockerContainer> dockerMysqlContainers =
                    createContainerInfos(dockerServer, namespace, Constants.DockerRelated.MYSQL_IMAGE_NAME,
                            Constants.DockerRelated.MYSQL_CONTAINER_DEFAULT_PORT, 1);
            List<DockerContainer> dockerOceanbaseContainers =
                    createContainerInfos(dockerServer, namespace, Constants.DockerRelated.OCEANBASE,
                            Constants.DockerRelated.OCEANBASE_CONTAINER_DEFAULT_PORT, 1);
            List<DockerContainer> dockerOpenGaussContainers =
                    createContainerInfos(dockerServer, namespace, Constants.DockerRelated.OPENGAUSS,
                            Constants.DockerRelated.OPENGAUSS_CONTAINER_DEFAULT_PORT, 1);
            // 检查dockerServer中现有容器实例情况
//            checkExistingContainerInstance(dockerMysqlContainers, dockerServer, recreate);
//            checkExistingContainerInstance(dockerOceanbaseContainers, dockerServer, recreate);
//            checkExistingContainerInstance(dockerOpenGaussContainers, dockerServer, recreate);
            // 根据容器信息创建容器实例
//            createMysqlContainerInstance(dockerMysqlContainers, dockerServer, server);
//            createOceanbaseContainerInstance(dockerOceanbaseContainers, dockerServer, server);
//            createOpenGaussContainerInstance(dockerOpenGaussContainers, dockerServer, server);
        }
    }

    private List<DockerContainer> createContainerInfos(DockerServer dockerServer, UUID namespace,
                                                       String imageName, int basePort, int num) {
        List<DockerContainer> result = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            String name = imageName + "-" + dockerServer.getId() + "-" + i;
            String password = "GS"+Generators.nameBasedGenerator(namespace).generate(name).toString();
            int port = basePort + i;
            result.add(new DockerContainer(i, name, password, port));
            log.info("Container " + name + " uses password " + password);
        }
        return result;
    }

    private void checkExistingContainerInstance(List<DockerContainer> containerList, DockerServer dockerServer, boolean recreate) {
        // 根据容器信息，为每个容器创建环境
        for (DockerContainer container : containerList) {
            // 获取同名容器
            Container container1 = dockerServer.getDockerContainerByName(container.getName());
            // 同名容器存在，且无需删除，即无需重新创建
            if (container1 != null && !recreate) {
                if (container1.getState().toLowerCase().equals("running")) {
                    log.info("Container " + container.getName() + " already exists and it's running.");
                    continue;
                }
            }
            // 同名容器存在，且需要删除，即需要重新创建
            if (container1 != null) {
                if ((container1.getState() == null && container1.getStatus().toLowerCase().startsWith("up")) ||
                        (container1.getState() != null && container1.getState().toLowerCase().equals("running"))) {
                    dockerServer.stopDockerContainerById(container1.getId());
                }
                dockerServer.removeDockerContainerById(container1.getId());
                log.info("Container " + container.getName() + " has been removed.");
            } else {
                //同名容器不存在
                log.info("Container " + container.getName() + " does not exist.");
            }
        }
    }

    private void createMysqlContainerInstance(List<DockerContainer> containerList, DockerServer dockerServer,
                                              SqlServer server) throws Exception {
        for (DockerContainer container : containerList) {
            // 检查可用端口
            try {
                server.detectServerPort(true, container.getPort(), container.getPort());
            } catch (Exception e) {
                e.printStackTrace();
            }
            dockerServer.createDockerContainerForMysql57(container.getName(), container.getPassword(), container.getPort());
            //以下部分已改成Spring异步执行，不阻塞主线程
            SqlDatabase sqlDatabase =
                    this.pool.getSqlDatabase("", "mysql", dockerServer.getId(), container.getIndex());
            myAsyncService.asyncInitMysqlContainer(dockerServer, container, sqlDatabase);
        }
    }

    private void createOceanbaseContainerInstance(List<DockerContainer> containerList, DockerServer dockerServer,
                                                  SqlServer server) throws Exception {
        for (DockerContainer container : containerList) {
            // 检查可用端口
            try {
                server.detectServerPort(true, container.getPort(), container.getPort());
            }catch (Exception e) {
                e.printStackTrace();
            }
            dockerServer.createDockerContainerForOceanbase(container.getName(), container.getPassword(), container.getPort());
            //使用Spring异步执行，不阻塞主线程
            SqlDatabase sqlDatabase =
                    this.pool.getSqlDatabase("", "oceanbase", dockerServer.getId(), container.getIndex());
            myAsyncService.asyncInitOceanbaseContainer(dockerServer, container, sqlDatabase);
        }
    }

    private void createOpenGaussContainerInstance(List<DockerContainer> containerList, DockerServer dockerServer,
                                              SqlServer server) throws Exception {
        for (DockerContainer container : containerList) {
            // 检查可用端口
            try {
                server.detectServerPort(true, container.getPort(), container.getPort());
            } catch (Exception e) {
                e.printStackTrace();
            }
            dockerServer.createDockerContainerForOpenGauss(container.getName(), container.getPassword(), container.getPort());
            //以下部分已改成Spring异步执行，不阻塞主线程
            SqlDatabase sqlDatabase =
                    this.pool.getSqlDatabase("", "openGauss", dockerServer.getId(), container.getIndex());
            myAsyncService.asyncInitOpenGaussContainer(dockerServer, container, sqlDatabase);
        }
    }


    private void createRedisContainerInstance(List<DockerContainer> containerList, DockerServer dockerServer,
                                              SqlServer server) {
        for (DockerContainer container : containerList) {
            // 检查可用端口
            try {
                server.detectServerPort(true, container.getPort(), container.getPort());
            } catch (Exception e) {
                e.printStackTrace();
            }
            dockerServer.createDockerContainerForRedis(container.getName(), container.getPassword(), container.getPort());
            // 启动 Redis container
            dockerServer.startDockerContainer(container.getName());
        }
    }
//    private void createRedisContainerInstance(List<DockerContainer> containerList, DockerServer dockerServer,
//                                              SqlServer server) {
//        for (DockerContainer container : containerList) {
//            // 检查可用端口
//            try {
//                server.detectServerPort(true, container.getPort(), container.getPort());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            dockerServer.createDockerContainerForRedis(container.getName(), container.getPassword(), container.getPort());
//            // 启动 Redis container
//            dockerServer.startDockerContainer(container.getName());
//        }
//    }

}
