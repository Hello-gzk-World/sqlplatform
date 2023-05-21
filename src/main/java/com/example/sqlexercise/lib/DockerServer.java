package com.example.sqlexercise.lib;

import com.example.sqlexercise.constant.Constants;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j(topic = "com.example.sqlexercise.lib.DockerServer")
@Getter
public class DockerServer {
    private String id;
    private String host;
    private int port;
    private String protocol;
    private String certPath;
    private int container;
    private DockerClient client;

    public DockerServer(String id, String host, int port, String protocol, String certPath, int container) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.certPath = certPath;
        this.container = container;
        this.client = null;
    }

    /**
     * 连接docker
     */
    public void connectDocker() {
        this.client = DockerClientBuilder.getInstance(this.protocol + "://" + this.host + ":" + this.port).build();
        log.info("Docker " + this.host + " 已连接！");
    }

    /**
     * 遍历本地image list，根据reference查找指定image
     * @param reference image name. e.g. mysql
     */
    public Image getDockerImageByReference(String reference) throws NullPointerException {
        if (this.client == null) {
            throw new NullPointerException("还未连接Docker");
        }
        // 获取本地image list
        List<Image> images = this.client.listImagesCmd().exec();
        for (Image image : images) {
            for (String tag : image.getRepoTags()) {
                if (tag.equals(reference)) {
                    return image;
                }
            }
        }
        return null;
    }

    /**
     * 拉取指定image
     * @param repository e.g. mysql/redis
     * @param tag e.g. 5.7/7.0
     */
    public void pullImageByRepository(String repository, String tag) {
        try {
            log.info("Please wait! Docker is starting to pull image " + repository + ":" + tag);
            this.client.pullImageCmd(repository).withTag(tag).start().awaitCompletion();
            log.info("Image " + repository + ":" + tag + " is pulled successfully.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Container getDockerContainerByName(String name) {
        List<Container> containers = this.client.listContainersCmd().withShowAll(true).exec();
        for (Container container : containers) {
            if (container.getNames()[0].equals("/" + name)) {
                return container;
            }
        }
        return null;
    }

    /**
     * 根据 docker 容器 id 删除容器
     * @param id container id
     */
    public void removeDockerContainerById(String id) {
        this.client.removeContainerCmd(id).exec();
    }

    /**
     * 创建 MySQL docker container
     * @param name 容器名
     * @param password 容器密码
     * @param port 容器端口
     */
    public void createDockerContainerForMysql57(String name, String password, int port) {
        HostConfig hostConfig = new HostConfig();
        PortBinding portBinding = new PortBinding(Ports.Binding.bindPort(port), ExposedPort.tcp(3306));
        hostConfig.withPortBindings(portBinding);
        List<String> cmd = new ArrayList<>();
        cmd.add("--lower_case_table_names=1");
        cmd.add("--max_connections=1024");
        CreateContainerResponse response = this.client.createContainerCmd(Constants.DockerRelated.MYSQL_IMAGE).withName(name)
                .withHostConfig(hostConfig)
                .withEnv("MYSQL_ROOT_PASSWORD=" + password).withCmd(cmd).exec();
        log.info(name + " container " + response.getId() + " is created successfully!");
    }

    /**
     * 创建OceanBase docker container
     */
    public void createDockerContainerForOceanbase(String name, String password, int port) {
        HostConfig hostConfig = new HostConfig();
        PortBinding portBinding = new PortBinding(Ports.Binding.bindPort(port), ExposedPort.tcp(2881));
        hostConfig.withPortBindings(portBinding);
        CreateContainerResponse response = this.client.createContainerCmd(Constants.DockerRelated.OCEANBASE_IMAGE).withName(name)
                .withHostConfig(hostConfig)
                .withExposedPorts(ExposedPort.tcp(2881))
                .withEnv("MINI_MODE=1","OB_ROOT_PASSWORD=" + password)
                .exec();
        //Date: 2023.3.4
        //Author: hewenbing
        //经过测试，目前OB_ROOT_PASSWORD的环境配置并未生效，可能是镜像原因，obce-mini或许可以
        log.info(name + " container " + response.getId() + " is created successfully!");
    }

    /**
     * 创建 OpenGauss docker container
     * @param name
     * @param password
     * @param port
     */
    public void createDockerContainerForOpenGauss(String name, String password, int port) {
        HostConfig hostConfig = new HostConfig();
        PortBinding portBinding = new PortBinding(Ports.Binding.bindPort(port), ExposedPort.tcp(5432));
        hostConfig.withPortBindings(portBinding);
        CreateContainerResponse response = this.client.createContainerCmd(Constants.DockerRelated.OPENGAUSS_IMAGE).withName(name).withPrivileged(true)
                .withHostConfig(hostConfig)
                .withExposedPorts(ExposedPort.tcp(5432))
                .withEnv("GS_USERNAME=" + "root","GS_PASSWORD=" + password).exec();
        log.info(name + " container " + response.getId() + " is created successfully!");
    }

    /**
     * 创建 Redis docker container
     * @param name 容器名
     * @param password 容器密码
     * @param port 容器端口
     */
    public void createDockerContainerForRedis(String name, String password, int port) {
        HostConfig hostConfig = new HostConfig();
        PortBinding portBinding = new PortBinding(Ports.Binding.bindPort(port), ExposedPort.tcp(6379));
        hostConfig.withPortBindings(portBinding);
        List<String> cmd = new ArrayList<>();
//        cmd.add("--requirepass=" + password);
        CreateContainerResponse response = this.client.createContainerCmd(Constants.DockerRelated.REDIS_IMAGE).withName(name)
                .withHostConfig(hostConfig)
                .withCmd(cmd).exec();
        log.info(name + " container " + response.getId() + " is created successfully!");
    }

    public void startDockerContainer(String containerName) {
        //根据源码，该方法没有response，所以无法精准地知道容器内的镜像什么时候启动完成
        this.client.startContainerCmd(containerName).exec();
        log.info("Container " + containerName + " is starting!");
    }

    public void stopDockerContainerById(String Id) {
        this.client.stopContainerCmd(Id).exec();
    }

}
