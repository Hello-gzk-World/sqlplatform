package com.example.sqlexercise.lib;

import com.alibaba.fastjson.JSONObject;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ListImagesCmd;
import com.github.dockerjava.api.command.LoadImageCmd;
import com.github.dockerjava.api.command.PullImageCmd;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;


public class DockerUtils {

    public DockerClient connectDocker(String protocol, String host, int port) {
        DockerClient dockerClient = DockerClientBuilder.getInstance(protocol + "://" + host + ":" + port).build();
        //Info info = dockerClient.infoCmd().exec();
        //String infoStr = JSONObject.toJSONString(info);
        System.out.println("docker已连接！");
        //System.out.println(infoStr);
        return dockerClient;
    }

    public CreateContainerResponse createContainers(DockerClient client, String containerName, String imageName) {
        //映射端口8080->80
        ExposedPort tcp80 = ExposedPort.tcp(80);
        Ports portBindings = new Ports();
        portBindings.bind(tcp80, Ports.Binding.bindPort(8088));
        CreateContainerResponse containerResponse = client.createContainerCmd(imageName).withName(containerName).
                withHostConfig(new HostConfig()).
                withPortBindings(portBindings).
                withExposedPorts(tcp80).exec();
        return containerResponse;
    }

    public LoadImageCmd loadImage(DockerClient client, String filePath) {
        LoadImageCmd loadImageCmd = null;
        try {
            loadImageCmd = client.loadImageCmd(new FileInputStream(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return loadImageCmd;
    }

    public PullImageCmd pullImage(DockerClient client, String repository) {
        PullImageCmd pullImageCmd = client.pullImageCmd(repository);
        return pullImageCmd;
    }

    public ListImagesCmd listImages(DockerClient client) {
        ListImagesCmd listImagesCmd = client.listImagesCmd();
        listImagesCmd.withShowAll(true).exec();
        return listImagesCmd;
    }

    public void removeImage(DockerClient client, String imageId) {
        client.removeImageCmd(imageId).exec();
    }

    //containerId 也可以是containerName
    public void removeContainer(DockerClient client, String containerId) {
        client.removeContainerCmd(containerId).exec();
    }

    //containerId 也可以是containerName
    public void startContainer(DockerClient client, String containerId) {
        client.startContainerCmd(containerId).exec();
    }

    //containerId 也可以是containerName
    public void stopContainer(DockerClient client, String containerId) {
        client.stopContainerCmd(containerId).exec();
    }

    public static void main(String[] args) {
        DockerUtils dockerUtils = new DockerUtils();
        DockerClient client = dockerUtils.connectDocker("tcp", "124.71.132.75", 2375);
        dockerUtils.startContainer(client, "opengauss-test");

        System.out.println("successfully start container!");
    }
    /*
    //使用路径挂载创建容器
    //不能直接使用Volumes
    public CreateContainerResponse createContainerResponse(DockerClient client, String containerName, String imageName){
        HostConfig hostConfig = new HostConfig();
        Bind bind = new Bind("服务器路径", new Volume("容器路径"));
        hostConfig.setBinds(bind);
        CreateContainerResponse containerResponse = client.createContainerCmd(imageName).
                withName(containerName).
                withHostConfig(hostConfig).exec();
        return containerResponse;
    }
    */

    /*
    创建容器并执行命令
    public CreateContainerResponse createContainers(DockerClient client,String containerName,String imageName){
        HostConfig hostConfig = new HostConfig();
        CreateContainerResponse container = client.createContainerCmd(imageName)
                .withName(containerName)
                .withHostConfig(hostConfig)
                .withCmd("python","/root/scripts/test.py")
                .exec();
        return container;
    }
    */

    /*
    执行命令时带参数
    public CreateContainerResponse createContainers(DockerClient client,String containerName,String imageName){
        HostConfig hostConfig = newHostConfig();
        CreateContainerResponse container = client.createContainerCmd(imageName)
                .withName(containerName)
                .withHostConfig(hostConfig)
                // 注意命令和参数不能进行组合，必须都用逗号隔开,也就是空格全部换成这里的,分割
                .withCmd("python","/root/scripts/test.py","-t","999")
                .exec();
        return container;
    }
     */

}
