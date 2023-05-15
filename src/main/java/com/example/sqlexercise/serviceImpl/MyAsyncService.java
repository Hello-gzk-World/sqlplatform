package com.example.sqlexercise.serviceImpl;

import com.example.sqlexercise.lib.DockerContainer;
import com.example.sqlexercise.lib.DockerServer;
import com.example.sqlexercise.lib.SqlDatabase;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Service
public class MyAsyncService {

    @Async("asyncTaskExecutor")
    public void myAsyncMethod(){
        System.out.println(Thread.currentThread().getName()+"---> enter async method!");
        try{
            Thread.sleep(5000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName()+"---> end of async method!");
    }

    /*
    @Async("asyncTaskExecutor")
    @Scheduled(fixedRate = 2000)
    public void testScheduleTask(){
        try{
            Thread.sleep(6000);
            System.out.println("任务1使用"+Thread.currentThread().getName()+"-"+simpleDateFormat.format(new Date()));
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
    */

    //@Async("asyncTaskExecutor")
    //@Scheduled(cron = "*/2 * * * * ?")
    //public void testAsync(){
    //    try{
    //        Thread.sleep(1000);
    //        System.out.println("任务2使用"+Thread.currentThread().getName()+"-"+simpleDateFormat.format(new Date()));
    //    }catch (InterruptedException e){
    //        e.printStackTrace();
    //    }
    //}

    @Async("asyncTaskExecutor")
    public void asyncInitMysqlContainer(DockerServer dockerServer, DockerContainer container, SqlDatabase sqlDatabase) throws Exception{
        dockerServer.startDockerContainer(container.getName());
        //等待90s以便容器内mysql启动完成初始化
        try {
            TimeUnit.SECONDS.sleep(90);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //连接数据库
        sqlDatabase.connect("SHOW DATABASES;", 5);
        if(!sqlDatabase.isConnected()) {
            throw new Exception("Connection Error");
        }
        sqlDatabase.createUser("CREATE USER 'sqlexercise'@'%' IDENTIFIED BY '"+container.getPassword()+"';\n" +
                "GRANT SELECT ON *.* TO 'sqlexercise'@'%';\n" +
                "FLUSH PRIVILEGES;", 1, "NySQL");
    }

    @Async("asyncTaskExecutor")
    public void asyncInitOceanbaseContainer(DockerServer dockerServer, DockerContainer container, SqlDatabase sqlDatabase) throws Exception{
        dockerServer.startDockerContainer(container.getName());
        //等待5*60s以便容器内oceanbase启动完成初始化
        try {
            TimeUnit.SECONDS.sleep(4*60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sqlDatabase.connect("SHOW DATABASES;", 5);
        if(!sqlDatabase.isConnected()){
            throw new Exception("Connection Error");
        }
        sqlDatabase.createUser("CREATE USER 'sqlexercise'@'%' IDENTIFIED BY '"+container.getPassword()+"';\n" +
                "GRANT SELECT ON *.* TO 'sqlexercise'@'%';\n", 1, "Oceanbase");
    }

    @Async("asyncTaskExecutor")
    public void asyncInitOpenGaussContainer(DockerServer dockerServer, DockerContainer container, SqlDatabase sqlDatabase) throws Exception {
        dockerServer.startDockerContainer(container.getName());
        // 等待5*60秒以便容器内openGauss启动完成初始化
        try {
            TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sqlDatabase.connect("SHOW DATABASES;", 5);
        if(!sqlDatabase.isConnected()){
            throw new Exception("Connection Error");
        }
        sqlDatabase.createUser("CREATE USER 'sqlexercise'@'%' IDENTIFIED BY '"+container.getPassword()+"';\n" +
                "GRANT SELECT ON *.* TO 'sqlexercise'@'%';\n", 1, "OpenGauss");
    }
}
