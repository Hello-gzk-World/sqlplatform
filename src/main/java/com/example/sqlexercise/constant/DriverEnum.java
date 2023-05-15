package com.example.sqlexercise.constant;

import com.example.sqlexercise.driver.Client;
import com.example.sqlexercise.driver.JDBC.MysqlClient;
import com.example.sqlexercise.driver.JDBC.OceanbaseClient;
import com.example.sqlexercise.driver.JDBC.OpenGaussClient;

public enum DriverEnum {
    MySQL("mysql",3306, new MysqlClient()),

    OceanBase("oceanbase",2881, new OceanbaseClient()),

    OpenGauss("openGauss",5432, new OpenGaussClient());

    private String name;
    private int port;
    private Client client;

    DriverEnum(String name, int port, Client client){
        this.name=name;
        this.port=port;
        this.client=client;
    }

    public static DriverEnum of(String name) {
        for (DriverEnum driverEnum : DriverEnum.values()) {
            if (driverEnum.getName() .equals(name)) {
                return driverEnum;
            }
        }
        throw new RuntimeException("No such driver");
    }

    public String getName(){
        return this.name;
    }

    public int getPort(){
        return this.port;
    }

    public Client getClinet(){
        return this.client;
    }

    void testEnum() {
        DriverEnum driverEnum = DriverEnum.of(MySQL.getName());
        // 单例
        Client clinet = driverEnum.getClinet();
        Client clinet2 = MySQL.getClinet();
        System.out.println();;
    }


}
