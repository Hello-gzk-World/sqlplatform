package com.example.sqlexercise.constant;

import com.example.sqlexercise.driver.Client;
import com.example.sqlexercise.driver.relational.MysqlClient;
import com.example.sqlexercise.driver.relational.OceanbaseClient;
import com.example.sqlexercise.driver.relational.OpenGaussClient;

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

    public Client getClient(){
        return this.client;
    }

    void testEnum() {
        DriverEnum driverEnum = DriverEnum.of(MySQL.getName());
        // 单例
        Client clinet = driverEnum.getClient();
        Client clinet2 = MySQL.getClient();
        System.out.println();;
    }


}
