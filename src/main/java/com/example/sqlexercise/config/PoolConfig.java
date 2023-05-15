package com.example.sqlexercise.config;

import com.example.sqlexercise.constant.DriverEnum;
import com.example.sqlexercise.lib.SqlDatabasePool;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collections;

@Configuration
public class PoolConfig {

    @Bean
    SqlDatabasePool sqlDatabasePool(){
        ArrayList<DriverEnum> drivers = new ArrayList<>(Collections.singleton(DriverEnum.MySQL));
        drivers.add(DriverEnum.OceanBase);
        drivers.add(DriverEnum.OpenGauss);
        return new SqlDatabasePool(new DockerConfig(), drivers);
    }
}
