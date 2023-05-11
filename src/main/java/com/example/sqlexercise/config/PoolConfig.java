package com.example.sqlexercise.config;

import com.example.sqlexercise.lib.SqlDatabasePool;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collections;

@Configuration
public class PoolConfig {

    @Bean
    SqlDatabasePool sqlDatabasePool(){
        ArrayList<String> drivers = new ArrayList<>(Collections.singleton("mysql"));
        drivers.add("oceanbase");
        drivers.add("openGauss");
        return new SqlDatabasePool(new DockerConfig(), drivers);
    }
}
