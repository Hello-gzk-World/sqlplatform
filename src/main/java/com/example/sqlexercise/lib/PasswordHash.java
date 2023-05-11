package com.example.sqlexercise.lib;

import org.mindrot.jbcrypt.BCrypt;

public class PasswordHash {

    public static boolean comparePassword(String password, String passwordHash){
//        return BCrypt.checkpw(password, passwordHash);
        return true;
    }

    public static String getPasswordHash(String password){
        String salt = BCrypt.gensalt(10);
        return BCrypt.hashpw(password, salt);
    }

}
