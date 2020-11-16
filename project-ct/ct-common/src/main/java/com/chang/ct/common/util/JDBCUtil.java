package com.chang.ct.common.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class JDBCUtil {

    private static final String MYSQL_DRIVER_CALSS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://hadoop101:3306/ct";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "000000";


    public static Connection getConnection()  {
        Connection conn = null;
        try {

            Class.forName(MYSQL_DRIVER_CALSS);
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);

            if (conn != null) {
                System.err.println("成功连接到mysql");
                System.out.println("成功连接到mysql");


            } else {
                System.err.println("连接失败");
                System.out.println("连接失败");
                throw new Exception("连接失败");
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return conn;
    }
}
