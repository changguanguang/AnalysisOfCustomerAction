package com.atguigu.ct.cache;


import com.chang.ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *  启动缓存客户端，向redis中增加缓存数据
 */
public class Bootstrap {

    public static void main(String[] args) {

        // @todo 从mysql中读取用户数据&时间数据到jvm堆中
        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();

        // 读取用户，时间数据

        Connection connection = null;
        PreparedStatement pstat = null;
        ResultSet rs = null;

        try {

            connection = JDBCUtil.getConnection();

            String queryUserSql = "select id, tel from ct_user";
            pstat = connection.prepareStatement(queryUserSql);
            rs = pstat.executeQuery();
            while ( rs.next() ) {
                Integer id = rs.getInt(1);
                String tel = rs.getString(2);
                userMap.put(tel, id);
            }

            rs.close();

            String queryDateSql = "select id, year, month, day from ct_date";
            pstat = connection.prepareStatement(queryDateSql);
            rs = pstat.executeQuery();
            while ( rs.next() ) {
                Integer id = rs.getInt(1);
                String year = rs.getString(2);
                String month = rs.getString(3);
                if ( month.length() == 1 ) {
                    month = "0" + month;
                }
                String day = rs.getString(4);
                if ( day.length() == 1 ) {
                    day = "0" + day;
                }
                dateMap.put(year + month + day, id);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if ( rs != null ) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if ( pstat != null ) {
                try {
                    pstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if ( connection != null ) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


        // @todo 将jvm堆中数据存储到redis缓存中


        Jedis jedis = new Jedis("hadoop101", 6379);

        // userMap 转移
        Iterator<String> keyIterator = userMap.keySet().iterator();
        while(keyIterator.hasNext()){
            String next = keyIterator.next();
            Integer value = userMap.get(next);
            jedis.hset("ct_user",next,""+value);
        }

        // dateMap 转移
        keyIterator = dateMap.keySet().iterator();
        while(keyIterator.hasNext()){
            String next = keyIterator.next();
            Integer value = dateMap.get(next);
            jedis.hset("ct_date",next,""+value);
        }


    }
}
