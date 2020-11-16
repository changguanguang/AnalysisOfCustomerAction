package com.chang.ct.common;

import com.chang.ct.common.util.JDBCUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

public class Test {
    public static void main(String[] args) throws  Exception {


        Connection connection = JDBCUtil.getConnection();
        PreparedStatement pstat = null;
        ResultSet rs = null;
        HashMap<String, Integer> userMap = new HashMap<String, Integer>();
        // 从mysql中读取需要的数据，放入到缓存中
        String queryUserSql = "select id, tel from ct_user";
        pstat = connection.prepareStatement(queryUserSql);
        rs = pstat.executeQuery();
        // Moves the cursor(游标) forward one row from its current position.
        while(rs.next()){
//               @param columnIndex the first column is 1, the second is 2, ...
            Integer id = rs.getInt(1);
            String tel = rs.getString(2);
            userMap.put(tel,id);

        }
        rs.close();

        for (String s : userMap.keySet()) {

            System.out.println("key = " +s + "   value = " + userMap.get(s));
        }
        System.out.println(userMap.size());

        String call1 = "13319935953";
        System.out.println(userMap.get(call1));

    }
}
