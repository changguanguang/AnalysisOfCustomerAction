package io;

import com.chang.ct.common.util.JDBCUtil;
import java.sql.Connection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


/**
 * mysql格式化输出对象
 */
public class MysqlTextOutputFormat extends OutputFormat<Text,Text> {
    static  class MysqlRecordWriter extends RecordWriter<Text,Text>{

        private Connection connection = null;
        Map<String,Integer> userMap = new HashMap<String,Integer>();
        Map<String,Integer> dateMap = new HashMap<String,Integer>();

        public MysqlRecordWriter(){
            // 初始化 & 获取资源
            try {
                connection = JDBCUtil.getConnection();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if(connection == null){
                System.err.println("connection is null");
                throw new NullPointerException("writer connection is null!!!!");
            }

            PreparedStatement pstat = null;
            ResultSet rs = null;

            try{

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

                ///////////////////////测试usermap是否初始化成功////////////////////////////
                for (String s : userMap.keySet()) {

                    System.out.println("key = " +s + "   value = " + userMap.get(s));
                }
                System.out.println(userMap.size());
                ////////////////////////////////////////////////////////////////////////////


                String queryDateSql = "select id, year, month, day from ct_date";
                pstat = connection.prepareStatement(queryDateSql);

                rs = pstat.executeQuery();

                while(rs.next()){

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

            }catch(Exception e){
                e.printStackTrace();
            }finally {
                // close the resource
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
            }

        }

        // 从 reduce端接收到的数据
        public void write(Text key, Text value)  {

            // 解析 key value 将其写入到mysql中
            // key:    call1 + "_" + year
            //         19834429314_2013
            //         19834429315_03
            // value:  sumCall + "_" + sumDuration

            String k= key.toString();
            String[] kSplit = k.split("_");
            String call1 = kSplit[0];
            String date = kSplit[1];

            String v= value.toString();
            String[] vSplit = v.split("_");
            String sumCall = vSplit[0];
            String sumDuration = vSplit[1];

            PreparedStatement statement = null;

            try {

                if(connection == null){
                    System.err.println("connection is null");
                    throw new NullPointerException("writer connection is null!!!!");
                }

                String sql = "insert into ct_call(tellid,dateid,sumcall,sumduration) values(?,?,?,?)";
                statement = connection.prepareStatement(sql);

                if(statement == null){
                    System.err.println("statement is null");
                    throw new NullPointerException(" write statement is null!!!!");
                }

                if(userMap.get(call1) != null){

                    statement.setInt(1, userMap.get(call1));
                }{
                    System.err.println("call1 = " + call1  );
                    if(userMap !=  null)
                        System.err.println("userMap.size = " + userMap.size());
                    else{
                        System.err.println("userMap is null!!!! " );
                    }

                    statement.setInt(1, 198);
                }
                if(dateMap.get(date) != null){

                    statement.setInt(2, dateMap.get(date));
                }else{
                    statement.setInt(2, 198);

                }
                statement.setString(3, sumCall);
                statement.setString(4, sumDuration);
                statement.execute();
                System.err.println("insert 成功");
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                // close the resource
                if ( statement != null ) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return  new MysqlRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }


    private FileOutputCommitter committer = null;

    private static Path getOutputPath(JobContext context){

        String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }


    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {

        if(committer == null){

            Path path = getOutputPath(context);
            // 得到committer
            committer = new FileOutputCommitter(path,context);

        }
        if(committer == null){
            throw new NullPointerException("committer is null");
        }
        return committer;

    }
}
