package io;

import com.chang.ct.common.util.JDBCUtil;
import kv.AnalysisKey;
import kv.AnalysisValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *  MYSQL 数据格式化输出对象
 */
public class MysqlBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {


    protected class MysqlRecordWriter extends RecordWriter<AnalysisKey,AnalysisValue>{

        private Connection conn = null;
        Map<String,Integer> userMap = new HashMap<String, Integer>();
        Map<String,Integer> dateMap = new HashMap<String, Integer>();

        // init 初始化format对象，填充field
        public MysqlRecordWriter(){

            // 获取资源
            conn = JDBCUtil.getConnection();
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            // 初始化成员变量
            String queryUserSQl = "select id,tel from ct_user";

            try {

                statement = conn.prepareStatement(queryUserSQl);
                resultSet = statement.executeQuery(queryUserSQl);

                while (resultSet.next()) {
                    int userid = resultSet.getInt(1);
                    String tel = resultSet.getString(2);
                    userMap.put(tel, userid);
                }
                resultSet.close();

                String queryDateSql = "select id,year,month,day from ct_date";
                PreparedStatement preparedStatement = conn.prepareStatement(queryDateSql);
                resultSet = preparedStatement.executeQuery(queryDateSql);

                // result.next()  里面有游标的概念
                // 每次调用next都会向后移动一次游标
                while (resultSet.next()) {
                    // 解析resultSet中的每条记录
                    int id = resultSet.getInt(1);
                    String year = resultSet.getString(2);
                    String month = resultSet.getString(3);
                    String day = resultSet.getString(4);

                    if (month.length() == 1) {
                        month = "0" + month;
                    }
                    if (day.length() == 1) {
                        day = "0" + day;
                    }

                    dateMap.put(year + month + day, id);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally {
                if ( resultSet != null ) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if ( statement != null ) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        /**
         *  写出数据到mysql
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {




            PreparedStatement pstat = null;
            try {
                String insertSQL = "insert into ct_call ( telid, dateid, sumcall, sumduration ) values ( ?, ?, ?, ? )";
                pstat = conn.prepareStatement(insertSQL);

                pstat.setInt(1, userMap.get(key.getTel()));
                pstat.setInt(2, dateMap.get(key.getDate()));
                pstat.setInt(3, Integer.parseInt(value.getSumCall()) );
                pstat.setInt(4, Integer.parseInt(value.getSumDuration()));
                pstat.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if ( pstat != null ) {
                    try {
                        pstat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext context) {

        return new MysqlRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) {

    }


    private OutputCommitter committer = null;


    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {

        if(committer == null){
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output,context);
        }

        return committer;
    }

    private Path getOutputPath(TaskAttemptContext context) {

        String path = context.getConfiguration().get(FileOutputFormat.OUTDIR);
        if(path == null){
            return null;
        }
        return new Path(path);

    }


}
