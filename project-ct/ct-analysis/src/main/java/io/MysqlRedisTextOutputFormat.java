package io;

import com.chang.ct.common.util.JDBCUtil;
import kv.AnalysisKey;
import kv.AnalysisValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;




public class MysqlRedisTextOutputFormat extends OutputFormat<Text,Text> {
    static  class MysqlRecordWriter extends RecordWriter<Text,Text>{

        private Connection connection = null;
        private Jedis jedis=  null;
        public MysqlRecordWriter(){
            // 初始化 & 获取资源
            connection = JDBCUtil.getConnection();
            jedis = new Jedis("hadoop101",6379);


        }

        public void write(Text key, Text value)  {

            // 解析 key value 将其写入到mysql中
            // key:    call1 + "_" + year
            // value:  sumCall + "_" + sumDuration

            String k= key.toString();
            String v= value.toString();
            String[] kSplit = k.split("_");
            String[] vSplit = v.split("_");
            String call1 = kSplit[0];
            String date = kSplit[1];

            String sumCall = vSplit[0];
            String sumDuration = vSplit[1];

            PreparedStatement statement = null;

            try {

                String sql = "insert into ct_call(tellid,dateid,sumcall,sumduration) values(?,?,?,?)";
                statement = connection.prepareStatement(sql);
                statement.setInt(1, Integer.parseInt(jedis.hget("ct_user",call1)));
                statement.setInt(2, Integer.parseInt(jedis.hget("ct_date",date)));
                statement.setString(3, sumCall);
                statement.setString(4, sumDuration);
                statement.execute();
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
            if(jedis != null){
                jedis.close();
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

        String name = context.getConfiguration().get(FileOutputFormat.OUTDIR) ;
        Path path = new Path(name);
        return path;
    }


    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {

        if(committer == null){

            Path path = getOutputPath(context);
            // 得到committer
            committer = new FileOutputCommitter(path,context);

        }
        return committer;

    }
}

