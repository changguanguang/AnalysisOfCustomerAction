package tool;

import com.chang.ct.common.constant.Names;
import io.MysqlTextOutputFormat;
import mapper.AnalysisTextMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import reducer.AnalysisTextReducer;

public class AnalysisTextTool implements Tool {
    public int run(String[] args) throws Exception {

        // 配置configuration对象

        //内部维护了一个 private ArrayList<Resource> resources = new ArrayList<Resource>();
        // Resource是Configuration的内部类，是一个KV类型的数据结构
//        Configuration configuration = new Configuration();
//        Configuration conf = HBaseConfiguration.create();

        // 将configuration对象传入到job中去
        Job job = Job.getInstance();

//        JAR = "mapreduce.job.jar";
        // 设置mapreduce.job.jar = AnalysisTextTool.class
        job.setJarByClass(AnalysisTextTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        // mapper
        // 使用 Hbase作为数据输入源
        TableMapReduceUtil.initTableMapperJob(
                Names.TABEL.getValue(),
                scan,
                AnalysisTextMapper.class,
                Text.class,
                Text.class,
                job
        );


        // reducer
        job.setReducerClass(AnalysisTextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // outputFormat
        job.setOutputFormatClass(MysqlTextOutputFormat.class);

        boolean flg = job.waitForCompletion(true);

        if(flg){
            return JobStatus.State.SUCCEEDED.getValue();
        }else{
            return JobStatus.State.FAILED.getValue();
        }

    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
