package tool;

import com.chang.ct.common.constant.Names;
import io.MysqlBeanOutputFormat;
import kv.AnalysisKey;
import kv.AnalysisValue;
import mapper.AnalysisBeanMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import reducer.AnalysisBeanReducer;

public class AnalysisBeanTool implements Tool {


    public int run(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(AnalysisBeanTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        //mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABEL.getValue(),
                scan,
                AnalysisBeanMapper.class,
                AnalysisKey.class,
                Text.class,
                job
        );

        // reducer
        job.setReducerClass(AnalysisBeanReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        // outputformat
        job.setOutputFormatClass(MysqlBeanOutputFormat.class);

        boolean flg = job.waitForCompletion(true);
        if ( flg ) {
            return JobStatus.State.SUCCEEDED.getValue();
        } else {
            return JobStatus.State.FAILED.getValue();
        }



    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
