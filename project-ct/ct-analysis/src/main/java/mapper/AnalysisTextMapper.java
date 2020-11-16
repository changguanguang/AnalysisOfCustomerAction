package mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AnalysisTextMapper extends TableMapper<Text, Text> {
    @Override
    /**
     *  TableMapper 的输入与输出已经在父类封装完成。
     *  只需要mapper的输出kv类型即可。
     */
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowKey = Bytes.toString(key.get());

        //从 rowKey中获取需要的数据
        String[] values = rowKey.split("_");



        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];

        // 两位 01 02 03 04~~~~
        String year = calltime.substring(0, 4);
        String month = calltime.substring(0, 6);
        String date = calltime.substring(0, 8);

        // 主叫用户 -年月日
        context.write(new Text(call1 + "_" + year),new Text(duration));
        context.write(new Text(call1 + "_" + month),new Text(duration));
        // 将数字封装为 Text
        context.write(new Text(call1 + "_" + date), new Text(duration));

        // 被叫用户 - 年
        context.write(new Text(call2 + "_" + year), new Text(duration));
        // 被叫用户 - 月
        context.write(new Text(call2 + "_" + month), new Text(duration));
        // 被叫用户 - 日
        context.write(new Text(call2 + "_" + date), new Text(duration));
    }
}
