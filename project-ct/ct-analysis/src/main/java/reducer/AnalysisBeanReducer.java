package reducer;

import kv.AnalysisKey;
import kv.AnalysisValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnalysisBeanReducer extends Reducer<AnalysisKey, Text,AnalysisKey,AnalysisValue> {

    @Override
    protected void reduce(AnalysisKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int sumCall = 0;
        int sumDuration = 0;

        for (Text value : values) {

            String string = value.toString();
            sumDuration += Integer.parseInt(string);
            sumCall ++;

        }

        context.write(key,new AnalysisValue(sumCall+"",""+sumDuration));
    }
}
