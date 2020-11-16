package kv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisValue implements Writable {

    private String sumCall;
    private String sumDuration;

    public AnalysisValue() {
    }

    public AnalysisValue(String sumCall, String sumDuration) {
        this.sumCall = sumCall;
        this.sumDuration = sumDuration;
    }

    public String getSumCall() {
        return sumCall;
    }

    public void setSumCall(String sumCall) {
        this.sumCall = sumCall;
    }

    public String getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(String sumDuration) {
        this.sumDuration = sumDuration;
    }

    //  in sequence
    // 所以写出与读取要按照顺序进行
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.sumCall);
        out.writeUTF(this.sumDuration);

    }

    public void readFields(DataInput in) throws IOException {

        this.sumCall = in.readUTF();
        this.sumDuration = in.readUTF();
    }
}
