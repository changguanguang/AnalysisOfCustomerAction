package kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisKey implements WritableComparable<AnalysisKey> {
    private String tel;
    private String date;
    private String tel2;


    public AnalysisKey(){

    }

    public AnalysisKey(String tel, String date) {
        this.tel = tel;
        this.date = date;

    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTel2() {
        return tel2;
    }

    public void setTel2(String tel2) {
        this.tel2 = tel2;
    }

    public int compareTo(AnalysisKey key) {

        int result = tel.compareTo(key.getTel());

        if(result == 0){
            result = date.compareTo(key.getDate());
        }

        return result;
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(tel);
        out.writeUTF(date);


    }

    public void readFields(DataInput in) throws IOException {

        tel = in.readUTF();
        date = in.readUTF();

    }

    @Override
    public int hashCode() {

        return super.hashCode();
    }
}
