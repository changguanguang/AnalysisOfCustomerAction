package com.chang.ct.producer.bean;

import com.chang.ct.common.bean.DataIn;
import com.chang.ct.common.bean.Dataout;
import com.chang.ct.common.bean.Producer;
import com.chang.ct.common.util.DateUtil;
import com.chang.ct.common.util.NumberUtil;

import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 *  本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    private DataIn in;
    private Dataout out;
    private  volatile boolean flg = true;

    public LocalFileProducer(DataIn in, Dataout out) {
        this.in = in;
        this.out = out;
    }

    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(Dataout out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    public void producer() {


        // 只允许读一次文件
        List<Compact> compacts = in.read(Compact.class);

        int len = compacts.size();
        while(flg){

            Random random = new Random();
            int call1Index = random.nextInt(len);
            int call2Index = random.nextInt(len);

            while(call2Index == call1Index){
                call2Index = random.nextInt(len);
            }

            // 主叫 被叫
            Compact call1 = compacts.get(call1Index);
            Compact call2 = compacts.get(call2Index);


            String startDate = "20200101000000";
            String endDate =   "20201030000000";

            long startTime = DateUtil.parse(startDate,"yyyyMMddHHmmss").getTime();
            long endTime = DateUtil.parse(endDate,"yyyyMMddHHmmss").getTime();

            // 通话时间
            long callTime = (long) (startTime + (endTime-startTime) * Math.random());
            String callTimeStr = DateUtil.format(new Date(callTime),"yyyyMMddHHmmss");

            // 持续时间 duration
            // 将数字统一化 化位特定的位数
            String duration = NumberUtil.format(new Random().nextInt(30000),4);

            Calllog calllog = new Calllog(call1.getTel(), call2.getTel(), callTimeStr, duration);
            System.out.println(calllog);
            out.write(calllog.toString());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    public void close() {

        if(in != null){
            in.close();
        }
        if(out != null){
            out.close();
        }

    }
}
