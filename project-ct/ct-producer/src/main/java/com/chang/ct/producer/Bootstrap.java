package com.chang.ct.producer;

import com.chang.ct.producer.bean.LocalFileProducer;
import com.chang.ct.producer.io.LogFileDataIn;
import com.chang.ct.producer.io.LogFileDataOut;

public class Bootstrap {
    public static void main(String[] args) {

         if ( args.length < 2 ) {
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar Produce.jar path1 path2 ");
            System.exit(1);
        }

//        // 构建生产者对象
//        LogFileDataIn logFileDataIn = new LogFileDataIn("D:\\ideaPrjects\\project-ct\\ct-producer\\src\\main\\resources\\contact.log");
//        LogFileDataOut logFileDataOut = new LogFileDataOut("D:\\ideaPrjects\\project-ct\\ct-producer\\src\\main\\resources\\call.log");
        LogFileDataIn logFileDataIn = new LogFileDataIn(args[0]);
        LogFileDataOut logFileDataOut = new LogFileDataOut(args[1]);


        LocalFileProducer producer = new LocalFileProducer(logFileDataIn, logFileDataOut);


        // 生产数据
        producer.producer();


        // 关闭生产者对象
        producer.close();

    }
}
