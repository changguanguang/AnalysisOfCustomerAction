package com.chang.ct.consumer;

import com.chang.ct.consumer.bean.HbaseConsumer;

public class Bootstrap {

    public static void main(String[] args) throws Exception {
        // 创建消费者

        HbaseConsumer hbaseConsumer = new HbaseConsumer();
        // 消费数据
        hbaseConsumer.consumer();

        // 关闭资源
        hbaseConsumer.close();
    }
}
