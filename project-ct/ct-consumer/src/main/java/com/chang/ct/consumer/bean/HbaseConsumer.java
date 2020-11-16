package com.chang.ct.consumer.bean;

import com.chang.ct.common.bean.Consumer;
import com.chang.ct.common.constant.Names;
import com.chang.ct.consumer.dao.HbaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class HbaseConsumer  implements Consumer {
    public void consumer() throws Exception {

        // 创建配置文件,从外部文件中获取配置
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));


        // 创建kafka消费者，从kafka中获取flume采集到的数据
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题
        kafkaConsumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

        // Hbase数据访问对象，以及对象的初始化
        HbaseDao hbaseDao = new HbaseDao();
        hbaseDao.init();

        // 消费数据
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.value());

                // 插入数据
                hbaseDao.insertData(record.value());
            }
        }

    }

    public void close() {

    }
}
