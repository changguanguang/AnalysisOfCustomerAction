package com.chang.ct.consumer.dao;

import com.chang.ct.common.bean.BaseDao;
import com.chang.ct.common.constant.Names;
import com.chang.ct.common.constant.ValueConstant;
import com.chang.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDao extends BaseDao {

    /**
     *  初始化
     * @throws IOException
     */
    public void init() throws IOException {
        start();

        createNameSpaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABEL.getValue(), ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());

        end();
    }


    /**
     * 插入对象
     * @param log
     */

    public void insertData(Calllog log) throws Exception {
        log.setRowKey(genRegionNum(log.getCall1(), log.getCalltime()) + "_" + log.getCall1() + "_" + log.getCalltime() + "_" + log.getCall2() + "_" + log.getDuration());
        putData(log);
    }

    /**
     * 插入普通数据（字符串）
     * @param value
     */
    public void insertData(String value) throws Exception {
        // 将通话日志保存到Hbase表中

        // 解析字符串
        // 1. 获取通话日志数据
        // Kafka中保存的数据： call1  call2  calltime  duration
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        // 2. 创建并配置数据put对象

        //  设计rowKey
        // rowkey = regionNum + call1 + time + call2 + duration_flg
        String rowKey = genRegionNum(call1,calltime) + "_"
                + call1 + "_"
                + calltime + "_"
                + call2 + "_"
                + duration+"_"
                + "1";

        Put put = new Put(Bytes.toBytes(rowKey));

        // 主叫用户
        // 添加到 caller列族中
        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());

        // put.add(family, columnQualifier, value)
        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(family,Bytes.toBytes("flg"),Bytes.toBytes("1"));

        String calleeRowkey = genRegionNum(call2, calltime) + "_"
                + call2 + "_"
                + calltime + "_"
                + call1 + "_"
                + duration + "_"
                + "0";

        // 被叫用户
        // 添加到callee列族中
        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("flg"), Bytes.toBytes("0"));

        // 3. 保存数据
        List<Put> putList = new ArrayList<Put>();
        putList.add(put);
        putList.add(calleePut);

        System.out.println("ready to putdata!");
        System.out.println("value =" + value+" table name=" + Names.TABEL.getValue());
        putData(Names.TABEL.getValue(),putList);


    }




}
