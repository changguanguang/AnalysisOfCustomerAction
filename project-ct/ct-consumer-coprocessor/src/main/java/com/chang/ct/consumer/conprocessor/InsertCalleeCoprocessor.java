package com.chang.ct.consumer.conprocessor;


import com.chang.ct.common.bean.BaseDao;
import com.chang.ct.common.constant.Names;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 1. 自定义协处理器 继承或者实现 协处理器规范（specification）
 * 2. 实现interface，只是实现了规范，但是未能于框架发生联系，未能集成到框架中，所以需要让表能找到（知道）hbase
 * 3. 将项目打成jar包发布到hbase中（关联的jar包也需要发布），并且需要分发
 *
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {


    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABEL.getValue()));

        // 获取主叫用户的Rowkey
        String rowKey = Bytes.toString(put.getRow());
        //1_133_2019_144_1010_1
        String[] values = rowKey.split("_");

        CoprocessorDao dao = new CoprocessorDao();
        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flg = values[5];

        if ("1".equals(flg)) {
            // 只有主叫用户保存后才需要触发被叫用户的保存
            String calleeRowKey = dao.getRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";

            Put calleePut = new Put(Bytes.toBytes(calleeRowKey));

            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("flg"), Bytes.toBytes("0"));
            table.put(calleePut);


            // 关闭表
            table.close();

        }
    }


    private class CoprocessorDao extends BaseDao {

        public int getRegionNum(String tel,String time){
            return super.genRegionNum(tel, time);
        }

    }
}
