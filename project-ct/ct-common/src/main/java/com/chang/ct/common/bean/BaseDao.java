package com.chang.ct.common.bean;


import com.chang.ct.common.api.Column;
import com.chang.ct.common.api.RowKey;
import com.chang.ct.common.api.TableRef;
import com.chang.ct.common.constant.ValueConstant;
import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

/**
 * 基础的数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws IOException {

        // 于Hbase 进行连接
        getConnection();
        getAdmin();

    }

    protected void end() throws IOException {

        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }

        Connection conn = getConnection();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }

    }


    protected Connection getConnection() throws IOException {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();

            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }


        return conn;
    }

    protected Admin getAdmin() throws IOException {

        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }

        return admin;
    }

    protected void createNameSpaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();
        NamespaceDescriptor namespaceDescriptor = null;
        try {
            // Get a namespace descriptor by name
            namespaceDescriptor = admin.getNamespaceDescriptor(namespace);

        } catch (Exception e) {
//            e.printStackTrace();
            // 获取命名空间描述器
            namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }


    }

    protected void createTableXX(String name, String... families) throws IOException {

        createTableXX(name, null, families);

    }

    protected void createTableXX(String name, Integer regionCount, String... families) throws IOException {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        if (admin.tableExists(tableName)) {
            deleteTable(name);
        }
        createTable(name, regionCount, families);
    }


    private void createTable(String name, Integer regionCount, String... families) throws IOException {

        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);


        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        if (families == null || families.length == 0) {
            families = new String[1];
//            families[0] =
        }

        for (String family : families) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        if (regionCount == null || regionCount <= 1) {
            admin.createTable(hTableDescriptor);
        } else {
            // 根据regionCount数量生成分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(hTableDescriptor, splitKeys);
        }


    }


    private void deleteTable(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 生成分区键
     *
     * @param regionCount
     * @return
     */
    private byte[][] genSplitKeys(Integer regionCount) {

        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];

//        List<byte[]> bsList = ne 需要排序吗？
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            bs[i] = Bytes.toBytes(splitKey);
        }
        return bs;

    }

    // 这里的目的与业务意义是：
    //      将同一手机号在同一年月打的电话放到同一个分区中

    /**
     *  计算分区号
     * @param tel
     * @param date
     * @return
     */
    protected int genRegionNum(String tel, String date) {
//        String usercode =
        String usercode = tel.substring(tel.length()-4);
        String yearMonth = date.substring(0,6);

        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        // crc校验 异或算法，hash
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;

    }


    /**
     * 向指定表中插入一个对象
     *  需要对对象进行解析，解析每一个字段，将字段与表的column相关联（使用自定义Annotation实现）
     * @param obj
     * @throws Exception
     */
    protected void putData(Object obj) throws Exception {
        // 反射
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        Field[] fs = clazz.getFields();

        String stringRowkey = null;
        // 在对象的所有字段中寻找哪个字段是rowKey
        //      遍历所有字段，看哪个字段被注解rowKey修饰
        for (Field f : fs) {
            RowKey rowKey = f.getAnnotation(RowKey.class);
            if(rowKey != null){
                f.setAccessible(true);
                stringRowkey = (String) f.get(obj);
                break;
            }
        }

        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(stringRowkey));

        // 遍历每一个字段，
        // 从字段注解中知道字段在hbase中对应的列
        // 将其封装到 put对象中
        for( Field f : fs){
            Column column = f.getAnnotation(Column.class);
            if(column != null){

                // setAccessible  压制java 语法警告
                // 在这里压制 private的禁止访问的警告
                f.setAccessible(true);
                String family = column.family();
                String columnName = column.column();
                if(columnName == null || "".equals(columnName)){
                    columnName = f.getName();
                }

                String value = (String)f.get(obj);
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(columnName),Bytes.toBytes(value));
            }
        }

        // 调用原始api增加数据
        table.put(put);

        // 关闭表
        table.close();

    }

    /**
     * 向指定的表中插入多条数据
     * @param name
     * @param puts
     * @throws Exception
     */
    protected void putData(String name, List<Put> puts) throws Exception{
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        table.put(puts);
        System.out.println("put data success");

        table.close();
    }

    /**
     * 向指定的表中插入一条数据
     * @param name
     * @param put
     * @throws Exception
     */
    protected void putData(String name,Put put) throws Exception{
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));

        table.put(put);
        table.close();
    }


}
