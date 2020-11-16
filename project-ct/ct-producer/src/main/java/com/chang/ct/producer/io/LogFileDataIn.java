package com.chang.ct.producer.io;

import com.chang.ct.common.bean.Data;
import com.chang.ct.common.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


// 实现DataIn规范
public class LogFileDataIn implements DataIn {
    private BufferedReader reader = null;

    public LogFileDataIn(String path){
        setPath(path);
    }


    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Object read() {
        return null;
    }

    /**
     *  读取数据，返回数据集合
     * @param clazz 数据的类型
     * @param <T>
     * @return
     */
    public <T extends Data> List<T> read(Class<T> clazz) {


        List<T> list = new ArrayList<T>();
        String line = null;
        try {

            while((line = reader.readLine()) != null) {
                // 使用反射降低耦合，从而实现这个DataIn类可以读取任何对象类型（只要这个class自己实现setContent细节）
                T t = clazz.newInstance();
                // 将解析每行的数据的逻辑封装到bean类中
                t.setContent(line);
                list.add(t);
            }

        }catch (Exception e){
            e.printStackTrace();
        }


        return list;
    }

    public void close() {
        if(reader != null){
            try {
                this.reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
