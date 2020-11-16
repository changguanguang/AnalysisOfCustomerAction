package com.chang.ct.producer.io;

import com.chang.ct.common.bean.Data;
import com.chang.ct.common.bean.DataIn;
import com.chang.ct.common.bean.Dataout;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 *  本地文件输入
 */
public class LogFileDataOut implements Dataout {


    private PrintWriter writer = null;


    public LogFileDataOut(String path){
        setPath(path);
    }

    public void setPath(String path) {

        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(String log) {

        writer.write(log + "\n");
        writer.flush();

    }

    public void close() {
        if(writer != null){
            writer.close();
        }

    }
}
