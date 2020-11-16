package com.chang.ct.common.bean;

import java.util.List;

public interface DataIn {

    // 设置路径
    public void setPath(String path);

    // 读取数据
    public Object read();

    // 读取数据并返回集合
    public  <T extends Data> List<T> read(Class<T> clazz);

    public void close();

}
