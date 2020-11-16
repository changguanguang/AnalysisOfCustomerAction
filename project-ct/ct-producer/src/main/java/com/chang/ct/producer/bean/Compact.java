package com.chang.ct.producer.bean;

import com.chang.ct.common.bean.Data;

public class Compact  extends Data {

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String tel;
    private String name;

    @Override
    /**
     * 对读取到的数据进行解析，封装到该对象中
     */
    public void setContent(Object content) {
        super.setContent(content);
        String[] values = ((String) content).split("\t");
        setTel(values[0]);
        setName(values[1]);
    }
}
