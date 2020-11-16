package com.chang.ct.common.constant;

import com.chang.ct.common.bean.Val;

public enum  Names implements Val {
    NAMESPACE("ct"),
    TABEL("ct:calllog"),
    CF_CALLER("caller"),
    CF_CALLEE("callee"),
    CF_INFO("info"),
    TOPIC("calllog")
    ;

    private String name;
    private Names(String name){
        this.name = name;
    }


    public void setValue(Object val) {
        this.name = (String)val;
    }

    public String getValue() {

        return this.name;
    }
}
