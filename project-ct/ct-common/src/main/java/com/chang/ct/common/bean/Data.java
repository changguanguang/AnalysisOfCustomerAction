package com.chang.ct.common.bean;

public abstract class Data {

    public String getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = (String) content;
    }

    public String content;

}
