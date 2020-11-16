package com.chang.ct.common.bean;

public interface Producer {

    public void setIn(DataIn in );

    public void setOut(Dataout out);

    public void producer();

    // 关闭生产者
    public void close();
}
