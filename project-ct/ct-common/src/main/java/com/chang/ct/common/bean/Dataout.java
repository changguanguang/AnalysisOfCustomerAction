package com.chang.ct.common.bean;

import java.io.FileNotFoundException;

public interface Dataout {

    public void setPath(String path) throws FileNotFoundException;

    public void write(String log);

    public void close();
}
