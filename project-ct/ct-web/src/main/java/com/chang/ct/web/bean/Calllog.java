package com.chang.ct.web.bean;

public class Calllog {

    private int tellid ;
    private int dateid ;
    private String sumcall ;
    private String sumduration;

    public int getTellid() {
        return tellid;
    }

    public void setTellid(int tellid) {
        this.tellid = tellid;
    }

    public int getDateid() {
        return dateid;
    }

    public void setDateid(int dateid) {
        this.dateid = dateid;
    }

    public String getSumcall() {
        return sumcall;
    }

    public void setSumcall(String sumcall) {
        this.sumcall = sumcall;
    }

    public String getSumduration() {
        return sumduration;
    }

    public void setSumduration(String sumduration) {
        this.sumduration = sumduration;
    }
}
