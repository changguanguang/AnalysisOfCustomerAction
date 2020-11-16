package com.chang.ct.web.dao;


import com.chang.ct.web.bean.Calllog;

import java.util.HashMap;
import java.util.List;

/**
 *
 *  与数据库进行通信规范
 */


public interface CalllogDao {
    List<Calllog> queryMonthDatas(HashMap<String, String> map);
}
