package com.chang.ct.web.service;


import com.chang.ct.web.bean.Calllog;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 业务处理服务规范
 */

public interface CalllogService {


    List<Calllog> queryMonthDatas(String tel, String calltime);
}
