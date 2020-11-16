package com.chang.ct.web.service.imlp;

import com.chang.ct.web.bean.Calllog;
import com.chang.ct.web.dao.CalllogDao;
import com.chang.ct.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;


@Service
public class CalllogServiceImpl implements CalllogService {

    @Autowired
    private CalllogDao calllogDao;

    /*
     * 查询指定用户的通话统计信息
     */
    @Override
    public List<Calllog> queryMonthDatas(String tel, String calltime) {

        HashMap<String, String> map = new HashMap<>();

        System.out.println("calltime = " + calltime);
        if(calltime == null) {
            calltime = "2020";
        }
        if(calltime.length() > 4){
            calltime = calltime.substring(0,4);
        }
        map.put("tel",tel);
        map.put("calltime",calltime);

        for (String s : map.keySet()) {

            System.out.println("key = " + s + " value = " + map.get(s) );
        }

        return calllogDao.queryMonthDatas(map);
    }
}
