package com.chang.ct.web.controller;

import com.chang.ct.web.bean.Calllog;
import com.chang.ct.web.dao.CalllogDao;
import com.chang.ct.web.service.CalllogService;
import com.chang.ct.web.service.imlp.CalllogServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 *  控制器：控制页面流转
 *
 */
@Controller
public class CalllogController {
    // 自动填装
    @Autowired
    private CalllogService calllogService;

    // 映射request
    @RequestMapping("/query")
    public String query(){
        return "query";

    }

//    @ResponseBody
    @RequestMapping("/view")
    public String getObject(String tel, String calltime, Model model){

        // 查询统计结果

        System.out.println("tel = "+tel + "calltime = "+ calltime);
        List<Calllog> logs = calllogService.queryMonthDatas(tel,calltime);
        model.addAttribute("calllogs",logs);

        return "view";
    }

}
