package com.chang.ct.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {


    // 将日期字符串按照指定格式解析为日期对象
    public static Date parse(String dateString, String format) {

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {


        }
        return date;

    }

    // 将日期按照指定格式转化为字符串
    public static String format(Date date, String format) {

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }


}
