package com.chang.ct.common.util;

import java.text.DecimalFormat;

public class NumberUtil {

    /**
     *  将指定数字转化为特定长度的字符串
     * @param num
     * @param length
     * @return
     */
    public static String format(int num, int length) {

        StringBuilder stringBuilder=  new StringBuilder();
        for(int i=0;i<length;i++){
            stringBuilder.append("0");
        }

        DecimalFormat df = new DecimalFormat(stringBuilder.toString());
        return df.format(num);
    }

    public static void main(String[] args) {
        System.out.println(NumberUtil.format(12, 5));
    }
}
