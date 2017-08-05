package com.haizhi.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeUtil {

    public static String getTime(long timeStamp) {
        return getSdFormat().format(new Date(timeStamp));
    }

    public static String getCurrentTime() {
        return getDf().format(new Date());
    }

    private static SimpleDateFormat getDateFormat() {
        return new SimpleDateFormat("yyyyMMdd");
    }

    private static SimpleDateFormat getDf() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    private static SimpleDateFormat getSdFormat() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    }

    public static String getBeforeTime(int period) {
        return getDf().format(getOneDay(0 - period));
    }

    public static String getBeforeDate(int period) {
        return getDateFormat().format(getOneDay(0 - period));
    }

    //获得昨日日期
    public static Date getYesterday() {
        return getOneDay(-1);
    }


    public static Date getOneDay(int amount) {
        Date date = new Date();//取时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, amount);//把日期往前减少一天，若想把日期向后推一天则将负数改为正数
        //logger.info("date = {} amount = {}", calendar.getTime(), amount);
        return calendar.getTime();
    }

    public static long getCurrentTimestamp() {
        Date date = new Date();
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        return calendar.getTimeInMillis();
    }

    public static long getStartTimestamp(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    public static long getStartTimestamp(int amount) {
        return getStartTimestamp(getOneDay(amount));
    }

    public static long getBeforeDateStartTimestamp(int period) {
        return getStartTimestamp(getOneDay(0 - period));
    }

    public static long getEndTimestamp(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        return calendar.getTimeInMillis();
    }

    public static long getEndTimestamp(int amount) {
        return getEndTimestamp(getOneDay(amount));
    }

    public static long getBeforeDateEndTimestamp(int period) {
        return getEndTimestamp(getOneDay(0 - period));
    }

    //获取昨日起始时间戳
    public static long getYesStartTimestamp() {
        return getStartTimestamp(getYesterday());
    }

    //获得昨日结束时间戳
    public static long getYesEndTimestamp() {
        return getEndTimestamp(getYesterday());
    }

    public static void main(String[] args) {
        Date date = new Date();//取时间

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        System.out.println("开始时间：" + calendar.getTime());
        System.out.println("开始时间：" + calendar.getTimeInMillis());
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        System.out.println("结束时间：" + calendar.getTime());
        System.out.println("结束时间：" + calendar.getTimeInMillis());

        long startTime = getYesStartTimestamp();
        long endTime = getYesEndTimestamp();

        System.out.println("startTime：" + startTime);
        System.out.println("endTime：" + endTime);
    }
}
