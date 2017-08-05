package com.haizhi.mongodb;

/**
 * Created by youfeng on 2017/5/28.
 * 完成状态
 */
public class TaskStatus {

    //未完成状态
    public static final int STATUS_NOT_FINISH = 0;

    //任务执行失败状态
    public static final int STATUS_FAIL = -1;

    //任务执行成功状态
    public static final int STATUS_SUCCESS = 1;

    //任务重试也未成功状态
    public static final int STATUS_NEVER_SUCCESS = -2;

    //    public static final int STATUS_NOT_FINISH = 0;
//    public static final int STATUS_FAIL = -1;
//    public static final int STATUS_SUCCESS = 1;
//    public static final int STATUS_NEVER_SUCCESS = -2;

    //没有数据
    public static final int STATUS_NO_DATA = 2;

    //最大出错次数
    public static final int MAX_ERROR_TIMES = 3;
}
