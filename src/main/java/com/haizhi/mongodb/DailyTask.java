package com.haizhi.mongodb;


import com.haizhi.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

//@Document(collection = "hbase_export_task")
public class DailyTask {

    private static final long DEFAULT_PERIOD = 3600000;

    // 当前日期信息
    private String date;

    // 任务时间段划分 毫秒级时间戳
    private Long taskPeriod;

    // 判断是否所有周期都已经完成
    private Boolean finish;

    // 不同时间段完成情况
    private List<PeriodTask> periodList = new ArrayList<>();

    //天级任务创建时间
    private String createTime;

    //天级任务最近被执行时间
    private String updateTime;

    public DailyTask() {
        finish = false;
        //默认一个小时为周期 毫秒级别
        taskPeriod = DEFAULT_PERIOD;
        createTime = updateTime = TimeUtil.getCurrentTime();
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getTaskPeriod() {
        return taskPeriod;
    }

    public void setTaskPeriod(Long taskPeriod) {
        this.taskPeriod = taskPeriod;
    }

    public Boolean getFinish() {
        return finish;
    }

    public void setFinish(Boolean finish) {
        this.finish = finish;
    }

    public List<PeriodTask> getPeriodList() {
        return periodList;
    }

    public void setPeriodList(List<PeriodTask> periodList) {
        this.periodList = periodList;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
