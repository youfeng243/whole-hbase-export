package com.haizhi.mongodb;

import com.haizhi.util.TimeUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PeriodTask implements Serializable {
    private static final long serialVersionUID = 1L;

    // 索引信息
    private String period;

    // 当前的任务状态,  1 执行成功 0 未执行, -1 执行失败 -2 达到失败次数也未成功
    private int status;

    // 起始时间
    private long startTimeStamp;

    // 时间格式展示
    private String startTime;

    // 结束时间
    private long endTimeStamp;

    // 时间格式展示
    private String endTime;

    // 失败次数记录
    private int errorTimes;

    //任务创建时间
    private String createTime;

    //任务最近被执行时间
    private String updateTime;

    //最终输出文件名 zip 文件名
    private String zipFileName;

    //文件列表
    private List<Map<String, String>> fileNameList = new ArrayList<>();

    public PeriodTask(String period, long startTimeStamp, long endTimeStamp) {
        this.period = period;
        this.status = TaskStatus.STATUS_NOT_FINISH;
        this.createTime = this.updateTime = TimeUtil.getCurrentTime();
        this.errorTimes = 0;
        this.startTimeStamp = startTimeStamp;
        this.endTimeStamp = endTimeStamp;

        this.startTime = TimeUtil.getTime(this.startTimeStamp);
        this.endTime = TimeUtil.getTime(this.endTimeStamp);

        this.zipFileName = "";
    }

    public PeriodTask() {

    }

    public List<Map<String, String>> getFileNameList() {
        return fileNameList;
    }

    public void setFileNameList(List<Map<String, String>> fileNameList) {
        this.fileNameList = fileNameList;
    }

    public String getZipFileName() {
        return zipFileName;
    }

    public void setZipFileName(String zipFileName) {
        this.zipFileName = zipFileName;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
        startTime = TimeUtil.getTime(startTimeStamp);
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public long getEndTimeStamp() {
        return endTimeStamp;
    }

    public void setEndTimeStamp(long endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
        endTime = TimeUtil.getTime(endTimeStamp);
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    public int getErrorTimes() {
        return errorTimes;
    }

    public void setErrorTimes(int errorTimes) {
        this.errorTimes = errorTimes;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
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

    @Override
    public String toString() {
        return "PeriodTask{" +
                "period='" + period + '\'' +
                ", status=" + status +
                ", startTimeStamp=" + startTimeStamp +
                ", startTime='" + startTime + '\'' +
                ", endTimeStamp=" + endTimeStamp +
                ", endTime='" + endTime + '\'' +
                ", errorTimes=" + errorTimes +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", zipFileName='" + zipFileName + '\'' +
                '}';
    }
}
