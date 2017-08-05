package com.haizhi.mongodb;

import com.haizhi.util.TimeUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by youfeng on 2017/6/4.
 * 任务完成情况记录
 */
public class FinishRecord {

    // 当前日期信息 _id 主键
    private String date;

    //天级任务创建时间
    private String createTime;

    //天级任务最近被执行时间
    private String updateTime;

    // 判断是否所有周期都已经完成
    private Boolean finish;

    // 不同的表任务信息
    private Map<String, SubTask> taskMap;

    public FinishRecord(String date) {
        this.date = date;
        this.createTime = this.updateTime = TimeUtil.getCurrentTime();
        this.finish = false;
        this.taskMap = new HashMap<>();
    }

    public FinishRecord() {
        this.createTime = this.updateTime = TimeUtil.getCurrentTime();
        this.finish = false;
        this.taskMap = new HashMap<>();
    }


    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
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

    public Boolean getFinish() {
        return finish;
    }

    public void setFinish(Boolean finish) {
        this.finish = finish;
    }

    public Map<String, SubTask> getTaskMap() {
        return taskMap;
    }

    public void setTaskMap(Map<String, SubTask> taskMap) {
        this.taskMap = taskMap;
    }

    public static class SubTask {

        //天级任务最近被执行时间
        private String updateTime;

        // 判断是否所有周期都已经完成
        private Boolean finish;

//        // zip信息记录
//        private Map<String, String> zipMap;
//
//        //update 文件信息记录
//        private Map<String, String> updateMap;
//
//        //删除文件信息
//        private Map<String, String> deleteMap;

        public SubTask() {
            this.updateTime = TimeUtil.getCurrentTime();
            this.finish = false;
//            zipMap = new HashMap<>();
//            updateMap = new HashMap<>();
//            deleteMap = new HashMap<>();
        }

        public String getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(String updateTime) {
            this.updateTime = updateTime;
        }

        public Boolean getFinish() {
            return finish;
        }

        public void setFinish(Boolean finish) {
            this.finish = finish;
        }

//        public Map<String, String> getZipMap() {
//            return zipMap;
//        }
//
//        public void setZipMap(Map<String, String> zipMap) {
//            this.zipMap = zipMap;
//        }
//
//        public Map<String, String> getUpdateMap() {
//            return updateMap;
//        }
//
//        public void setUpdateMap(Map<String, String> updateMap) {
//            this.updateMap = updateMap;
//        }
//
//        public Map<String, String> getDeleteMap() {
//            return deleteMap;
//        }
//
//        public void setDeleteMap(Map<String, String> deleteMap) {
//            this.deleteMap = deleteMap;
//        }

        @Override
        public String toString() {
            return "SubTask{" +
                    "updateTime='" + updateTime + '\'' +
                    ", finish=" + finish +
//                    ", zipMap=" + zipMap +
//                    ", updateMap=" + updateMap +
//                    ", deleteMap=" + deleteMap +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "FinishRecord{" +
                "date='" + date + '\'' +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", finish=" + finish +
                ", taskMap=" + taskMap +
                '}';
    }
}
