package com.haizhi.mongodb;

import java.util.List;

public interface DailyTaskService {

    DailyTask findOne(String date, String collectionName);

    boolean deleteOne(String date, String collectionName);

    boolean updateOne(DailyTask dailyTask, String collectionName);

    boolean insertOne(DailyTask dailyTask, String collectionName);

    //创建索引
    void createIndexs(String index, String collectionName);

    //找到所有未完成的天级任务
    List<DailyTask> findAllByAllFinishIsFalse(String collectionName);

    //删除所有已经完成的且时间周期小于某个时间点的
    void deleteAllByAllFinishIsTrueAndDateLessThan(String date, String collectionName);

    List<String> getAllCollectionNameList();

    List<String> getFilterCollectionNameList(String filter);
}
