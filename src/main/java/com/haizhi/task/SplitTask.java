package com.haizhi.task;


import com.haizhi.hbase.HBaseDao;
import com.haizhi.mongodb.*;
import com.haizhi.util.PropertyUtil;
import com.haizhi.util.TimeUtil;
import com.haizhi.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SplitTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SplitTask.class);

    private static final long PERIOD_TIME = 10 * 60 * 1000;

    // 休眠周期 2 * 60 * 60 * 1000
    private long sleepPeriod = 0;

    // mongodb访问接口
    private DailyTaskService dailyTaskService;

    // 任务完成情况接口
    private FinishRecordServiceImpl finishRecordService;

    //校验周期
    private int checkPeriod;

    //任务划分粒度
    private long taskPeriod;

    // 记录表标识
    private String recordChangeFlag;

    // 任务表标识
    private String taskChangeFlag;

    // HBase
    private HBaseDao hBaseDao;

    public SplitTask(long period) {
        this.sleepPeriod = period;

        this.checkPeriod = PropertyUtil.getInt("time.check.period");
        logger.info("校验周期为: {}天", this.checkPeriod);

        this.recordChangeFlag = PropertyUtil.getProperty("record.change.flag");

        this.taskChangeFlag = PropertyUtil.getProperty("task.change.flag");

        this.dailyTaskService = new DailyTaskServiceImpl();

        this.finishRecordService = new FinishRecordServiceImpl();

        String quorum = PropertyUtil.getProperty("hbase.zookeeper.quorum");
        String clientPort = PropertyUtil.getProperty("hbase.zookeeper.property.clientPort");
        String master = PropertyUtil.getProperty("hbase.master");
        this.hBaseDao = new HBaseDao(quorum, clientPort, master);

        this.taskPeriod = PropertyUtil.getLong("time.task.period", String.valueOf(PERIOD_TIME));
        if (this.taskPeriod < PERIOD_TIME / 2) {
            this.taskPeriod = PERIOD_TIME / 2;
        }

        //创建索引
        createIndexs();
    }

    //创建索引
    private void createIndexs() {
        logger.info("开始创建索引..");

        //获得所有记录表
        List<String> recordNameList = hBaseDao.getFilterTableNameList(recordChangeFlag);
        recordNameList.forEach(tableName -> {
            // 先获得业务表名称
            String businessTable = getBusinessTable(tableName);


            //拼装成
            String collectionName = getTaskCollectionName(businessTable);
            logger.info("当前创建索引的表为: {}", collectionName);

            //创建索引
            dailyTaskService.createIndexs("finish", collectionName);
        });

        logger.info("索引创建完成..");
    }

    // 获得业务表名称, 对应的mongodb表
    private String getBusinessTable(String recordTableName) {
        return recordTableName.split(ToolUtil.TABLE_SEPARATOR)[1];
    }

    // 获得任务表名称
    private String getTaskCollectionName(String businessTable) {
        return taskChangeFlag + ToolUtil.TABLE_SEPARATOR + businessTable;
    }

    private void startRun() {

        logger.info("开始分解任务..");
        long startCountTime = System.currentTimeMillis();

        //获得所有记录表
        List<String> recordNameList = hBaseDao.getFilterTableNameList(recordChangeFlag);

        //分解任务
        for (int i = 0; i <= checkPeriod; i++) {

            String date = TimeUtil.getBeforeDate(i);
            logger.info("当前需要检测是的任务日期: date = {} period = {}", date, i);

            FinishRecord finishRecord = finishRecordService.findOne(date);
            final boolean[] isChanged = {false};
            if (finishRecord == null) {
                finishRecord = new FinishRecord(date);
                isChanged[0] = true;
            }

            //前闭后开 [startBeforeTime, endBeforeTime)
            long startBeforeTime = TimeUtil.getBeforeDateStartTimestamp(i);
            long endBeforeTime = TimeUtil.getBeforeDateEndTimestamp(i);

            FinishRecord finalFinishRecord = finishRecord;
            recordNameList.forEach(tableName -> {

                // 先获得业务表名称
                String businessTable = getBusinessTable(tableName);
                logger.info("当前需要分解任务的业务表为: {}", businessTable);

                //拼装成
                String collectionName = getTaskCollectionName(businessTable);

                // 判断是否需要填充表名称
                if (!finalFinishRecord.getTaskMap().containsKey(collectionName)) {
                    finalFinishRecord.getTaskMap().put(collectionName, new FinishRecord.SubTask());
                    isChanged[0] = true;
                }

                //如果没有找到任务信息
                if (dailyTaskService.findOne(date, collectionName) == null) {

                    logger.info("创建天级任务: {}", date);

                    //创建天级任务...
                    DailyTask dailyTask = new DailyTask();
                    dailyTask.setTaskPeriod(taskPeriod);
                    dailyTask.setDate(date);

                    List<PeriodTask> periodList = new ArrayList<>();

                    long start = startBeforeTime;
                    while (start < endBeforeTime) {

                        long tempTime = start + taskPeriod;
                        if (tempTime > endBeforeTime) {
                            tempTime = endBeforeTime;
                        }

                        String _id = String.valueOf(start) + "_" + String.valueOf(tempTime);
                        PeriodTask taskPeriod = new PeriodTask(_id, start, tempTime);
                        periodList.add(taskPeriod);
                        start = tempTime;
                    }
                    dailyTask.setPeriodList(periodList);
                    dailyTaskService.insertOne(dailyTask, collectionName);
                }

            });

            //如果需要填充信息 则更新数据库信息
            if (isChanged[0]) {
                // 如果被更改了, 则修正为没有完成状态
                finalFinishRecord.setFinish(false);
                finishRecordService.updateOne(finalFinishRecord);
            }
        }

        logger.info("分解任务花费时间: {}ms", System.currentTimeMillis() - startCountTime);
        logger.info("分解任务完成..");
    }

    @Override
    public void run() {
        while (true) {
            try {
                startRun();
                Thread.sleep(sleepPeriod);
            } catch (InterruptedException e) {
                logger.error("线程休眠被中断...");
                logger.error("ERROR", e);
            }
        }
    }
}
