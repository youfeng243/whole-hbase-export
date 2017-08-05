package com.haizhi.task;

import com.haizhi.HBaseExportServer;
import com.haizhi.mongodb.DailyTaskService;
import com.haizhi.mongodb.DailyTaskServiceImpl;
import com.haizhi.util.PropertyUtil;
import com.haizhi.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HBaseExportServer.class);

    // 休眠周期 2 * 60 * 60 * 1000
    private long sleepPeriod = 0;

    // 清理周期
    private int cleanPeriod = 0;

    // 任务标识
    private String taskChangeFlag;

    // mongodb访问接口
    private DailyTaskService dailyTaskService;

    public CleanTask(long period) {
        this.sleepPeriod = period;

        this.cleanPeriod = PropertyUtil.getInt("time.clean.period");
        logger.info("清理周期为: {}天", this.cleanPeriod);

        this.taskChangeFlag = PropertyUtil.getProperty("task.change.flag");

        this.dailyTaskService = new DailyTaskServiceImpl();
    }

    private void startRun() {
        logger.info("开始清理历史任务..");
        long startTime = System.currentTimeMillis();

        dailyTaskService.getAllCollectionNameList().parallelStream().
                filter(tableName -> tableName.startsWith(this.taskChangeFlag)).forEach(tableName -> {
            logger.info("当前需要清理的mongodb任务表: {}", tableName);
            dailyTaskService.deleteAllByAllFinishIsTrueAndDateLessThan(TimeUtil.getBeforeDate(cleanPeriod), tableName);
        });

        logger.info("清理任务花费时间: {}ms", System.currentTimeMillis() - startTime);
        logger.info("清理历史任务完成..");
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
