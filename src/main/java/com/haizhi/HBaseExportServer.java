package com.haizhi;

import com.haizhi.mongodb.MongoManager;
import com.haizhi.task.CheckTask;
import com.haizhi.task.CleanTask;
import com.haizhi.task.ScanTask;
import com.haizhi.task.SplitTask;
import com.haizhi.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseExportServer {

    private static final Logger logger = LoggerFactory.getLogger(HBaseExportServer.class);

    static {
        //先装在配置信息
        PropertyUtil.loadProperties("application.properties");
    }

    // 默认清理任务周期
    private static final long cleanPeriodTime = 2 * 60 * 60 * 1000;

    // 默认任务分解周期
    private static final long splitPeriodTime = 2 * 60 * 60 * 1000;

    // 默认任务执行周期
    private static final long scanPeriodTime = 30 * 60 * 1000;

    // 默认任务结果检测周期
    private static final long checkPeriodTime = 30 * 60 * 1000;

    public static void main(String[] args) {
        logger.info("启动HBase数据导出服务...");

        long cleanPeriodTimeTemp = PropertyUtil.getLong("clean.task.time", cleanPeriodTime);
        long splitPeriodTimeTemp = PropertyUtil.getLong("split.task.time", splitPeriodTime);
        long scanPeriodTimeTemp = PropertyUtil.getLong("scan.task.time", scanPeriodTime);
        long checkPeriodTimeTemp = PropertyUtil.getLong("check.task.time", checkPeriodTime);

        logger.info("任务清理周期: {}", cleanPeriodTimeTemp);
        logger.info("任务分解周期: {}", splitPeriodTimeTemp);
        logger.info("任务执行周期: {}", scanPeriodTimeTemp);
        logger.info("任务检测周期: {}", checkPeriodTimeTemp);


        //启动清理任务线程
        Thread cleanThread = new Thread(new CleanTask(cleanPeriodTimeTemp));
        cleanThread.setDaemon(true);
        cleanThread.start();

        //启动任务分解线程
        Thread splitThread = new Thread(new SplitTask(splitPeriodTimeTemp));
        splitThread.setDaemon(true);
        splitThread.start();

        //启动任务执行线程
        Thread scanThread = new Thread(new ScanTask(scanPeriodTimeTemp));
        scanThread.setDaemon(true);
        scanThread.start();

        //启动任务执行线程
        Thread checkThread = new Thread(new CheckTask(checkPeriodTimeTemp));
        checkThread.setDaemon(true);
        checkThread.start();

        //阻塞任务
        try {
            cleanThread.join();
            splitThread.join();
            scanThread.join();
            checkThread.join();
        } catch (InterruptedException e) {
            logger.error("ERROR", e);
        }

        //关闭mongodb连接
        MongoManager.close();
    }
}
