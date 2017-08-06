package com.haizhi.task;

import com.haizhi.hbase.HBaseDao;
import com.haizhi.model.ResultMsg;
import com.haizhi.mongodb.*;
import com.haizhi.util.PropertyUtil;
import com.haizhi.util.TimeUtil;
import com.haizhi.util.ToolUtil;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ScanTask implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ScanTask.class);

    // 索引文件名称
    //private static final String INDEX_FILE = "status.txt";

    // 休眠周期 30 * 60 * 1000
    private long sleepPeriod = 0;

    // 记录表标识
    private String recordChangeFlag;

    // 任务表标识
    private String taskChangeFlag;

    //文件夹位置
    private String downLoadPath = null;
    private String tmpPath = null;

    // 线程数目
    private int threadNum = 0;

    //最大排队任务数目
    private int maxWaitTaskNum = 0;

    // HBase
    private HBaseDao hBaseDao;

    //mongodb
    private DailyTaskService dailyTaskService;

    public ScanTask(long period) {
        this.sleepPeriod = period;

        this.recordChangeFlag = PropertyUtil.getProperty("record.change.flag");
        logger.info("记录表标识: {}", this.recordChangeFlag);

        this.taskChangeFlag = PropertyUtil.getProperty("task.change.flag");
        logger.info("任务表标识: {}", this.taskChangeFlag);

        this.threadNum = PropertyUtil.getInt("task.thread.num");
        if (threadNum <= 0) {
            threadNum = 4;
        }
        logger.info("当前开启的执行线程数目: {}", this.threadNum);

        this.maxWaitTaskNum = PropertyUtil.getInt("max.wait.thread.num");
        if (maxWaitTaskNum < threadNum) {
            maxWaitTaskNum = threadNum;
        }
        logger.info("当前最大的任务排队数目: {}", maxWaitTaskNum);

        this.downLoadPath = PropertyUtil.getProperty("download.location");
        this.tmpPath = PropertyUtil.getProperty("temp.location");
        logger.info("当前下载路径: {}", this.downLoadPath);
        logger.info("当前临时文件夹路径: {}", this.tmpPath);

        //确保文件目录有执行权限
        ToolUtil.createFolder(this.downLoadPath);
        ToolUtil.createFolder(this.tmpPath);

        this.dailyTaskService = new DailyTaskServiceImpl();

        String quorum = PropertyUtil.getProperty("hbase.zookeeper.quorum");
        String clientPort = PropertyUtil.getProperty("hbase.zookeeper.property.clientPort");
        String master = PropertyUtil.getProperty("hbase.master");
        this.hBaseDao = new HBaseDao(quorum, clientPort, master);
    }

    // 获得业务表名称, 对应的mongodb表
    private String getBusinessTable(String collectName) {
        return collectName.split(ToolUtil.TABLE_SEPARATOR)[1];
    }

    // 获得记录表信息
    private String getRecordTable(String businessTableName) {
        return recordChangeFlag + businessTableName;
    }

    // 等待任务完成, 存储完成状态
    private void waitForFinish(List<Pair<PeriodTask, Future<ResultMsg>>> resultList, List<ResultMsg> msgList) {

        //存储计算结果
        resultList.forEach(taskPeriodFuturePair -> {
            PeriodTask taskPeriod = taskPeriodFuturePair.getKey();
            Future<ResultMsg> integerFuture = taskPeriodFuturePair.getValue();
            try {
                ResultMsg result = integerFuture.get();

                //存储文件结果
                msgList.add(result);

                //判断是否完成..
                if (result.getResult() == TaskStatus.STATUS_FAIL) {
                    taskPeriod.setErrorTimes(taskPeriod.getErrorTimes() + 1);
                    if (taskPeriod.getErrorTimes() >= TaskStatus.MAX_ERROR_TIMES) {
                        taskPeriod.setStatus(TaskStatus.STATUS_NEVER_SUCCESS);
                    } else {
                        taskPeriod.setStatus(TaskStatus.STATUS_FAIL);
                    }
                } else {
                    taskPeriod.setStatus(result.getResult());
                }

                //如果已经完成, 则记录文件名
                if (result.getResult() == TaskStatus.STATUS_SUCCESS) {
                    taskPeriod.setZipFileName(result.getZipFileName());
                    taskPeriod.setFileNameList(result.getFileNameList());
                }

                //更新当前时间
                taskPeriod.setUpdateTime(TimeUtil.getCurrentTime());
            } catch (InterruptedException | ExecutionException e) {
                logger.error("ERROR", e);
            }
        });
    }

    // 判断是否所有的记录全部不完成了..
    private boolean isAllFinish(DailyTask dailyTask) {


        for (PeriodTask periodTask : dailyTask.getPeriodList()) {
            if (periodTask.getStatus() == TaskStatus.STATUS_FAIL ||
                    periodTask.getStatus() == TaskStatus.STATUS_NOT_FINISH) {
                logger.warn("当前任务还未完成: {} {}", dailyTask.getDate(), periodTask.getPeriod());
                return false;
            }
        }

        return true;
    }

    //创建索引文件
//    private boolean createIndexFile(String indexFilePath, String indexTempPath, DailyTask dailyTask) {
//
//        boolean isSuccess = true;
//
//        List<PeriodTask> periodTaskList = dailyTask.getPeriodList();
//
//        FileExport fileExport = new FileExport(indexTempPath);
//        for (PeriodTask periodTask : periodTaskList) {
//
//            //没有数据
//            if (periodTask.getStatus() == TaskStatus.STATUS_NO_DATA) {
//                continue;
//            }
//
//            //如果还有未成功导出的, 则直接返回判断任务失败
//            if (periodTask.getStatus() != TaskStatus.STATUS_SUCCESS) {
//                isSuccess = false;
//                break;
//            }
//
//            logger.info("当前检测文件名: {}", periodTask.getZipFileName());
//            logger.info("当前检测路径: {}", periodTask.getFilePath());
//            if (!Files.exists(Paths.get(periodTask.getFilePath()))) {
//                logger.error("压缩文件不存在, 不能生成索引文件...");
//                isSuccess = false;
//                periodTask.setStatus(TaskStatus.STATUS_FAIL);
//                break;
//            }
//            fileExport.write(periodTask.getZipFileName());
//        }
//        fileExport.close();
//
//        //如果文件生成完成 则移动文件
//        if (isSuccess) {
//            //如果移动索引文件成功 才算最终任务完成...
//            if (FileExport.moveFile(indexTempPath, indexFilePath)) {
//                return true;
//            }
//        }
//
//        return false;
//    }

    //开始执行线程..
    private void startRun() {
        logger.info("#####开始批量导出任务..");
        long startTime = System.currentTimeMillis();

        // 遍历mongodb任务表
        List<String> taskCollectionList = dailyTaskService.getFilterCollectionNameList(taskChangeFlag);

        ExecutorService pool = Executors.newFixedThreadPool(threadNum);
        List<Pair<PeriodTask, Future<ResultMsg>>> resultList = new ArrayList<>();
        taskCollectionList.forEach(taskCollectName -> {

            logger.info("当前扫描对应的任务表: {}", taskCollectName);

            // 获得业务表名称 业务表名则为子文件夹名 enterprise_data_gov
            String businessTableName = getBusinessTable(taskCollectName);
            logger.info("当前扫描对应的业务表: {}", businessTableName);

            // 获得记录表信息
            String recordTableName = getRecordTable(businessTableName);
            logger.info("当前扫描对应的记录表: {}", recordTableName);

            // 获得任务表的未完成的任务信息
            List<DailyTask> dailyTaskList = dailyTaskService.findAllByAllFinishIsFalse(taskCollectName);

            // 遍历业务表
            dailyTaskList.forEach(dailyTask -> {

                List<ResultMsg> msgList = new ArrayList<>();

                //生成最终下载文件路径
                String downPath = downLoadPath + dailyTask.getDate() + "/";
                String tPath = tmpPath + dailyTask.getDate() + "/";

                logger.info("下载目录: {}", downPath);
                logger.info("临时目录: {}", tPath);
                //logger.info("索引文件: {}", indexFilePath);

                //确保文件路径存在
                ToolUtil.createFolder(downPath);
                ToolUtil.createFolder(tPath);

                List<PeriodTask> periodTaskList = dailyTask.getPeriodList();
                periodTaskList.forEach(periodTask -> {

                    if (periodTask.getStatus() == TaskStatus.STATUS_SUCCESS ||
                            periodTask.getStatus() == TaskStatus.STATUS_NEVER_SUCCESS ||
                            periodTask.getStatus() == TaskStatus.STATUS_NO_DATA) {
                        logger.info("当前任务不需要执行: {} {} {}", taskCollectName, dailyTask.getDate(), periodTask.getPeriod());
                        return;
                    }

                    //获得当前时间戳
                    long currentTimeStamp = TimeUtil.getCurrentTimestamp();
                    long endTimeStamp = periodTask.getEndTimeStamp();

                    //如果当前时间大于最后的时间戳 则开始导出
                    if (currentTimeStamp < endTimeStamp + 10 * 60 * 1000) {
                        return;
                    }

                    logger.info("开始执行任务: {} {} {}", taskCollectName, dailyTask.getDate(), periodTask.getPeriod());
                    resultList.add(new Pair<>(periodTask, pool.submit(new ExecuteTask(
                            periodTask,
                            downPath,
                            tPath,
                            recordTableName,
                            businessTableName,
                            hBaseDao))));

                    // 当达到最大的排队数目时, 需要等待所有线程完成先..
                    if (resultList.size() >= maxWaitTaskNum) {
                        //等待线程完成
                        waitForFinish(resultList, msgList);

                        //清空结果信息
                        resultList.clear();

                        // 完成批量任务后先保存
                        dailyTaskService.updateOne(dailyTask, taskCollectName);
                    }
                });

                // 这里先等待所有线程完成, 然后再判断是否所有的线程都成功下载完数据了..
                //等待线程完成
                waitForFinish(resultList, msgList);

                //清空结果信息
                resultList.clear();

                //如果所有任务都完成了...
                if (isAllFinish(dailyTask)) {
                    dailyTask.setFinish(true);
                }

                //记录到数据库中
                dailyTaskService.updateOne(dailyTask, taskCollectName);
            });

        });

        pool.shutdown();
        try {
            logger.info("开始等待所有线程完成....");
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.error("ERROR", e);
        }

        logger.info("#####批量导出任务花费时间: {}ms", System.currentTimeMillis() - startTime);
        logger.info("#####批量导出任务完成..");
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

//    //5 分钟扫描执行一次任务
//    @Scheduled(fixedDelay = 5 * 1000 * 60)
//    //@Scheduled(fixedDelay = 10 * 1000)
//    public void executeTask() {
//        logger.info("#####开始批量导出任务..");
//        long startTime = System.currentTimeMillis();
//
//        // 先获得所有记录表名
//        List<String> recordTableList = hBaseDao.getTableNameList().
//                parallelStream().
//                filter(tableName -> tableName.startsWith(recordChangeFlag)).
//                collect(Collectors.toList());
//
//        //打印出所有记录表
//        logger.info("开始打印所有记录表信息:");
//        recordTableList.forEach(tableName -> logger.info(tableName));
//        logger.info("记录表信息打印结束!");
//
//
////        //开始运行线程池
////        List<Pair<PeriodTask, Future<Integer>>> resultList = new ArrayList<>();
////        ExecutorService pool = Executors.newFixedThreadPool(threadNum);
////        dailyTaskService.findAllNotFinish().forEach(taskPeriod -> resultList.add(
////                new Pair<>(taskPeriod, pool.submit(new ExecuteTask(
////                        taskPeriod,
////                        downLoadPath,
////                        tmpPath,
////                        recordTableName,
////                        hBaseDao)))));
////
////        //关闭线程池
////        pool.shutdown();
////
////        //存储计算结果
////        resultList.forEach(taskPeriodFuturePair -> {
////            PeriodTask taskPeriod = taskPeriodFuturePair.getKey();
////            Future<Integer> integerFuture = taskPeriodFuturePair.getValue();
////            try {
////                int result = integerFuture.get();
////                if (result == PeriodTask.STATUS_FAIL) {
////                    taskPeriod.setErrorTimes(taskPeriod.getErrorTimes() + 1);
////                    if (taskPeriod.getErrorTimes() >= PeriodTask.MAX_ERROR_TIMES) {
////                        taskPeriod.setStatus(PeriodTask.STATUS_NEVER_SUCCESS);
////                    } else {
////                        taskPeriod.setStatus(PeriodTask.STATUS_FAIL);
////                    }
////                } else {
////                    taskPeriod.setStatus(result);
////                }
////                dailyTaskService.updateOne(taskPeriod);
////            } catch (InterruptedException | ExecutionException e) {
////                logger.error("ERROR", e);
////            }
////        });
////
////        try {
////            logger.info("开始等待所有线程完成....");
////            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
////        } catch (InterruptedException e) {
////            logger.error("等待线程结束被中断...");
////            logger.error("ERROR", e);
////        }
//
//
//        logger.info("#####批量导出任务花费时间: {}ms", System.currentTimeMillis() - startTime);
//        logger.info("#####批量导出任务完成..");
//    }
}
