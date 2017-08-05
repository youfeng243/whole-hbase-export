package com.haizhi.task;

import com.haizhi.file.FileExport;
import com.haizhi.mongodb.*;
import com.haizhi.util.GenRecord;
import com.haizhi.util.PropertyUtil;
import com.haizhi.util.TimeUtil;
import com.haizhi.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by youfeng on 2017/6/6.
 * 检查任务完成状态线程
 */
public class CheckTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CheckTask.class);

    private static final String STATUS_FILE = "status.txt";
    private static final String CMD_FILE = "import.run";

    // 休眠周期 5 * 60 * 1000 5分钟扫描一次
    private long sleepPeriod = 0;

    // 数据库操作句柄
    private FinishRecordService finishRecordService;

    // 任务表操作
    private DailyTaskService dailyTaskService;

    //文件夹位置
    private String downLoadPath = null;
    private String tmpPath = null;


    public CheckTask(long period) {
        this.sleepPeriod = period;
        this.finishRecordService = new FinishRecordServiceImpl();
        this.dailyTaskService = new DailyTaskServiceImpl();

        this.downLoadPath = PropertyUtil.getProperty("download.location");
        this.tmpPath = PropertyUtil.getProperty("temp.location");
        logger.info("当前下载路径: {}", this.downLoadPath);
        logger.info("当前临时文件夹路径: {}", this.tmpPath);

        //确保文件目录有执行权限
        ToolUtil.createFolder(this.downLoadPath);
        ToolUtil.createFolder(this.tmpPath);
    }

    private boolean isAllFinish(FinishRecord finishRecord) {
        boolean isAllFinish = true;
        Map<String, FinishRecord.SubTask> taskMap = finishRecord.getTaskMap();
        for (Map.Entry<String, FinishRecord.SubTask> subTaskEntry : taskMap.entrySet()) {
            if (!subTaskEntry.getValue().getFinish()) {
                isAllFinish = false;
                break;
            }
        }
        return isAllFinish;
    }

    //获得所有的任务信息
    private List<DailyTask> getAllDailyTask(FinishRecord finishRecord) {
        List<DailyTask> dailyTaskList = new ArrayList<>();

        finishRecord.getTaskMap().forEach((collectionName, subTask) -> {
            DailyTask dailyTask = dailyTaskService.findOne(finishRecord.getDate(), collectionName);
            if (dailyTask == null) {
                logger.error("没有查找到任务信息: {}, {}", collectionName, finishRecord.getDate());
                return;
            }
            dailyTaskList.add(dailyTask);
        });
        return dailyTaskList;
    }

    //生成状态文件信息
    private void genStatusFile(List<DailyTask> dailyTaskList, String date) {
        //生成最终下载文件路径
        String downPath = downLoadPath + date + "/" + STATUS_FILE;
        String tPath = tmpPath + date + "/" + STATUS_FILE;

        //确保文件路径存在
        ToolUtil.createPathDirs(downPath);
        ToolUtil.createPathDirs(tPath);

        FileExport fileExport = new FileExport(tPath);

        dailyTaskList.forEach(dailyTask -> dailyTask.getPeriodList().forEach(periodTask -> {
            if (periodTask.getStatus() == TaskStatus.STATUS_SUCCESS) {
                fileExport.write(periodTask.getZipFileName());
            }
        }));

        fileExport.close();
        FileExport.moveFile(tPath, downPath);
    }

    //生成命令文件信息
    private void genCommandFile(List<DailyTask> dailyTaskList, String date) {
        String downPath = downLoadPath + date + "/" + CMD_FILE;
        String tPath = tmpPath + date + "/" + CMD_FILE;

        //确保文件路径存在
        ToolUtil.createPathDirs(downPath);
        ToolUtil.createPathDirs(tPath);

        FileExport fileExport = new FileExport(tPath);

        dailyTaskList.forEach(dailyTask -> {
            // 先解压数据
            dailyTask.getPeriodList().forEach(periodTask -> {
                if (periodTask.getStatus() == TaskStatus.STATUS_SUCCESS) {
                    String cmd = "unzip " + periodTask.getZipFileName();
                    fileExport.write(cmd);
                }
            });

            // 再导入更新数据
            dailyTask.getPeriodList().forEach(periodTask -> {
                if (periodTask.getStatus() == TaskStatus.STATUS_SUCCESS) {
                    List<Map<String, String>> fileNameList = periodTask.getFileNameList();
                    fileNameList.forEach(nameMap -> {
                        String fileName = nameMap.get("fileName");
                        String tableName = nameMap.get("tableName");

                        //更新数据
                        if (fileName.startsWith(GenRecord.COLUMN_UPDATE)) {
                            String cmd = GenRecord.COLUMN_UPDATE + " " +
                                    tableName + " " +
                                    fileName;
                            fileExport.write(cmd);
                        }

                    });
                }
            });

            // 再导入删除数据
            dailyTask.getPeriodList().forEach(periodTask -> {
                if (periodTask.getStatus() == TaskStatus.STATUS_SUCCESS) {
                    List<Map<String, String>> fileNameList = periodTask.getFileNameList();
                    fileNameList.forEach(nameMap -> {
                        String fileName = nameMap.get("fileName");
                        String tableName = nameMap.get("tableName");

                        //删除
                        if (fileName.startsWith(GenRecord.COLUMN_DELETE)) {
                            String cmd = GenRecord.COLUMN_DELETE + " " +
                                    tableName + " " +
                                    fileName;
                            fileExport.write(cmd);
                        }

                    });
                }
            });

        });

        fileExport.close();
        FileExport.moveFile(tPath, downPath);
    }

    private void startRun() {
        logger.info("开始检测执行状态...");
        long timeStamp = System.currentTimeMillis();

        //获取没有完成的任务信息
        List<FinishRecord> finishRecordList = finishRecordService.findAllNotFinish();
        finishRecordList.forEach(finishRecord -> {
            Map<String, FinishRecord.SubTask> taskMap = finishRecord.getTaskMap();
            String _id = finishRecord.getDate();

            //遍历没一张表
            taskMap.forEach((collectionName, task) -> {
                // 如果已经完成 则不进行查询检测
                if (task.getFinish()) {
                    return;
                }
                DailyTask dailyTask = dailyTaskService.findOne(_id, collectionName);
                if (dailyTask == null) {
                    logger.error("查询任务失败: {} {}", collectionName, _id);
                    return;
                }

                //记录更新时间
                if (dailyTask.getFinish()) {
                    task.setFinish(dailyTask.getFinish());
                    task.setUpdateTime(TimeUtil.getCurrentTime());
                }
            });

            // 判断是否全部完成..
            if (isAllFinish(finishRecord)) {
                // 获得所有的文件信息
                List<DailyTask> dailyTaskList = getAllDailyTask(finishRecord);

                // 生成status.txt文件
                genStatusFile(dailyTaskList, finishRecord.getDate());

                // 生成import.run文件
                genCommandFile(dailyTaskList, finishRecord.getDate());

                //更新完成状态
                finishRecord.setFinish(true);
            }
            //存储更新信息
            finishRecordService.updateOne(finishRecord);
        });


        logger.info("任务执行状态检测完成...{}ms", System.currentTimeMillis() - timeStamp);
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
