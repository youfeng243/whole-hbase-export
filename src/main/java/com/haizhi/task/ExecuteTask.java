package com.haizhi.task;

import com.haizhi.file.FileExport;
import com.haizhi.file.ZipCompress;
import com.haizhi.hbase.HBaseDao;
import com.haizhi.model.ResultMsg;
import com.haizhi.mongodb.PeriodTask;
import com.haizhi.mongodb.TaskStatus;
import com.haizhi.util.GenRecord;
import com.haizhi.util.JsonUtil;
import com.haizhi.util.ToolUtil;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class ExecuteTask implements Callable<ResultMsg> {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteTask.class);

    private SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private long startTimeStamp;
    private long endTimeStamp;
    private String startTime;
    private String endTime;
    private String recordTableName;
    private HBaseDao hBaseDao;

    //时间段压缩包名称
    private String zipFileName = null;

    //zip文件路径
    private String zipFilePath = null;

    //zip临时存放路径
    private String zipTempPath = null;

    //当前下载目录
    private String downLoadPath = null;

    //当前临时目录
    private String tmpLoadPath = null;

    //批量文件句柄
    private Map<String, FileExport> updateExportMap = new HashMap<>();
    private Map<String, FileExport> deleteExportMap = new HashMap<>();

    //临时文件路径
    private Map<String, String> tempPathMap = new HashMap<>();

    //数据块文件名举例
    // insert#wd_enterprise_data_gov-key_person#1495419000000#1495419600000
    // delete#wd_enterprise_data_gov-key_person#1495419000000#1495419600000

    public ExecuteTask(PeriodTask taskPeriod,
                       String downLoadPath,
                       String tmpPath,
                       String recordTableName,
                       String businessTableName,
                       HBaseDao hBaseDao) {
        this.startTimeStamp = taskPeriod.getStartTimeStamp();
        this.endTimeStamp = taskPeriod.getEndTimeStamp();
        logger.info("当前导出时间戳: {} - {}", startTimeStamp, endTimeStamp);

        this.startTime = getTime(this.startTimeStamp);
        this.endTime = getTime(this.endTimeStamp);
        logger.info("当前导出时间段: {} - {}", startTime, endTime);

        this.recordTableName = recordTableName;
        logger.info("当前记录表名称: {}", recordTableName);

        this.hBaseDao = hBaseDao;

        this.downLoadPath = downLoadPath;
        logger.info("当前下载目录: {}", this.downLoadPath);

        this.tmpLoadPath = tmpPath;
        logger.info("当前临时目录: {}", tmpLoadPath);


        //当前压缩包名称 {业务表名}#{起始时间戳}#{结束时间戳}.zip
        zipFileName = ZipCompress.getFileName(
                businessTableName +
                        ToolUtil.FILENAME_SEPARATOR +
                        String.valueOf(startTimeStamp) +
                        ToolUtil.FILENAME_SEPARATOR +
                        String.valueOf(endTimeStamp));
        logger.info("当前压缩包名称: {}", zipFileName);

        //当前压缩包临时路径
        zipTempPath = tmpPath + zipFileName;
        logger.info("当前压缩包临时完整路径: {}", zipTempPath);

        //当前压缩包最终存储下载路径
        zipFilePath = downLoadPath + zipFileName;
        logger.info("当前压缩包存储完整路径: {}", zipFilePath);
    }

    public String getTime(long timeStamp) {
        return sdFormat.format(new Date(timeStamp));
    }

    // 关闭文件句柄
    private void closeFiles() {

        // 关闭插入数据 句柄
        for (Map.Entry<String, FileExport> entry : updateExportMap.entrySet()) {
            entry.getValue().close();
        }

        for (Map.Entry<String, FileExport> entry : deleteExportMap.entrySet()) {
            entry.getValue().close();
        }

        logger.info("文件句柄关闭完成..");
    }

    //压缩
    private void compressFiles() {
        long startTime = System.currentTimeMillis();
        logger.info("开始压缩: {}", zipTempPath);
        new ZipCompress(zipTempPath, tempPathMap).compress().close();
        logger.info("压缩完成: {} 耗时: {} ms", zipTempPath, System.currentTimeMillis() - startTime);
    }

    private void moveFiles() {

        long startTime = System.currentTimeMillis();
        logger.info("开始移动压缩包: {}", zipTempPath);
        FileExport.moveFile(zipTempPath, zipFilePath);
        logger.info("移动完成: {} 耗时: {} ms", zipFilePath, System.currentTimeMillis() - startTime);
    }

    //获得数据块文件名
    private String getDataFileName(String tableName, String op) {
        return op +
                ToolUtil.FILENAME_SEPARATOR +
                tableName +
                ToolUtil.FILENAME_SEPARATOR +
                String.valueOf(startTimeStamp) +
                ToolUtil.FILENAME_SEPARATOR +
                String.valueOf(endTimeStamp);
    }

    //写入删除的数据...
    private void writeDelete(String tableName, String refRowKey) {

        // 先判断当前表名是否已经创建了文件操作句柄
        if (deleteExportMap.containsKey(tableName)) {
            deleteExportMap.get(tableName).write(refRowKey);
            return;
        }

        //先拼接写入文件名称 delete#wd_enterprise_data_gov-key_person#1495419000000#1495419600000
        String fileName = getDataFileName(tableName, GenRecord.COLUMN_DELETE);

        // 再拼接文件路径
        String filePath = tmpLoadPath + fileName;

        // 再生成文件操作句柄
        FileExport fileExport = new FileExport(filePath);

        //记录句柄
        deleteExportMap.put(tableName, fileExport);

        //记录压缩路径信息
        tempPathMap.put(fileName, filePath);

        //写入文件
        fileExport.write(refRowKey);
    }

    // 写入插入数据
    private void writeInsert(String tableName, String refRowKey, String value) {

        FileExport fileExport = null;

        // 如果表的文件操作句柄还未创建, 先创建
        if (!updateExportMap.containsKey(tableName)) {

            //先拼接写入文件名称 update#wd_enterprise_data_gov-key_person#1495419000000#1495419600000
            String fileName = getDataFileName(tableName, GenRecord.COLUMN_UPDATE);

            // 再拼接文件路径
            String filePath = tmpLoadPath + fileName;

            // 再生成文件操作句柄
            fileExport = new FileExport(filePath);

            //记录句柄
            updateExportMap.put(tableName, fileExport);

            //记录压缩路径信息
            tempPathMap.put(fileName, filePath);
        }

        //先获得文件操作句柄
        if (fileExport == null) {
            fileExport = updateExportMap.get(tableName);
        }

        Map<String, Map<String, String>> genData = (Map<String, Map<String, String>>) JsonUtil.jsonToObject(value, Map.class);
        assert genData != null;
        for (Map.Entry<String, Map<String, String>> entry : genData.entrySet()) {

            String family = entry.getKey();
            Map<String, String> valueKey = entry.getValue();
            for (Map.Entry<String, String> keyEntry : valueKey.entrySet()) {
                String column = keyEntry.getKey();
                String v = keyEntry.getValue();

                fileExport.write(ToolUtil.genInsertData(
                        refRowKey,
                        family,
                        column,
                        v));
            }

        }
    }

    //删除临时文件
    private void deleteTempFiles() {
        long deleteTime = System.currentTimeMillis();
        logger.info("开始删除临时文件...");
        tempPathMap.forEach((fileName, filePath) -> {
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                logger.error("删除文件失败: {} {}", fileName, filePath);
                logger.error("ERROR", e);
            }
        });
        logger.info("删除临时文件数据块完成, 耗时: {}", System.currentTimeMillis() - deleteTime);
    }

    //得到文件列表
    private List<Map<String, String>> getFileList() {

        List<Map<String, String>> fileNameList = new ArrayList<>();
        //updateExportMap -> {表名, 临时文件句柄}
        //deleteExportMap -> {表名, 临时文件句柄}

        updateExportMap.forEach((tableName, fileExport) -> {
            //获得文件名
            String fileName = getDataFileName(tableName, GenRecord.COLUMN_UPDATE);
            if (fileExport.getWriteNum() > 0) {
                Map<String, String> sub = new HashMap<>();
                sub.put("fileName", fileName);
                sub.put("tableName", tableName);
                fileNameList.add(sub);
            }
        });

        deleteExportMap.forEach((tableName, fileExport) -> {
            String fileName = getDataFileName(tableName, GenRecord.COLUMN_DELETE);
            if (fileExport.getWriteNum() > 0) {
                Map<String, String> sub = new HashMap<>();
                sub.put("fileName", fileName);
                sub.put("tableName", tableName);
                fileNameList.add(sub);
            }
        });

        return fileNameList;
    }

    @Override
    public ResultMsg call() throws Exception {
        logger.info("开始执行导出任务...");
        ResultMsg resultMsg = new ResultMsg();
        resultMsg.setZipFileName(zipFileName);

        long taskStartTime = System.currentTimeMillis();

        long totalCount = 0;
        final long[] currentPoint = {0};
        final long[] insertTotal = {0};
        final long[] deleteTotal = {0};
        Table recordTable;
        ResultScanner resultScanner;

        // 这里是具体的扫描流程
        recordTable = hBaseDao.getTable(recordTableName);
        resultScanner = hBaseDao.getRangeRows(recordTable, startTimeStamp, endTimeStamp);
        if (resultScanner == null) {
            recordTable.close();
            logger.warn("获取扫描器失败: table = {}", recordTableName);
            resultMsg.setResult(TaskStatus.STATUS_FAIL);
            return resultMsg;
        }

        Result scanResult = resultScanner.next();
        if (scanResult == null) {
            logger.info("没有扫描到任何数据...[{} - {}]", startTime, endTime);
            resultMsg.setResult(TaskStatus.STATUS_NO_DATA);
            return resultMsg;
        }

        logger.info("获取扫描器成功, 开始扫描区间数据...[{} - {}]", startTime, endTime);
        do {

            totalCount += scanResult.listCells().parallelStream().map(cell -> {
                String rowKey = new String(CellUtil.cloneRow(cell));
                String operation = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));

                currentPoint[0] += 1;
                if (currentPoint[0] % 10000 == 0) {
                    logger.info("[{} - {}] 当前进度: {}", startTime, endTime, currentPoint[0]);
                }

                String[] splitList = rowKey.split("#");
                if (splitList.length < 2) {
                    logger.error("[{} - {}] 分割数目不正确: {}", startTime, endTime, rowKey);
                    return 0;
                }

                String refRowKey = splitList[0];
                String refTableName = splitList[1];

                //如果是删除
                if (Objects.equals(operation, GenRecord.COLUMN_DELETE)) {
                    writeDelete(refTableName, refRowKey);
                    deleteTotal[0] += 1;
                    return 1;
                }

                // 写入插入数据
                writeInsert(refTableName, refRowKey, value);
                insertTotal[0] += 1;
                return 1;
            }).count();

            //再次扫描
            scanResult = resultScanner.next();
            if (scanResult == null) {
                logger.info("[{} - {}] 没有扫描到任何数据...", startTime, endTime);
                break;
            }

        } while (true);

        //关闭扫描器
        resultScanner.close();

        if (recordTable != null) {
            try {
                recordTable.close();
            } catch (IOException e) {
                logger.error("关闭recordTable失败");
            }
        }

        //关闭文件
        closeFiles();

        //得到文件列表
        List<Map<String, String>> fileNameList = getFileList();

        //压缩文件
        compressFiles();

        //移动文件
        moveFiles();

        //删除临时文件
        deleteTempFiles();

        logger.info("[{} - {}] insert or update操作总数目: {}", startTime, endTime, insertTotal[0]);
        logger.info("[{} - {}] delete操作总数目: {}", startTime, endTime, deleteTotal[0]);
        logger.info("[{} - {}] 总操作数据量为: totalCount = {}", startTime, endTime, totalCount);
        logger.info("[{} - {}] 导出任务执行完成...", startTime, endTime);
        logger.info("[{} - {}] 任务耗时: {}ms", startTime, endTime, System.currentTimeMillis() - taskStartTime);

        //判断是否有数据导出..
        if (fileNameList.size() > 0) {
            resultMsg.setResult(TaskStatus.STATUS_SUCCESS);
        } else {
            resultMsg.setResult(TaskStatus.STATUS_NO_DATA);
            logger.info("没有任何数据导出: {} {} {}", zipFileName, startTime, endTime);
        }
        resultMsg.setFileNameList(fileNameList);
        return resultMsg;
    }
}
