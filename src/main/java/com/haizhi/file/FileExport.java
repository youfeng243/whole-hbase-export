package com.haizhi.file;

import com.haizhi.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * 数据导出到文件类
 */
public class FileExport {

    private static Logger logger = LoggerFactory.getLogger(FileExport.class);

    //最大写入界限
    private static final long MAX_FLUSH = 1000000;

    private String fileName;
    private FileOutputStream fileOutputStream;
    private OutputStreamWriter outputStreamWriter;
    private BufferedWriter bufferedWriter;

    //记录起始时间
    private long startTime;

    //写入数据的总数
    private long totalNum;

    //强制刷新计数器
    private long flushNum;

    //初始化成功标志
    private boolean isInitSuccess;

    //写入缓存
    private StringBuilder stringBuilder = null;

    public FileExport(String fileName) {

        totalNum = 0;
        flushNum = 0;
        isInitSuccess = false;

        //获取起始时间
        startTime = System.currentTimeMillis();

        this.fileName = fileName;
        File file = new File(fileName);

        try {
            fileOutputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            logger.error("ERROR:", e);
            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                    fileOutputStream = null;
                } catch (IOException e1) {
                    logger.error("关闭fileOutputStream错误!!");
                    logger.error("ERROR:", e1);
                    fileOutputStream = null;
                }
            }

            logger.error("文件操作初始化失败: {}", fileName);
            return;
        }
        outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        bufferedWriter = new BufferedWriter(outputStreamWriter);

        isInitSuccess = true;
        logger.info("文件操作初始化成功: {}", fileName);
    }

    public boolean isInitSuccess() {
        return isInitSuccess;
    }

    //获取写入数目
    public long getWriteNum() {
        return totalNum;
    }

    //写入数据..
    public void write(String data) {
        if (!isInitSuccess) {
            logger.error("初始化失败, 不进行写入!");
            return;
        }

        try {
            if (stringBuilder == null) {
                stringBuilder = new StringBuilder();
            }

            stringBuilder.append(data).append("\n");
//            bufferedWriter.append(data);
            //bufferedWriter.newLine();
            totalNum += 1;
            flushNum += 1;
            if (totalNum % 1000 == 0) {
                bufferedWriter.append(stringBuilder.toString());
                stringBuilder = null;
            }

            if (flushNum >= MAX_FLUSH) {
                flushNum = 0;
                bufferedWriter.flush();
                logger.info("强制刷新: {}", fileName);
            }
        } catch (IOException e) {
            logger.error("写入数据失败: {} {}", fileName, data);
            logger.error("ERROR", e);
        }
    }

    //关闭文件句柄
    public void close() {

        try {
            if (stringBuilder != null) {
                bufferedWriter.append(stringBuilder.toString());
                bufferedWriter.flush();
                stringBuilder = null;
            }
        } catch (IOException e) {
            logger.error("ERROR:", e);
        }

        try {
            if (bufferedWriter != null) {
                bufferedWriter.close();
                bufferedWriter = null;
                logger.info("bufferedWriter 关闭成功!!!");
            }

        } catch (IOException e) {
            logger.error("ERROR:", e);
        }

        try {
            if (outputStreamWriter != null) {
                outputStreamWriter.close();
                outputStreamWriter = null;
                logger.info("outputStreamWriter 关闭成功!!!");
            }
        } catch (IOException e) {
            logger.error("ERROR:", e);
        }

        try {
            if (fileOutputStream != null) {
                fileOutputStream.close();
                fileOutputStream = null;
                logger.info("fileOutputStream 关闭成功!!!");
            }
        } catch (IOException e) {
            logger.error("ERROR:", e);
        }

        logger.info("本次文件操作耗时: {} {}ms", fileName, System.currentTimeMillis() - startTime);
    }

    public static boolean moveFile(String source, String target) {
        try {
            //确保移动的目标文件路径上的文件夹是存在的.
            ToolUtil.createPathDirs(target);
            Files.move(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.error("文件移动失败: tmp = {} file = {}", source, target);
            logger.error("ERROR:", e);
            return false;
        }
        return true;
    }

    //删除文件接口
    public static boolean deleteFile(String filePath) {
        try {
            Files.deleteIfExists(Paths.get(filePath));
        } catch (IOException e) {
            logger.error("文件删除失败: file = {}", filePath);
            logger.error("ERROR:", e);
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        FileExport fileExport = new FileExport("test.csv");
        if (fileExport.isInitSuccess()) {
            for (int i = 0; i < 1000000; i++) {
                fileExport.write("dafadfsafdsadf");
            }
        }
        fileExport.close();
    }
}
