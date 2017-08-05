package com.haizhi.file;

import com.haizhi.util.PropertyUtil;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by youfeng on 2017/5/26.
 * 压缩类
 */
public class SevenZipCompress implements Compress {

    private static Logger logger = LoggerFactory.getLogger(SevenZipCompress.class);

    //待压缩文件路径
    private Map<String, String> filePathList;

    private SevenZOutputFile sevenZOutput = null;

    public SevenZipCompress(String zipPath, Map<String, String> filePathList) {
        this.filePathList = filePathList;

        try {
            sevenZOutput = new SevenZOutputFile(new File(zipPath));
        } catch (IOException e) {
            logger.error("ERROR", e);
        }
    }


    //压缩
    @Override
    public Compress compress() {

        if (sevenZOutput == null) {
            logger.error("未初始化, 不能进行压缩...");
            return this;
        }

        byte[] buf = new byte[4096]; //设定读入缓冲区尺寸
        filePathList.forEach((fileName, filePath) -> {
            try {
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(filePath));
                SevenZArchiveEntry entry = sevenZOutput.createArchiveEntry(new File(filePath), fileName);
                sevenZOutput.putArchiveEntry(entry);
                int length;
                while ((length = in.read(buf, 0, 4096)) != -1) {
                    sevenZOutput.write(buf, 0, length);  //从源文件读出，往压缩文件中写入
                }
                in.close();
                sevenZOutput.closeArchiveEntry();
            } catch (IOException e) {
                logger.error("ERROR", e);
            }
        });

        //关闭压缩文件
        close();
        return this;
    }

    //关闭文件句柄
    @Override
    public void close() {
        try {
            if (sevenZOutput != null) {
                sevenZOutput.close();
                sevenZOutput = null;
            }
        } catch (IOException e) {
            logger.error("ERROR", e);
        }
    }

    public static String getFileName(String name) {
        return name + ".7z";
    }

    public static void main(String... args) {

        PropertyUtil.loadProperties("application.properties");

        long startTime = System.currentTimeMillis();
        String path = "/tmp/test";
        String zipName = "/tmp/test.zip";


        File file = new File(path);
        File[] files = file.listFiles();

        Map<String, String> fileMap = new HashMap<>();
        assert files != null;
        for (File file1 : files) {
            fileMap.put(file1.getName(), file1.getAbsolutePath());
            logger.info("name = {} path = {}", file1.getName(), file1.getAbsolutePath());
        }
        new SevenZipCompress(zipName, fileMap).compress().close();

        logger.info("压缩耗时: {} ms", System.currentTimeMillis() - startTime);
    }

}
