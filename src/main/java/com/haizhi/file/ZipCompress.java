package com.haizhi.file;

import com.haizhi.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by youfeng on 2017/5/26.
 * 压缩类
 */
public class ZipCompress implements Compress {

    private static Logger logger = LoggerFactory.getLogger(ZipCompress.class);

    //待压缩文件路径
    private Map<String, String> filePathList;

    private FileOutputStream zipFileOut = null;
    private ZipOutputStream zipOut = null;

    public ZipCompress(String zipPath, Map<String, String> filePathList) {
        this.filePathList = filePathList;

        try {
            zipFileOut = new FileOutputStream(zipPath);
            zipOut = new ZipOutputStream(new BufferedOutputStream(zipFileOut));
        } catch (FileNotFoundException e) {
            logger.error("ERROR", e);
        }
    }


    //压缩
    @Override
    public Compress compress() {

        if (zipFileOut == null || zipOut == null) {
            logger.error("未初始化, 不能进行压缩...");
            return this;
        }

        byte[] buf = new byte[4096]; //设定读入缓冲区尺寸

        filePathList.forEach((fileName, filePath) -> {
            try {
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(filePath));
                zipOut.putNextEntry(new ZipEntry(fileName));  //设置 ZipEntry 对象

                int length;
                while ((length = in.read(buf, 0, 4096)) != -1) {
                    zipOut.write(buf, 0, length);  //从源文件读出，往压缩文件中写入
                }
                in.close();

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
            if (zipOut != null) {
                zipOut.close();
                zipOut = null;
            }

            if (zipFileOut != null) {
                zipFileOut.close();
                zipFileOut = null;
            }

        } catch (IOException e) {
            logger.error("ERROR", e);
        }
    }

    public static String getFileName(String name) {
        return name + ".zip";
    }

    public static void main(String... args) {

        PropertyUtil.loadProperties("application.properties");
        long startTime = System.currentTimeMillis();
        String path = "/tmp/test";
        String zipName = "/tmp/speed_test.zip";


        File file = new File(path);
        File[] files = file.listFiles();

        Map<String, String> fileMap = new HashMap<>();
        assert files != null;
        for (File file1 : files) {
            fileMap.put(file1.getName(), file1.getAbsolutePath());
            logger.info("name = {} path = {}", file1.getName(), file1.getAbsolutePath());
        }
        new ZipCompress(zipName, fileMap).compress().close();

        logger.info("压缩耗时: {} ms", System.currentTimeMillis() - startTime);
    }
}
