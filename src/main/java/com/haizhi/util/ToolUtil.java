package com.haizhi.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

public class ToolUtil {

    private static Logger logger = LoggerFactory.getLogger(ToolUtil.class);

    private static final String FILE_SEPARATOR = "|@|";
    private static final String ROWKEY_SEPARATOR = "-";
    public static final String TABLE_SEPARATOR = "-";
    public static final String FILENAME_SEPARATOR = "#";

    //转换成base64编码
    public static String stringToBase64(String origin) {
        // 编码
        String asB64 = null;
        try {
            asB64 = Base64.getEncoder().encodeToString(origin.getBytes("utf-8"));
        } catch (UnsupportedEncodingException e) {
            logger.error("ERROR", e);
        }
        return asB64;
    }

    //从base64转换成字符串
    public static String base64ToString(String base64) {
        // 解码
        byte[] asBytes = Base64.getDecoder().decode(base64);

        try {
            return new String(asBytes, "utf-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("ERROR", e);
        }
        return null;
    }

    //根据文件绝对路径创建对应的目录层次
    public static void createPathDirs(String filePath) {

        if (filePath == null || Objects.equals(filePath, "")) {
            logger.error("filepath 不合法..");
            return;
        }

        Path realPath = Paths.get(filePath);
        createPathDirs(realPath);
    }

    //根据文件绝对路径创建对应的目录层次
    public static void createPathDirs(Path filePath) {

        if (filePath == null) {
            logger.error("filepath 不合法..");
            return;
        }

        if (!Files.exists(filePath.getParent())) {
            try {
                Files.createDirectories(filePath.getParent());
                logger.info("文件夹不存在, 创建文件夹: {}", filePath.getParent());
            } catch (IOException e) {
                logger.error("创建文件夹失败..");
                logger.error("ERROR:", e);
            }
        }
    }

    //根据文件夹绝对路径创建对应的目录层次
    public static void createFolder(String filePath) {

        if (filePath == null || Objects.equals(filePath, "")) {
            logger.error("filepath 不合法..");
            return;
        }

        Path realPath = Paths.get(filePath);
        createFolder(realPath);
    }

    //根据文件夹绝对路径创建对应的目录层次
    public static void createFolder(Path filePath) {

        if (filePath == null) {
            logger.error("filepath 不合法..");
            return;
        }

        if (!Files.exists(filePath)) {
            try {
                Files.createDirectories(filePath);
                logger.info("文件夹不存在, 创建文件夹: {}", filePath);
            } catch (IOException e) {
                logger.error("创建文件夹失败..");
                logger.error("ERROR:", e);
            }
        }
    }

    /**
     * 生成插入数据信息
     *
     * @param rawCell hbase单元信息
     * @return Base64(text)
     */
    // {rowkey}|@|{family}|@|{column}|@|{Base64(value)}
    public static String genInsertData(Cell rawCell) {

        return Arrays.toString(CellUtil.cloneRow(rawCell)) + FILE_SEPARATOR +
                Arrays.toString(CellUtil.cloneFamily(rawCell)) + FILE_SEPARATOR +
                Arrays.toString(CellUtil.cloneQualifier(rawCell)) + FILE_SEPARATOR +
                ToolUtil.stringToBase64(Arrays.toString(CellUtil.cloneValue(rawCell)));
    }

    // 生成插入数据
    // {rowkey}|@|{family}|@|{column}|@|{Base64(value)}
    public static String genInsertData(String rowKey, String family, String column, String value) {
        return rowKey + FILE_SEPARATOR +
                family + FILE_SEPARATOR +
                column + FILE_SEPARATOR +
                ToolUtil.stringToBase64(value);
    }

    //生成删除数据信息
    //{rowkey}
    public static String genDeleteData(String rowKey) {
        return rowKey;
    }

    /**
     * 生成工商代码rowkey
     *
     * @param objId mongodb objectId
     * @return 生成的rowkey
     */
    public static String genRowKey(String objId) {
        return objId;
    }

    /**
     * 生成工商代码rowkey
     *
     * @param objId mongodb objectId
     * @return 生成的rowkey
     */
    public static String genRowKey(String objId, String index) {
        return objId +
                ROWKEY_SEPARATOR +
                index;
    }


    public static void main(String[] args) {
        logger.info(genRowKey("58b06035a4e66b7731cb4e1d"));
        logger.info(genRowKey("58b06035a4e66b7731cb4e1d", "1"));

        logger.info(genRowKey("58b06035a4e66b7731cb4e1e"));
        logger.info(genRowKey("58b06035a4e66b7731cb4e1e", "1"));
    }

}
