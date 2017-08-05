package com.haizhi.util;

import com.haizhi.hbase.HBaseDao;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 生成记录表存储信息
 */
public class GenRecord {

    private static final Logger logger = LoggerFactory.getLogger(GenRecord.class);

    //列名 column
    public static final String COLUMN_INSERT = "insert";
    public static final String COLUMN_UPDATE = "update";
    public static final String COLUMN_DELETE = "delete";

    public static final String COLUMN_FAMILY = "data";

    // 需要插入或者更新的数据
    public Map<String, Map<String, String>> genData;

    // 引用rowkey
    private String refRowKey;

    // 列信息
    private String column;

    // rowKey
    private String rowKey;

    public GenRecord(String tableName, String refRowKey, String column) {
        genData = new HashMap<>();
        this.refRowKey = refRowKey;
        this.column = column;

        // rowKey生成规则为 业务表 rowKey + # + 表名
        rowKey = refRowKey + "#" + tableName;
    }

    //获得记录表rowkey
    public String getRowKey() {
        return rowKey;
    }

    // 获得列族
    public String getColumnFamily() {
        return COLUMN_FAMILY;
    }

    // 获得value信息
    public String getValue() {

        if (Objects.equals(column, COLUMN_DELETE)) {
            return refRowKey;
        }

        if (Objects.equals(column, COLUMN_INSERT) || Objects.equals(column, COLUMN_UPDATE)) {
            return JsonUtil.objectToJson(genData);
        }

        return "ERROR: not support type!";
    }

    //获得列名
    public String getColumn() {
        return column;
    }

    public void put(String family, Map<String, String> map) {
        genData.put(family, map);
    }

    public static void main(String[] args) throws Exception {
        String tableName = "_record_change";
        PropertyUtil.loadProperties("application.properties");
        HBaseDao hBaseBase = new HBaseDao(PropertyUtil.getProperty("hbase.zookeeper.quorum"),
                PropertyUtil.getProperty("hbase.zookeeper.property.clientPort"),
                PropertyUtil.getProperty("hbase.master"));

        GenRecord genRecord = new GenRecord("gsxt_main",
                ToolUtil.genRowKey("58b06035a4e66b7731cb4e1d"),
                GenRecord.COLUMN_INSERT);

        //存储需要放入的数据
        Map<String, String> data = new HashMap<>();
        data.put("period", "2003-12-17至2013-12-16");
        data.put("business_status", "吊销");
        data.put("province", "福建");
        //....
        //....
        genRecord.put("data", data);

        logger.info("生成的rowKey = {}", genRecord.getRowKey());
        logger.info("生成的columnFamily = {}", genRecord.getColumnFamily());
        logger.info("生成的column = {}", genRecord.getColumn());
        logger.info("生成的value = {}", genRecord.getValue());

        Table table = hBaseBase.getTable(tableName);
        hBaseBase.addRow(table, genRecord.getRowKey(),
                genRecord.getColumnFamily(),
                genRecord.getColumn(),
                genRecord.getValue());

        logger.info("存储完成...");

    }
}
