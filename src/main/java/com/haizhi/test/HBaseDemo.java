package com.haizhi.test;

import com.haizhi.hbase.HBaseDao;
import com.haizhi.util.PropertyUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDemo {

    private static Logger logger = LoggerFactory.getLogger(HBaseDemo.class);

    private List<String> tableList = new ArrayList<>();

    private HBaseDao hBaseBase = null;

    public HBaseDemo(List<String> tableList) {
        this.tableList.addAll(tableList);
        PropertyUtil.loadProperties("application.properties");
        hBaseBase = new HBaseDao(PropertyUtil.getProperty("hbase.zookeeper.quorum"),
                PropertyUtil.getProperty("hbase.zookeeper.property.clientPort"),
                PropertyUtil.getProperty("hbase.master"));
    }

    public int getTableTotalRows() {
        final int[] rowNum = {0};

        tableList.forEach(tableName -> {
            try {
                Table table = hBaseBase.getTable(tableName);

                rowNum[0] += hBaseBase.getRowNum(table);

                table.close();
            } catch (IOException e) {
                logger.error("ERROR:", e);
            }
        });

        return rowNum[0];
    }

    public void showData() {
        final long[] count = {0};
        tableList.forEach(tableName -> {
            try {
                Table table = hBaseBase.getTable(tableName);
                List<Result> resultList = hBaseBase.getAllRow(table);
                resultList.forEach(result -> count[0] += hBaseBase.printResult(result));
                table.close();
            } catch (Exception e) {
                logger.error("ERROR:", e);
            }
        });
        logger.info("总数据行数: {}", count[0]);
    }

    public void getRowData(String tableName, String rowKey) throws Exception {
        logger.info("开始打印一行数据..");
        Table table = hBaseBase.getTable(tableName);

        Result result = hBaseBase.getRow(table, rowKey);

        hBaseBase.printResult(result);

        table.close();
    }

    public static void main(String[] args) throws Exception {
        logger.info("开始table 统计...");

        List<String> tableList = new ArrayList<>();

        tableList.add("gsxt_key_persons");

        new HBaseDemo(tableList).showData();

//        tableList = new ArrayList<>();
//
//        tableList.add("gsxt_main");
//        tableList.add("gsxt_key_person");
//        tableList.add("gsxt_shareholder_information");
//        tableList.add("gsxt_branch");
//        tableList.add("gsxt_changerecords");
//        tableList.add("gsxt_invested_companies");
//        tableList.add("gsxt_contributor_information");
//        tableList.add("gsxt_trademark");
//
//        logger.info(String.valueOf(new HBaseDemo(tableList).getTableTotalRows()));

        //new HBaseDemo(tableList).getRowData("gsxt_key_person", "58b06035a4e66b7731cb5249");
    }
}
