package com.haizhi.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HBaseTest {

    private static Logger logger = LoggerFactory.getLogger(HBaseTest.class);

    private static Connection connection = null;

    private static Admin admin = null;

    static {
        System.setProperty("hadoop.home.dir", "/");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.17.186.40,172.17.186.41,172.17.186.36");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "hdfs://172.17.186.45:16010");

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.error("ERROR:", e);
            System.exit(-1);
        }
    }

    //判断表是否存在
    private static boolean isTableExist(String tableName) throws IOException {
        return getAdmin().tableExists(TableName.valueOf(tableName));
    }

    private static Admin getAdmin() throws IOException {
        return admin;
    }

    private static Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    // 创建数据库表
    private static void createTable(String tableName, String[] columnFamilies)
            throws Exception {

        // 新建一个数据库管理员
        Admin hAdmin = getAdmin();
        if (isTableExist(tableName)) {
            logger.info("表 " + tableName + " 已存在！");
        } else {
            // 新建一个students表的描述
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            // 在描述里添加列族
            for (String columnFamily : columnFamilies) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }
            // 根据配置好的描述建表
            hAdmin.createTable(tableDesc);
            logger.info("创建表 " + tableName + " 成功!");
        }
    }

    // 删除数据库表
    private static void deleteTable(String tableName) throws Exception {
        // 新建一个数据库管理员
        Admin hAdmin = getAdmin();
        if (isTableExist(tableName)) {
            // 关闭一个表
            hAdmin.disableTable(TableName.valueOf(tableName));
            hAdmin.deleteTable(TableName.valueOf(tableName));
            logger.info("删除表 " + tableName + " 成功！");
        } else {
            logger.info("删除的表 " + tableName + " 不存在！");
        }
    }

    // 添加一条数据
    private static void addRow(String tableName, String row,
                               String columnFamily, String column, String value) throws Exception {
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(row));// 指定行
        // 参数分别:列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    // 删除一条(行)数据
    private static void delRow(String tableName, String row) throws Exception {
        Table table = getTable(tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }

    // 删除多条数据
    private static void delMultiRows(String tableName, String[] rows)
            throws Exception {
        Table table = getTable(tableName);
        List<Delete> delList = new ArrayList<>();
        for (String row : rows) {
            Delete del = new Delete(Bytes.toBytes(row));
            delList.add(del);
        }
        table.delete(delList);
    }

    public static void printResult(Result result) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (Cell cell : result.rawCells()) {
            logger.info("rowkey:" + new String(CellUtil.cloneRow(cell)));
            logger.info("timestamp:" + cell.getTimestamp());

            Date date = new Date(cell.getTimestamp());
            logger.info("time:" + simpleDateFormat.format(date));
            logger.info("family:" + new String(CellUtil.cloneFamily(cell)));
            logger.info("column:" + new String(CellUtil.cloneQualifier(cell)));
            logger.info("value:" + new String(CellUtil.cloneValue(cell)));
            logger.info("");

        }
    }

    // 获取一条数据
    private static void getRow(String tableName, String row) throws Exception {
        Table table = getTable(tableName);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        printResult(result);
    }

    // 获取所有数据
    private static void getAllRows(String tableName) throws Exception {
        Table table = getTable(tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);

        do {

            Result[] resultArray = results.next(1000);
            if (resultArray == null) {
                logger.info("读完了...null");
                break;
            }
            if (resultArray.length <= 0) {
                logger.info("读完了...0");
                break;
            }
            logger.info("读取的数量为: {}", resultArray.length);

            // 输出结果
            for (Result result : resultArray) {
                printResult(result);
            }
        } while (true);

        results.close();
    }

    private static List<String> listTables() {
        logger.info("listTables tables.");
        List<String> tblArr = new ArrayList<>();
        try {
            Admin admin = getAdmin();
            TableName[] tableNames = admin.listTableNames();

            for (TableName tableName : tableNames) {
                tblArr.add(tableName.getNameAsString());
                logger.info("Table: " + tableName.getNameAsString());
            }
        } catch (IOException e) {
            logger.error("ERROR:", e);
        }
        return tblArr;
    }

    // 主函数
    public static void main(String[] args) throws Exception {

        listTables();

        String tableName = "_record_change";
//        // 第一步：创建数据库表：“student”
//        String[] columnFamines = {"info", "course"};
//        HBaseTest.createTable(tableName, columnFamines);
//        // 第二步：向数据表的添加数据
//        // 添加第一行数据
//        if (isTableExist(tableName)) {
//            long startTime = System.currentTimeMillis();
//            for (int i = 0; i <= 10000; i++) {
//                HBaseTest.addRow(tableName, String.valueOf(i), "info", "age", "20");
//                HBaseTest.addRow(tableName, String.valueOf(i), "info", "sex", "boy");
//                HBaseTest.addRow(tableName, String.valueOf(i), "course", "china", "97");
//                HBaseTest.addRow(tableName, String.valueOf(i), "course", "math", "128");
//                HBaseTest.addRow(tableName, String.valueOf(i), "course", "english", "85");
//            }
//            logger.info("插入10000条数据耗时: {}ms", System.currentTimeMillis() - startTime);
//
//            HBaseTest.addRow(tableName, "zpc", "info", "age", "20");
//            HBaseTest.addRow(tableName, "zpc", "info", "sex", "boy");
//            HBaseTest.addRow(tableName, "zpc", "course", "china", "97");
//            HBaseTest.addRow(tableName, "zpc", "course", "math", "128");
//            HBaseTest.addRow(tableName, "zpc", "course", "english", "85");
//            // 添加第二行数据
//            HBaseTest.addRow(tableName, "henjun", "info", "age", "19");
//            HBaseTest.addRow(tableName, "henjun", "info", "sex", "boy");
//            HBaseTest.addRow(tableName, "henjun", "course", "china", "90");
//            HBaseTest.addRow(tableName, "henjun", "course", "math", "120");
//            HBaseTest.addRow(tableName, "henjun", "course", "english", "90");
//            // 添加第三行数据
//            HBaseTest.addRow(tableName, "niaopeng", "info", "age", "18");
//            HBaseTest.addRow(tableName, "niaopeng", "info", "sex", "girl");
//            HBaseTest.addRow(tableName, "niaopeng", "course", "china", "100");
//            HBaseTest.addRow(tableName, "niaopeng", "course", "math", "100");
//            HBaseTest.addRow(tableName, "niaopeng", "course", "english", "99");
//            // 第三步：获取一条数据
//            logger.info("**************获取一条(zpc)数据*************");
//            HBaseTest.getRow(tableName, "zpc");
//            // 第四步：获取所有数据
//            logger.info("**************获取所有数据***************");
//            HBaseTest.getAllRows(tableName);
//
//            // 第五步：删除一条数据
//            logger.info("************删除一条(zpc)数据************");
//            HBaseTest.delRow(tableName, "zpc");
//            HBaseTest.getAllRows(tableName);
//            // 第六步：删除多条数据
//            logger.info("**************删除多条数据***************");
//            String rows[] = new String[]{"qingqing", "xiaoxue"};
//            HBaseTest.delMultiRows(tableName, rows);
        HBaseTest.getAllRows(tableName);
        // 第七步：删除数据库
        //logger.info("***************删除数据库表**************");
        //HBaseTest.deleteTable(tableName);
        logger.info("表" + tableName + "存在吗？" + isTableExist(tableName));
//        } else {
//            logger.info(tableName + "此数据库表不存在！");
//        }

    }

}