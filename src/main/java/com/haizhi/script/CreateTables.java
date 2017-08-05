package com.haizhi.script;

import com.haizhi.hbase.HBaseDao;
import com.haizhi.util.PropertyUtil;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class CreateTables {

    private static final Logger logger = LoggerFactory.getLogger(CreateTables.class);

    private HBaseDao hBaseDao;

    static {
        //先装在配置信息
        PropertyUtil.loadProperties("application.properties");
    }

    public CreateTables() throws Exception {
        hBaseDao = new HBaseDao(PropertyUtil.getProperty("hbase.zookeeper.quorum"),
                PropertyUtil.getProperty("hbase.zookeeper.property.clientPort"),
                PropertyUtil.getProperty("hbase.master"));
    }

    public void createTable(String tableName) throws Exception {
        String[] family = new String[]{"data"};
        hBaseDao.createTable(tableName, family);
    }

    public void deteleTable(String tableName) throws Exception {
        hBaseDao.deleteTable(tableName);
    }

    public void deteleTables() throws Exception {

        hBaseDao.deleteTable("gsxt__src");
        hBaseDao.deleteTable("gsxt_branch");
        hBaseDao.deleteTable("gsxt_changerecords");
        hBaseDao.deleteTable("gsxt_contributor_information");
        hBaseDao.deleteTable("gsxt_invested_companies");
        hBaseDao.deleteTable("gsxt_investor_change");
        hBaseDao.deleteTable("gsxt_key_person");
        hBaseDao.deleteTable("gsxt_list");

        hBaseDao.deleteTable("gsxt_main");
        hBaseDao.deleteTable("gsxt_shareholder_information");
        hBaseDao.deleteTable("gsxt_src");
        hBaseDao.deleteTable("gsxt_trademark");
        hBaseDao.deleteTable("gsxt_used_name_list");
        hBaseDao.deleteTable("_record_change");

        logger.info("删除表完成...");
    }

    private void deleteRecords(String confFile) throws FileNotFoundException {

        String tempString;
        BufferedReader reader = null;
        InputStream is = null;
        InputStreamReader inputStreamReader = null;
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        Table table = null;
        try {
            is = classloader.getResourceAsStream(confFile);
            inputStreamReader = new InputStreamReader(is);
            reader = new BufferedReader(inputStreamReader);
            table = hBaseDao.getTable(confFile);
            while ((tempString = reader.readLine()) != null) {
                logger.info(tempString);
                hBaseDao.delRow(table, tempString);
            }
        } catch (Exception e) {
            logger.error("ERROR", e);
        } finally {

            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error("ERROR", e);
                }
            }

            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("ERROR", e);
            }

            if (inputStreamReader != null) {
                try {
                    inputStreamReader.close();
                } catch (IOException e) {
                    logger.error("ERROR", e);
                }
            }

            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error("ERROR", e);
                }
            }
        }
    }

    public static void main(String... args) throws Exception {
        //logger.info("删除HBase所有工商系列表..");

        CreateTables createTables = new CreateTables();
        createTables.createTable("rc-enterprise_data_gov");
        createTables.createTable("rc-shixin_info");
        createTables.createTable("rc-zhixing_info");
        //new DeleteTables().deteleTable("gsxt_key_persons");
    }
}
