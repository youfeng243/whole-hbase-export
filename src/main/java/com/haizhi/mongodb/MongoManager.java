package com.haizhi.mongodb;

import com.haizhi.util.PropertyUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * MongoDB连接管理
 *
 * @author youfeng
 * @version 1.0
 * @company 海致网络科技有限公司
 * @date 2017年5月9日
 */
public class MongoManager {

    private static final Logger logger = LoggerFactory.getLogger(MongoManager.class);

    private static MongoClient client;

    private static void init() {
        List<ServerAddress> addressList = new ArrayList<>();
        String host = PropertyUtil.getProperty("mongo.host");
        String[] hosts = host.split(",");
        for (String hostName : hosts) {
            addressList.add(splitHost(hostName));
        }
        List<MongoCredential> list = new ArrayList<>();
        MongoCredential credential = MongoCredential.createCredential(
                PropertyUtil.getProperty("mongo.username"),
                PropertyUtil.getProperty("mongo.auth.db"),
                PropertyUtil.getProperty("mongo.password").toCharArray());
        list.add(credential);

        //增加超时控制
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.socketKeepAlive(true);
        builder.connectTimeout(30000);
        builder.socketTimeout(30000);
        builder.maxConnectionIdleTime(60000);

        client = new MongoClient(addressList, list, builder.build());
        logger.info("mongodb初始化完成..");
    }

    private static ServerAddress splitHost(String host) {
        String[] hostName = host.split(":");
        return new ServerAddress(hostName[0], Integer.parseInt(hostName[1]));
    }

    public static MongoDatabase getDb(String databaseName) {

        if (client == null) {
            init();
        }
        return client.getDatabase(databaseName);
    }

    public static void close() {
        if (client != null) {
            client.close();
            client = null;
            logger.info("mongodb 关闭成功..");
        }
    }

    public static void main(String[] args) {

    }
}
