package com.haizhi.script;

import com.haizhi.mongodb.Mongo;
import com.haizhi.util.PropertyUtil;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by youfeng on 2017/5/29.
 * 任务表操作相关
 */
public class TaskMongo {
    private static final Logger logger = LoggerFactory.getLogger(TaskMongo.class);

    private Mongo taskMongo;

    // 数据库
    private MongoDatabase mongoDatabase;

    static {
        //先装在配置信息
        PropertyUtil.loadProperties("application.properties");
    }

    public TaskMongo() {
        //mongodb句柄
        taskMongo = new Mongo(PropertyUtil.getProperty("mongo.host"),
                PropertyUtil.getProperty("mongo.username"),
                PropertyUtil.getProperty("mongo.password"),
                PropertyUtil.getProperty("mongo.auth.db"));

        mongoDatabase = taskMongo.getDb(PropertyUtil.getProperty("mongo.database"));
    }

    public void deleteTaskTable() {
        mongoDatabase.listCollectionNames().forEach((Consumer<? super String>) collectionName -> {

            if (collectionName.startsWith("hbase_export_task") ||
                    collectionName.startsWith("daily_task") ||
                    Objects.equals(collectionName, "finish_record")) {
                logger.info("当前删除表: {}", collectionName);
                mongoDatabase.getCollection(collectionName).drop();
            }
        });
    }

    public static void main(String... args) {
        new TaskMongo().deleteTaskTable();
    }
}
