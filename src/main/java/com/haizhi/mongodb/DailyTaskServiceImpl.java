package com.haizhi.mongodb;

import com.haizhi.util.PropertyUtil;
import com.haizhi.util.TimeUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

public class DailyTaskServiceImpl implements DailyTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DailyTaskService.class);

    //mongodb句柄
    private ConcurrentMap<String, MongoCollection<Document>> collectionMap = new ConcurrentHashMap<>();

    // 数据库名称
    private String dbName;

    //更新设置
    private final FindOneAndUpdateOptions updateOptions;


    public DailyTaskServiceImpl() {
        dbName = PropertyUtil.getProperty("mongo.database");

        updateOptions = new FindOneAndUpdateOptions();
        updateOptions.upsert(true);
    }

    // 获取mongodb句柄
    private MongoCollection<Document> getCollection(String collectionName) {
        if (collectionMap.containsKey(collectionName)) {
            return collectionMap.get(collectionName);
        }

        MongoCollection<Document> collection = MongoManager.getDb(dbName).getCollection(collectionName);
        collectionMap.put(collectionName, collection);
        return collection;
    }

    private DailyTask documentToDailyTask(Document document) {

        if (document == null) {
            return null;
        }

        DailyTask dailyTask = new DailyTask();

        dailyTask.setDate(document.getString("_id"));
        dailyTask.setCreateTime(document.getString("createTime"));
        dailyTask.setTaskPeriod(document.getLong("taskPeriod"));
        dailyTask.setUpdateTime(document.getString("updateTime"));
        dailyTask.setFinish(document.getBoolean("finish"));

        List<Document> subList = (List<Document>) document.get("periodList");
        if (subList != null) {
            List<PeriodTask> periodList = new ArrayList<>();
            for (Document doc : subList) {
                PeriodTask periodTask = new PeriodTask();
                periodTask.setCreateTime(doc.getString("createTime"));
                periodTask.setEndTimeStamp(doc.getLong("endTimeStamp"));
                periodTask.setEndTime(doc.getString("endTime"));
                periodTask.setStartTimeStamp(doc.getLong("startTimeStamp"));
                periodTask.setStartTime(doc.getString("startTime"));
                periodTask.setErrorTimes(doc.getInteger("errorTimes"));
                periodTask.setPeriod(doc.getString("period"));
                periodTask.setStatus(doc.getInteger("status"));
                periodTask.setUpdateTime(doc.getString("updateTime"));
                periodTask.setZipFileName(doc.getString("zipFileName"));

                List<Document> fileNameDoc = (List<Document>) doc.get("fileNameList");
                if (fileNameDoc != null) {
                    List<Map<String, String>> fileNameList = new ArrayList<>();
                    for (Document fileDoc : fileNameDoc) {

                        Map<String, String> sub = new HashMap<>();
                        fileDoc.forEach((key, value) -> {
                            sub.put(key, (String) value);
                        });

                        fileNameList.add(sub);
                    }
                    periodTask.setFileNameList(fileNameList);
                }

                periodList.add(periodTask);
            }
            dailyTask.setPeriodList(periodList);
        }

        return dailyTask;
    }

    private Document dailyTaskToDocument(DailyTask dailyTask) {

        if (dailyTask == null) {
            return null;
        }

        Document document = new Document();
        List<Document> periodList = new ArrayList<>();

        for (PeriodTask periodTask : dailyTask.getPeriodList()) {
            Document subDoc = new Document();
            subDoc.append("createTime", periodTask.getCreateTime());
            subDoc.append("endTimeStamp", periodTask.getEndTimeStamp());
            subDoc.append("endTime", periodTask.getEndTime());
            subDoc.append("startTimeStamp", periodTask.getStartTimeStamp());
            subDoc.append("startTime", periodTask.getStartTime());
            subDoc.append("errorTimes", periodTask.getErrorTimes());
            subDoc.append("period", periodTask.getPeriod());
            subDoc.append("status", periodTask.getStatus());
            subDoc.append("updateTime", periodTask.getUpdateTime());
            subDoc.append("zipFileName", periodTask.getZipFileName());

            List<Document> fileNameDoc = new ArrayList<>();
            for (Map<String, String> sub : periodTask.getFileNameList()) {
                Document fileSubDoc = new Document();
                sub.forEach(fileSubDoc::append);
                fileNameDoc.add(fileSubDoc);
            }
            subDoc.append("fileNameList", fileNameDoc);

            periodList.add(subDoc);
        }

        document.append("_id", dailyTask.getDate()).
                append("createTime", dailyTask.getCreateTime()).
                append("taskPeriod", dailyTask.getTaskPeriod()).
                append("updateTime", dailyTask.getUpdateTime()).
                append("finish", dailyTask.getFinish()).
                append("periodList", periodList);

        return document;
    }

    @Override
    public DailyTask findOne(String _id, String collectionName) {

        if (_id == null) {
            logger.error("传入查询参数错误: date = null");
            return null;
        }

        try {
            return documentToDailyTask(getCollection(collectionName).find(new Document("_id", _id)).first());
        } catch (Exception e) {
            logger.error("访问mongodb失败 findone..{}", _id);
            logger.error("ERROR", e);
        }

        return null;
    }

    @Override
    public boolean deleteOne(String _id, String collectionName) {

        if (_id == null) {
            logger.error("传入删除参数错误: date = null");
            return false;
        }

        try {
            getCollection(collectionName).deleteOne(new Document("_id", _id));
            return true;
        } catch (Exception e) {

            logger.error("访问mongodb失败 deleteOne..{}", _id);
            logger.error("ERROR", e);
        }

        return false;
    }

    @Override
    public boolean updateOne(DailyTask dailyTask, String collectionName) {
        if (dailyTask == null) {
            logger.error("传入更新参数错误: date = null");
            return false;
        }

        dailyTask.setUpdateTime(TimeUtil.getCurrentTime());

        Document document = dailyTaskToDocument(dailyTask);
        if (document != null) {
            document.remove("_id");
            getCollection(collectionName).findOneAndUpdate(new Document("_id", dailyTask.getDate()),
                    new Document().append("$set", document), updateOptions);
            return true;
        }
        return false;
    }

    @Override
    public boolean insertOne(DailyTask dailyTask, String collectionName) {
        if (dailyTask == null) {
            logger.error("传入插入参数错误: date = null");
            return false;
        }

        Document document = dailyTaskToDocument(dailyTask);
        if (document != null) {
            document.remove("_id");
            getCollection(collectionName).findOneAndUpdate(new Document("_id", dailyTask.getDate()),
                    new Document().append("$set", document), updateOptions);
            return true;
        }

        return false;
    }

    @Override
    public void createIndexs(String index, String collectionName) {
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.background(true);
        indexOptions.name(index);
        getCollection(collectionName).createIndex(Indexes.ascending(index), indexOptions);
    }

    @Override
    public List<DailyTask> findAllByAllFinishIsFalse(String collectionName) {

        List<DailyTask> resultList = new ArrayList<>();
        try {
            for (Document document : getCollection(collectionName).find(or(eq("finish", false),
                    eq("finish", null)))) {
                DailyTask task = documentToDailyTask(document);
                if (task != null) {
                    resultList.add(task);
                }
            }
        } catch (Exception e) {
            logger.error("查询未完成任务失败:", e);
        }
        return resultList;
    }

    @Override
    public void deleteAllByAllFinishIsTrueAndDateLessThan(String date, String collectionName) {
        try {

            getCollection(collectionName).deleteMany(new Document("finish", true).
                    append("_id", new Document("$lte", date)));
        } catch (Exception e) {
            logger.error("删除已完成任务失败:", e);
        }
    }

    @Override
    public List<String> getAllCollectionNameList() {

        List<String> colNameList = new ArrayList<>();
        for (String collection : MongoManager.getDb(dbName).listCollectionNames()) {
            colNameList.add(collection);
        }

        return colNameList;
    }

    @Override
    public List<String> getFilterCollectionNameList(String filter) {
        List<String> collectList = getAllCollectionNameList().
                parallelStream().
                filter(tableName -> tableName.startsWith(filter)).
                collect(Collectors.toList());

        //打印出所有记录表
        logger.info("开始打印所有任务表信息:");
        collectList.forEach(logger::info);
        logger.info("任务表信息打印结束!");

        return collectList;
    }

    public static void main(String... args) {
        PropertyUtil.loadProperties("application.properties");
        DailyTaskServiceImpl dailyTaskService = new DailyTaskServiceImpl();

        dailyTaskService.deleteAllByAllFinishIsTrueAndDateLessThan("20170524", "hbase_export_task");

        dailyTaskService.getAllCollectionNameList().forEach(logger::info);
    }

}
