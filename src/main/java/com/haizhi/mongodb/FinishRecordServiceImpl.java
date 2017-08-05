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

public class FinishRecordServiceImpl implements FinishRecordService {

    private static final Logger logger = LoggerFactory.getLogger(FinishRecordServiceImpl.class);

    //数据库句柄
    private MongoCollection<Document> collection;

    //更新设置
    private final FindOneAndUpdateOptions updateOptions;

    public FinishRecordServiceImpl() {
        String dbName = PropertyUtil.getProperty("mongo.database");
        String collectionName = PropertyUtil.getProperty("mongo.collection.finishrecord");
        collection = MongoManager.getDb(dbName).getCollection(collectionName);

        updateOptions = new FindOneAndUpdateOptions();
        updateOptions.upsert(true);

        //创建索引
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.background(true);
        indexOptions.name("finish");
        collection.createIndex(Indexes.ascending("finish"), indexOptions);
    }

    private FinishRecord documentToFinishRecord(Document document) {
        if (document == null) {
            return null;
        }
        FinishRecord finishRecord = new FinishRecord();

        finishRecord.setCreateTime(document.getString("createTime"));
        finishRecord.setDate(document.getString("_id"));
        finishRecord.setFinish(document.getBoolean("finish"));
        finishRecord.setUpdateTime(document.getString("updateTime"));

        Document subDoc = (Document) document.get("taskMap");
        if (subDoc == null) {
            return finishRecord;
        }

        Map<String, FinishRecord.SubTask> taskMap = new HashMap<>();

        //遍历表信息
        subDoc.forEach((key, value) -> {
            Document doc = (Document) value;
            FinishRecord.SubTask subTask = new FinishRecord.SubTask();
            subTask.setFinish(doc.getBoolean("finish"));
            subTask.setUpdateTime(doc.getString("updateTime"));

//            Map<String, String> zipMap = new HashMap<>();
//            Map<String, String> updateMap = new HashMap<>();
//            Map<String, String> deleteMap = new HashMap<>();
//
//            Document zipDoc = (Document) doc.get("zipMap");
//            if (zipDoc != null) {
//                zipDoc.forEach((k, v) -> zipMap.put(k, (String) v));
//                subTask.setZipMap(zipMap);
//            }
//            Document updateDoc = (Document) doc.get("updateMap");
//            if (updateDoc != null) {
//                updateDoc.forEach((k, v) -> updateMap.put(k, (String) v));
//                subTask.setUpdateMap(updateMap);
//            }
//            Document deleteDoc = (Document) doc.get("deleteMap");
//            if (deleteDoc != null) {
//                deleteDoc.forEach((k, v) -> deleteMap.put(k, (String) v));
//                subTask.setDeleteMap(deleteMap);
//            }

            taskMap.put(key, subTask);
        });

        finishRecord.setTaskMap(taskMap);
        return finishRecord;
    }

    private Document finishRecordToDocument(FinishRecord finishRecord) {
        if (finishRecord == null) {
            return null;
        }
        Document document = new Document();

        document.append("_id", finishRecord.getDate());
        document.append("createTime", finishRecord.getCreateTime());
        document.append("finish", finishRecord.getFinish());
        document.append("updateTime", finishRecord.getUpdateTime());

        Document taskMap = new Document();
        finishRecord.getTaskMap().forEach(((s, subTask) -> {
            Document subDoc = new Document();

            subDoc.append("finish", subTask.getFinish());
            subDoc.append("updateTime", subTask.getUpdateTime());

            taskMap.append(s, subDoc);
        }));

        document.append("taskMap", taskMap);
        return document;
    }

    @Override
    public FinishRecord findOne(String date) {

        if (date == null) {
            logger.error("传入查询参数错误: date = null");
            return null;
        }

        try {

            return documentToFinishRecord(collection.find(new Document("_id", date)).first());
        } catch (Exception e) {
            logger.error("访问mongodb失败 findone..{}", date);
            logger.error("ERROR", e);
        }
        return null;
    }

    @Override
    public boolean updateOne(FinishRecord finishRecord) {
        if (finishRecord == null) {
            logger.error("传入查询参数错误: finishRecord = null");
            return false;
        }

        try {
            //更新时间
            finishRecord.setUpdateTime(TimeUtil.getCurrentTime());

            //转换格式
            Document document = finishRecordToDocument(finishRecord);
            if (document == null) {
                return false;
            }

            document.remove("_id");
            collection.findOneAndUpdate(new Document("_id", finishRecord.getDate()),
                    new Document().append("$set", document), updateOptions);
        } catch (Exception e) {
            logger.error("访问mongodb失败 updateOne..{}", finishRecord);
            logger.error("ERROR", e);
            return false;
        }

        return true;
    }

    @Override
    public List<FinishRecord> findAllNotFinish() {

        List<FinishRecord> finishRecordList = new ArrayList<>();

        //查找record 为false的
        for (Document document : collection.find(new Document("finish", false))) {
            FinishRecord finishRecord = documentToFinishRecord(document);
            if (finishRecord != null) {
                finishRecordList.add(finishRecord);
            }
        }

        return finishRecordList;
    }

//    public void addZipInfo(FinishRecord finishRecord, String collectionName, String zipName) {
//        if( finishRecord == null ||
//                collectionName == null ||
//                zipName == null) {
//            logger.error("参数错误!!");
//            return;
//        }
//
//        finishRecord.getTaskMap().get(collectionName).setUpdateTime(TimeUtil.getCurrentTime());
//        finishRecord.getTaskMap().get(collectionName).getZipMap().put(zipName, zipName);
//    }
//
//    public void addUpdateInfo(FinishRecord finishRecord, String collectionName, String updateName) {
//        if( finishRecord == null ||
//                collectionName == null ||
//                updateName == null) {
//            logger.error("参数错误!!");
//            return;
//        }
//
//        finishRecord.getTaskMap().get(collectionName).setUpdateTime(TimeUtil.getCurrentTime());
//        finishRecord.getTaskMap().get(collectionName).getUpdateMap().put(updateName, updateName);
//    }
//
//    public void addDeleteInfo(FinishRecord finishRecord, String collectionName, String deleteName) {
//        if( finishRecord == null ||
//                collectionName == null ||
//                deleteName == null) {
//            logger.error("参数错误!!");
//            return;
//        }
//        finishRecord.getTaskMap().get(collectionName).setUpdateTime(TimeUtil.getCurrentTime());
//        finishRecord.getTaskMap().get(collectionName).getDeleteMap().put(deleteName, deleteName);
//    }

}
