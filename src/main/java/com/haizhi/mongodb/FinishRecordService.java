package com.haizhi.mongodb;

import java.util.List;

public interface FinishRecordService {

    FinishRecord findOne(String date);

    boolean updateOne(FinishRecord finishRecord);

    //查找所有没有被记录的批次任务信息
    List<FinishRecord> findAllNotFinish();
}
