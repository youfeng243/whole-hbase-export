package com.haizhi.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResultMsg {

    //返回结果
    private int result;

    //返回的文件名称
    private String zipFileName;

    //文件列表
    private List<Map<String, String>> fileNameList = new ArrayList<>();

    public List<Map<String, String>> getFileNameList() {
        return fileNameList;
    }

    public void setFileNameList(List<Map<String, String>> fileNameList) {
        this.fileNameList = fileNameList;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public String getZipFileName() {
        return zipFileName;
    }

    public void setZipFileName(String zipFileName) {
        this.zipFileName = zipFileName;
    }
}
