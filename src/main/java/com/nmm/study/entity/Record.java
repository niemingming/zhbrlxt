package com.nmm.study.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.UUID;

@Data
public class Record {

    private long visitTime;
    private String userId;
    private String searchWord;
    private String url;
    private int sort;
    private int order;
    private String sessionId;
    private String id;

    public String toBulkStr(){
        JSONObject obj = (JSONObject) JSONObject.toJSON(this);
        obj.remove("id");
        obj.remove("quesGroupId");
        StringBuilder json = new StringBuilder();
        json.append("{\"index\":{\"_index\":\"").append("sougou-123")
                .append("\",\"_type\":\"doc\",\"_id\":\"").append(id).append("\"}}\n").append(obj.toJSONString()).append("\n");
        return json.toString();
    }
}
