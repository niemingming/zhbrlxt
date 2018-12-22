package com.nmm.study.data;

import com.nmm.study.entity.Record;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

@Service
public class ReadData implements InitializingBean{
    //文件记录
    private String path = "F:/个人/alg/SogouQ.reduced";
    @Value("${elasticsearch.address}")
    private String address;
    private RestClient restClient;

    /**
     * 加载数据
     */
    public void loadData() throws Exception {
        //用户session记录
        Map<String,Session> map = new HashMap<String, Session>();

        long currentpos = 0;
        int start = 1556000;

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path),"GBK"));
        String line = null;
        StringBuilder builder = new StringBuilder();
        String endPoint = "/_bulk";
        while ((line = br.readLine()) != null) {
            line = line.replace(".\t",".").replace("\tP","P").replace("、\t","、");
//            if (currentpos > start)
//                System.out.println(line);
            String[] recordinfo = line.trim().split("\t");
            long time = 0;
            int j = 2;
            for (String s : recordinfo[0].split(":")) {
                time += Long.parseLong(s)*Math.pow(60,j--);
            }
            String userId = recordinfo[1];
            String searchWord = recordinfo[2].replace("[","").replace("]","");
            int sort = Integer.parseInt(recordinfo[3].split(" ")[0]);
            int order = Integer.parseInt(recordinfo[3].split(" ")[1]);
            String url = recordinfo[4];
            String id = currentpos + "";
            //获取sessionId
            Session session = map.get(userId);
            if (session == null || time - session.lastTime > 15*60) {
                session = new Session();
                session.sessionId = UUID.randomUUID().toString();
                map.put(userId,session);
            }
            //赋值上次时间
            session.lastTime = time;
            //构建记录
            Record record = new Record();
            record.setId(id);
            record.setOrder(order);
            record.setSort(sort);
            record.setSearchWord(searchWord);
            record.setSessionId(session.sessionId);
            record.setVisitTime(time);
            record.setUserId(userId);
            record.setUrl(url);

            builder.append(record.toBulkStr());

            currentpos++;

            if (currentpos%1000==0) {
                if (currentpos < start) {
                    System.out.println("跳过：" + currentpos + "条！");
                    builder = new StringBuilder();
                    continue;
                }
                StringEntity stringEntity = new StringEntity(builder.toString(),"UTF-8");
                stringEntity.setContentType("application/json;charset=UTF-8");
                Response response = restClient.performRequest("POST",endPoint,new HashMap<String,String>(),stringEntity);
                int code = response.getStatusLine().getStatusCode();
                if (code < 200 || code >= 400) {//发送失败
                    System.out.println(stringEntity);
                }else {
                    System.out.println("成功导入数据：" + currentpos + "条！");
                }
                builder = new StringBuilder();
            }
        }
        if (currentpos%1000>0) {
            StringEntity stringEntity = new StringEntity(builder.toString(),"UTF-8");
            stringEntity.setContentType("application/json;charset=UTF-8");
            Response response = restClient.performRequest("POST",endPoint,new HashMap<String,String>(),stringEntity);
            int code = response.getStatusLine().getStatusCode();
            if (code < 200 || code >= 400) {//发送失败
                System.out.println(stringEntity);
            }else {
                System.out.println("成功导入数据：" + currentpos + "条！");
            }
            builder = new StringBuilder();
        }


    }


    public class Session{
        public String sessionId;
        public long lastTime;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        HttpHost host = new HttpHost(address,9200);
        restClient = RestClient.builder(host).build();
    }
}
