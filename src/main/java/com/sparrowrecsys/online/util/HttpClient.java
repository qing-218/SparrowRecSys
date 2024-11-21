package com.sparrowrecsys.online.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class HttpClient {
    /**
     * 异步发送 HTTP POST 请求并获取单一字符串响应
     * @param host 目标服务器地址
     * @param body POST 请求的内容
     * @return 服务器返回的响应内容，或为空字符串表示请求失败
     */
    public static String asyncSinglePostRequest(String host, String body){
        // 如果请求体为空，直接返回 null
        if (null == body || body.isEmpty()){
            return null;
        }

        try {
            // 创建异步 HTTP 客户端
            final CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
            client.start();
            
            // 设置请求体为字节数组形式
            HttpEntity bodyEntity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
            HttpPost request = new HttpPost(host);
            request.setEntity(bodyEntity);

            // 异步发送请求并获取响应
            final Future<HttpResponse> future = client.execute(request, null);
            final HttpResponse response = future.get();

            // 关闭客户端连接
            client.close();

            // 解析响应内容并返回
            return getRespondContent(response);
        }catch (Exception e){
            // 打印异常堆栈信息
            e.printStackTrace();
            return "";
        }
    }

    /**
     * 异步发送 HTTP POST 请求并处理多个键值对请求
     * @param host 目标服务器地址
     * @param bodyMap 包含多个请求体的映射，每个键值对对应一个请求
     * @return 每个请求的响应内容映射，或 null 表示请求失败
     */
    public static Map<String, String> asyncMapPostRequest(String host, Map<String, String> bodyMap) throws Exception {
        // 如果请求映射为空，直接返回 null
        if (null == bodyMap || bodyMap.isEmpty()){
            return null;
        }

        try {
            // 创建异步 HTTP 客户端
            final CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
            client.start();

            // 保存每个请求的 Future 响应对象
            HashMap<String, Future<HttpResponse>> futures = new HashMap<>();
            for (Map.Entry<String, String> bodyEntry : bodyMap.entrySet()) {
                String body = bodyEntry.getValue();
                HttpEntity bodyEntity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
                HttpPost request = new HttpPost(host);
                request.setEntity(bodyEntity);
                futures.put(bodyEntry.getKey(), client.execute(request, null));
            }

            // 保存每个请求的响应内容
            HashMap<String, String> responds = new HashMap<>();
            for (Map.Entry<String, Future<HttpResponse>> future : futures.entrySet()) {
                final HttpResponse response = future.getValue().get();
                responds.put(future.getKey(), getRespondContent(response));
            }

             // 关闭客户端连接并返回响应映射
            client.close();
            return responds;
        }catch (Exception e){
            // 打印异常堆栈信息
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 从 HttpResponse 对象中提取响应内容
     * @param response HTTP 响应对象
     * @return 响应内容字符串
     */
    public static String getRespondContent(HttpResponse response) throws Exception{
        HttpEntity entity = response.getEntity();
        InputStream is = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), 8);
        StringBuilder sb = new StringBuilder();
        String line;
         // 逐行读取响应内容并拼接为字符串
        while ((line = reader.readLine()) != null)
            sb.append(line).append("\n");
        return sb.toString();
    }

    /**
     * 主方法，用于测试 HTTP POST 请求的实现
     * @param args 命令行参数
     */
    public static void main(String[] args){


        //keys must be equal to:
        // movieAvgRating,
        // movieGenre1,movieGenre2,movieGenre3,
        // movieId,
        // movieRatingCount,
        // movieRatingStddev,
        // rating,
        // releaseYear,
        // timestamp,
        // userAvgRating,
        // userAvgReleaseYear,
        // userGenre1,userGenre2,userGenre3,userGenre4,userGenre5,
        // userId,
        // userRatedMovie1,
        // userRatedMovie2,
        // userRatedMovie3,
        // userRatedMovie4,
        // userRatedMovie5,
        // userRatingCount,
        // userRatingStddev,
        // userReleaseYearStddev"
        //}

        // 创建第一个请求的 JSON 对象，包含用户 ID 和电影 ID
        JSONObject instance = new JSONObject();
        instance.put("userId",10351);
        instance.put("movieId",52);

        /*
        instance.put("timestamp",1254725234);
        instance.put("userGenre1","Thriller");
        instance.put("userGenre2","Crime");
        instance.put("userGenre3","Drama");
        instance.put("userGenre4","Comedy");
        instance.put("userGenre5","Action");

        instance.put("movieGenre1","Comedy");
        instance.put("movieGenre2","Drama");
        instance.put("movieGenre3","Romance");

        instance.put("userRatedMovie1",608);
        instance.put("userRatedMovie2",6);
        instance.put("userRatedMovie3",1);
        instance.put("userRatedMovie4",32);
        instance.put("userRatedMovie5",25);

        instance.put("movieId",52);
        instance.put("rating",4.0);

        instance.put("releaseYear",1995);
        instance.put("movieRatingCount",2033);
        instance.put("movieAvgRating",3.54);
        instance.put("movieRatingStddev",0.91);
        instance.put("userRatingCount",7);
        instance.put("userAvgReleaseYear","1995.43");
        instance.put("userReleaseYearStddev",0.53);
        instance.put("userAvgRating",3.86);
        instance.put("userRatingStddev",0.69);*/

        // 创建第二个请求的 JSON 对象
        JSONObject instance2 = new JSONObject();
        instance2.put("userId",10351);
        instance2.put("movieId",53);

        // 将两个请求打包为一个 JSON 数组
        JSONArray instances = new JSONArray();
        instances.put(instance);
        instances.put(instance2);

        // 构造最终的 JSON 请求体
        JSONObject instancesRoot = new JSONObject();
        instancesRoot.put("instances", instances);

        // 打印请求体内容
        System.out.println(instancesRoot.toString());

        // 发送 HTTP POST 请求并打印响应内容
        System.out.println(asyncSinglePostRequest("http://localhost:8501/v1/models/recmodel:predict", instancesRoot.toString()));
    }
}
