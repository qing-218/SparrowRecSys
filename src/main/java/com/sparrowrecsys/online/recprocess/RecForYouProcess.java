#导入相应的包
package com.sparrowrecsys.online.recprocess;

import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.User;
import com.sparrowrecsys.online.datamanager.Movie;
import com.sparrowrecsys.online.datamanager.RedisClient;
import com.sparrowrecsys.online.util.Config;
import com.sparrowrecsys.online.util.Utility;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

import static com.sparrowrecsys.online.util.HttpClient.asyncSinglePostRequest;

/**
 * 推荐相似电影的推荐流程
 *

public class RecForYouProcess {

   /**
     * 获取推荐电影列表
     * @param userId 输入的用户 ID
     * @param size 推荐列表的大小
     * @param model 用于计算相似度的模型
     * @return 推荐的电影列表
     */
    public static List<Movie> getRecList(int userId, int size, String model){
        User user = DataManager.getInstance().getUserById(userId);
        if (null == user){
            return new ArrayList<>();
        }
        final int CANDIDATE_SIZE = 800;
       //通过 DataManager 根据评分获取前 800 部电影作为候选集。
        List<Movie> candidates = DataManager.getInstance().getMovies(CANDIDATE_SIZE, "rating");

         // 如果数据源是 Redis，从 Redis 加载用户嵌入向量
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_REDIS)){
            String userEmbKey = "uEmb:" + userId;
            String userEmb = RedisClient.getInstance().get(userEmbKey);
           //如果获取到数据，使用 Utility.parseEmbStr 将字符串解析为向量，并存储到用户对象中。
            if (null != userEmb){
                user.setEmb(Utility.parseEmbStr(userEmb));
            }
        }
         // 如果从 Redis 加载用户特征
        if (Config.IS_LOAD_USER_FEATURE_FROM_REDIS){
            String userFeaturesKey = "uf:" + userId;
           //通过 Redis 的 hgetAll 方法获取用户特征（以哈希结构存储）。
            Map<String, String> userFeatures = RedisClient.getInstance().hgetAll(userFeaturesKey);
            if (null != userFeatures){
                user.setUserFeatures(userFeatures);
            }
        }
       //依据 user 信息、候选电影 candidates 和指定的推荐模型 model 生成排序结果。
        List<Movie> rankedList = ranker(user, candidates, model);

        if (rankedList.size() > size){
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    /**
     * 对候选集进行排序
     * @param user 输入用户
     * @param candidates 候选电影列表
     * @param model 用于排序的模型名称
     * @return 排序后的电影列表
     */
    public static List<Movie> ranker(User user, List<Movie> candidates, String model){
       //使用 HashMap 存储每个候选电影的分数。
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();

        switch (model){
            //计算用户与候选电影的嵌入向量相似度
            case "emb":
                for (Movie candidate : candidates){
                    double similarity = calculateEmbSimilarScore(user, candidate);
                    candidateScoreMap.put(candidate, similarity);
                }
                break;
            //调用神经协同过滤服务 (callNeuralCFTFServing)，为每部电影生成分数。
            case "nerualcf":
                callNeuralCFTFServing(user, candidates, candidateScoreMap);
                break;
            default:
                // 候选集默认排序
                for (int i = 0 ; i < candidates.size(); i++){
                    candidateScoreMap.put(candidates.get(i), (double)(candidates.size() - i));
                }
        }

        List<Movie> rankedList = new ArrayList<>();
        candidateScoreMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(m -> rankedList.add(m.getKey()));
        return rankedList;
    }

   /**
     * 基于嵌入向量计算相似度得分
     * @param user 输入用户
     * @param candidate 候选电影
     * @return 相似度得分
     */
    public static double calculateEmbSimilarScore(User user, Movie candidate){
        if (null == user || null == candidate || null == user.getEmb()){
            return -1;
        }
        return user.getEmb().calculateSimilarity(candidate.getEmb());
    }

    /**
     * 调用 TensorFlow Serving 获取 NeuralCF 模型的推理结果
     * @param user 输入用户
     * @param candidates 候选电影列表
     * @param candidateScoreMap 保存预测分数的映射
     */
    public static void callNeuralCFTFServing(User user, List<Movie> candidates, HashMap<Movie, Double> candidateScoreMap){
       //检查输入是否合法、用户对象是否为空、候选电影列表是否为空或长度为零
        if (null == user || null == candidates || candidates.size() == 0){
            return;
        }
       
         //为每部候选电影创建 JSON 对象，将其 userId 和 movieId 添加到 instances 数组中
        JSONArray instances = new JSONArray();
        for (Movie m : candidates){
            JSONObject instance = new JSONObject();
            instance.put("userId", user.getUserId());
            instance.put("movieId", m.getMovieId());
            instances.put(instance);
        }

        JSONObject instancesRoot = new JSONObject();
        instancesRoot.put("instances", instances);

        // 需要确认 TensorFlow Serving 的终端地址
        String predictionScores = asyncSinglePostRequest("http://localhost:8501/v1/models/recmodel:predict", instancesRoot.toString());
        System.out.println("send user" + user.getUserId() + " request to tf serving.");

        JSONObject predictionsObject = new JSONObject(predictionScores);
        JSONArray scores = predictionsObject.getJSONArray("predictions");
        for (int i = 0 ; i < candidates.size(); i++){
            candidateScoreMap.put(candidates.get(i), scores.getJSONArray(i).getDouble(0));
        }
    }
}
