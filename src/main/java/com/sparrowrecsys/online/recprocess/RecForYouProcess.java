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

/**
 * 处理推荐相似电影的推荐流程。
 * 这个类包含了从数据源获取用户和电影信息、计算相似度、
 * 调用外部服务以及生成推荐列表的逻辑。
 */
public class RecForYouProcess {

    /**
     * 获取推荐电影列表。
     * <p>
     * 这个方法首先获取用户信息，然后从数据源中获取候选电影列表。
     * 如果配置指定使用Redis作为嵌入向量的来源，它会从Redis中加载用户的嵌入向量。
     * 同样，如果配置指定从Redis加载用户特征，它也会从Redis中加载这些特征。
     * 最后，它使用传入的模型来对候选电影进行排序，并返回指定数量的推荐电影。
     * </p>
     *
     * @param userId 输入的用户 ID
     * @param size   推荐列表的大小
     * @param model  用于计算相似度的模型（例如 "emb" 或 "nerualcf"）
     * @return 推荐的电影列表
     */
    public static List<Movie> getRecList(int userId, int size, String model){
        User user = DataManager.getInstance().getUserById(userId);
        if (null == user){
            return new ArrayList<>();
        }
        final int CANDIDATE_SIZE = 800;
        List<Movie> candidates = DataManager.getInstance().getMovies(CANDIDATE_SIZE, "rating");

        // 如果数据源是 Redis，从 Redis 加载用户嵌入向量
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_REDIS)){
            String userEmbKey = "uEmb:" + userId;
            String userEmb = RedisClient.getInstance().get(userEmbKey);
            if (null != userEmb){
                user.setEmb(Utility.parseEmbStr(userEmb));
            }
        }
        // 如果从 Redis 加载用户特征
        if (Config.IS_LOAD_USER_FEATURE_FROM_REDIS){
            String userFeaturesKey = "uf:" + userId;
            Map<String, String> userFeatures = RedisClient.getInstance().hgetAll(userFeaturesKey);
            if (null != userFeatures){
                user.setUserFeatures(userFeatures);
            }
        }

        List<Movie> rankedList = ranker(user, candidates, model);

        if (rankedList.size() > size){
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    /**
     * 对候选集进行排序。
     * <p>
     * 这个方法根据传入的模型对候选电影列表进行排序。对于 "emb" 模型，它计算用户和每部候选电影之间的嵌入向量相似度；
     * 对于 "nerualcf" 模型，它调用外部 TensorFlow Serving 服务来获取预测分数。
     * 然后，它根据分数对候选电影进行排序，并返回排序后的列表。
     * </p>
     *
     * @param user    输入用户
     * @param candidates 候选电影列表
     * @param model   用于排序的模型名称
     * @return 排序后的电影列表
     */
    public static List<Movie> ranker(User user, List<Movie> candidates, String model){
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();

        switch (model){
            case "emb":
                for (Movie candidate : candidates){
                    double similarity = calculateEmbSimilarScore(user, candidate);
                    candidateScoreMap.put(candidate, similarity);
                }
                break;
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
     * 基于嵌入向量计算相似度得分。
     * <p>
     * 这个方法使用用户的嵌入向量和候选电影的嵌入向量来计算它们之间的余弦相似度。
     * 如果用户或候选电影的嵌入向量为空，它返回 -1 表示无效的相似度得分。
     * </p>
     *
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
     * 调用 TensorFlow Serving 获取 NeuralCF 模型的推理结果。
     * <p>
     * 这个方法构建了一个包含用户 ID 和候选电影 ID 的请求体，并将其发送到 TensorFlow Serving 服务。
     * 然后，它解析响应体中的预测分数，并将这些分数存储在传入的 candidateScoreMap 中。
     * </p>
     *
     * @param user 输入用户
     * @param candidates 候选电影列表
     * @param candidateScoreMap 保存预测分数的映射
     */
    public static void callNeuralCFTFServing(User user, List<Movie> candidates, HashMap<Movie, Double> candidateScoreMap){
        if (null == user || null == candidates || candidates.size() == 0){
            return;
        }

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
