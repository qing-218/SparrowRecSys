package com.sparrowrecsys.online.datamanager;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.sparrowrecsys.online.model.Embedding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User 类，包含从 movielens ratings.csv 加载的用户信息。
 * 用户类记录了用户的评分信息、平均评分、最高评分、最低评分等属性，并提供了添加评分和更新评分统计的功能。
 */
public class User {
    // 用户ID
    int userId;
    
    // 用户的平均评分
    double averageRating = 0;
    
    // 用户的最高评分
    double highestRating = 0;
    
    // 用户的最低评分
    double lowestRating = 5.0;
    
    // 用户的评分数量
    int ratingCount = 0;

    // 用户的评分列表（序列化时会使用 RatingListSerializer）
    @JsonSerialize(using = RatingListSerializer.class)
    List<Rating> ratings;

    // 用户的嵌入向量（Embeddings），用于机器学习等任务
    @JsonIgnore
    Embedding emb;

    // 用户的特征，存储一些其他用户信息（例如：偏好、行为等）
    @JsonIgnore
    Map<String, String> userFeatures;

    /**
     * 默认构造函数，初始化用户评分列表等基本属性。
     */
    public User(){
        this.ratings = new ArrayList<>();
        this.emb = null;
        this.userFeatures = null;
    }

    /**
     * 获取用户ID
     * @return 用户ID
     */
    public int getUserId() {
        return userId;
    }

    /**
     * 设置用户ID
     * @param userId 用户ID
     */
    public void setUserId(int userId) {
        this.userId = userId;
    }

    /**
     * 获取用户的评分列表
     * @return 用户评分列表
     */
    public List<Rating> getRatings() {
        return ratings;
    }

    /**
     * 设置用户的评分列表
     * @param ratings 用户评分列表
     */
    public void setRatings(List<Rating> ratings) {
        this.ratings = ratings;
    }

    /**
     * 添加一条评分记录，并更新相关的统计信息（平均评分、最高评分、最低评分）
     * @param rating 新评分对象
     */
    public void addRating(Rating rating) {
        // 将评分添加到列表中
        this.ratings.add(rating);
        
        // 更新平均评分
        this.averageRating = (this.averageRating * ratingCount + rating.getScore()) / (ratingCount + 1);
        
        // 更新最高评分
        if (rating.getScore() > highestRating){
            highestRating = rating.getScore();
        }

        // 更新最低评分
        if (rating.getScore() < lowestRating){
            lowestRating = rating.getScore();
        }

        // 增加评分数量
        ratingCount++;
    }

    /**
     * 获取用户的平均评分
     * @return 用户的平均评分
     */
    public double getAverageRating() {
        return averageRating;
    }

    /**
     * 设置用户的平均评分
     * @param averageRating 用户的平均评分
     */
    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    /**
     * 获取用户的最高评分
     * @return 用户的最高评分
     */
    public double getHighestRating() {
        return highestRating;
    }

    /**
     * 设置用户的最高评分
     * @param highestRating 用户的最高评分
     */
    public void setHighestRating(double highestRating) {
        this.highestRating = highestRating;
    }

    /**
     * 获取用户的最低评分
     * @return 用户的最低评分
     */
    public double getLowestRating() {
        return lowestRating;
    }

    /**
     * 设置用户的最低评分
     * @param lowestRating 用户的最低评分
     */
    public void setLowestRating(double lowestRating) {
        this.lowestRating = lowestRating;
    }

    /**
     * 获取用户的评分数量
     * @return 用户的评分数量
     */
    public int getRatingCount() {
        return ratingCount;
    }

    /**
     * 设置用户的评分数量
     * @param ratingCount 用户的评分数量
     */
    public void setRatingCount(int ratingCount) {
        this.ratingCount = ratingCount;
    }

    /**
     * 获取用户的嵌入向量（Embedding）
     * @return 用户的嵌入向量
     */
    public Embedding getEmb() {
        return emb;
    }

    /**
     * 设置用户的嵌入向量（Embedding）
     * @param emb 用户的嵌入向量
     */
    public void setEmb(Embedding emb) {
        this.emb = emb;
    }

    /**
     * 获取用户的特征（如偏好、行为等信息）
     * @return 用户特征的键值对
     */
    public Map<String, String> getUserFeatures() {
        return userFeatures;
    }

    /**
     * 设置用户的特征（如偏好、行为等信息）
     * @param userFeatures 用户特征的键值对
     */
    public void setUserFeatures(Map<String, String> userFeatures) {
        this.userFeatures = userFeatures;
    }
}
