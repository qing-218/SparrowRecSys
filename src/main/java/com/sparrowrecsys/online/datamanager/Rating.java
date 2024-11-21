package com.sparrowrecsys.online.datamanager;

/**
 * 评分类，包含从 MovieLens ratings.csv 加载的属性
 */
public class Rating {
    // 电影ID，表示该评分对应的电影
    int movieId;
    // 用户ID，表示该评分是哪个用户给出的
    int userId;
    // 评分分数，表示用户给电影的评分
    float score;
    // 评分时间戳，表示用户给出评分的时间
    long timestamp;

    // 获取电影ID
    public int getMovieId() {
        return movieId;
    }

    // 设置电影ID
    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    // 获取用户ID
    public int getUserId() {
        return userId;
    }

    // 设置用户ID
    public void setUserId(int userId) {
        this.userId = userId;
    }

    // 获取评分分数
    public float getScore() {
        return score;
    }

    // 设置评分分数
    public void setScore(float score) {
        this.score = score;
    }

    // 获取评分时间戳
    public long getTimestamp() {
        return timestamp;
    }

    // 设置评分时间戳
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
