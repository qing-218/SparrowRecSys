package com.sparrowrecsys.online.datamanager;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.sparrowrecsys.online.model.Embedding;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 电影类，包含从 MovieLens movies.csv 中加载的属性以及其他高级数据如平均评分、嵌入向量等。
 */
public class Movie {
    // 电影ID
    int movieId;
    // 电影标题
    String title;
    // 电影发布年份
    int releaseYear;
    // IMDb ID
    String imdbId;
    // TMDb ID
    String tmdbId;
    // 电影类型列表
    List<String> genres;
    // 评分人数
    int ratingNumber;
    // 平均评分
    double averageRating;

    // 电影的嵌入向量，标记为@JsonIgnore，表示该字段不会被序列化
    @JsonIgnore
    Embedding emb;

    // 所有评分数据列表，标记为@JsonIgnore，表示该字段不会被序列化
    @JsonIgnore
    List<Rating> ratings;

    // 电影特征数据，标记为@JsonIgnore，表示该字段不会被序列化
    @JsonIgnore
    Map<String, String> movieFeatures;

    // 常量：记录最高评分数量
    final int TOP_RATING_SIZE = 10;

    // 电影的前N个评分（按评分高低排序）
    @JsonSerialize(using = RatingListSerializer.class)
    List<Rating> topRatings;

    // 构造方法，初始化电影对象的各项数据
    public Movie() {
        ratingNumber = 0; // 初始评分人数为0
        averageRating = 0; // 初始平均评分为0
        this.genres = new ArrayList<>(); // 初始化类型列表
        this.ratings = new ArrayList<>(); // 初始化评分列表
        this.topRatings = new LinkedList<>(); // 初始化前N个评分列表
        this.emb = null; // 嵌入向量初始化为null
        this.movieFeatures = null; // 电影特征初始化为null
    }

    // 获取电影ID
    public int getMovieId() {
        return movieId;
    }

    // 设置电影ID
    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    // 获取电影标题
    public String getTitle() {
        return title;
    }

    // 设置电影标题
    public void setTitle(String title) {
        this.title = title;
    }

    // 获取电影发布年份
    public int getReleaseYear() {
        return releaseYear;
    }

    // 设置电影发布年份
    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }

    // 获取电影类型列表
    public List<String> getGenres() {
        return genres;
    }

    // 添加电影类型
    public void addGenre(String genre){
        this.genres.add(genre);
    }

    // 设置电影类型列表
    public void setGenres(List<String> genres) {
        this.genres = genres;
    }

    // 获取所有评分数据
    public List<Rating> getRatings() {
        return ratings;
    }

    // 添加评分数据，并更新电影的平均评分
    public void addRating(Rating rating) {
        // 更新平均评分：新的评分 = (原评分总和 + 新评分) / 新的评分人数
        averageRating = (averageRating * ratingNumber + rating.getScore()) / (ratingNumber+1);
        ratingNumber++; // 评分人数加1
        this.ratings.add(rating); // 将新的评分数据添加到评分列表中
        addTopRating(rating); // 添加该评分到前N个评分列表中
    }

    // 将评分添加到前N个评分列表（按评分从高到低排序）
    public void addTopRating(Rating rating){
        // 如果当前前N个评分列表为空，则直接添加
        if (this.topRatings.isEmpty()){
            this.topRatings.add(rating);
        }else{
            int index = 0;
            // 找到该评分应插入的位置
            for (Rating topRating : this.topRatings){
                if (topRating.getScore() >= rating.getScore()){
                    break;
                }
                index++;
            }
            topRatings.add(index, rating); // 插入评分
            // 如果列表大小超过了TOP_RATING_SIZE，则移除最小的评分
            if (topRatings.size() > TOP_RATING_SIZE) {
                topRatings.remove(0);
            }
        }
    }

    // 获取IMDb ID
    public String getImdbId() {
        return imdbId;
    }

    // 设置IMDb ID
    public void setImdbId(String imdbId) {
        this.imdbId = imdbId;
    }

    // 获取TMDb ID
    public String getTmdbId() {
        return tmdbId;
    }

    // 设置TMDb ID
    public void setTmdbId(String tmdbId) {
        this.tmdbId = tmdbId;
    }

    // 获取评分人数
    public int getRatingNumber() {
        return ratingNumber;
    }

    // 获取平均评分
    public double getAverageRating() {
        return averageRating;
    }

    // 获取电影的嵌入向量
    public Embedding getEmb() {
        return emb;
    }

    // 设置电影的嵌入向量
    public void setEmb(Embedding emb) {
        this.emb = emb;
    }

    // 获取电影特征数据
    public Map<String, String> getMovieFeatures() {
        return movieFeatures;
    }

    // 设置电影特征数据
    public void setMovieFeatures(Map<String, String> movieFeatures) {
        this.movieFeatures = movieFeatures;
    }
}
