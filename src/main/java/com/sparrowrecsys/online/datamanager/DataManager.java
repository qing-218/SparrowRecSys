package com.sparrowrecsys.online.datamanager;

import com.sparrowrecsys.online.util.Config;
import com.sparrowrecsys.online.util.Utility;

import java.io.File;
import java.util.*;

/**
 * DataManager 是一个工具类，负责所有数据加载逻辑。
 * 它从文件系统加载电影、评分、链接等数据，以及模型数据（如嵌入向量）。
 */
public class DataManager {
    // 单例实例
    private static volatile DataManager instance;
    HashMap<Integer, Movie> movieMap; // 用于存储电影信息的映射，键是电影ID，值是电影对象
    HashMap<Integer, User> userMap; // 用于存储用户信息的映射，键是用户ID，值是用户对象
    // 电影类型反向索引，用于快速查询某一类型下的所有电影
    HashMap<String, List<Movie>> genreReverseIndexMap;

    // 私有构造函数，初始化数据结构
    private DataManager(){
        this.movieMap = new HashMap<>();
        this.userMap = new HashMap<>();
        this.genreReverseIndexMap = new HashMap<>();
        instance = this;
    }

    // 获取 DataManager 单例实例
    public static DataManager getInstance(){
        if (null == instance){
            synchronized (DataManager.class){ // 双重检查锁定，确保线程安全
                if (null == instance){
                    instance = new DataManager();
                }
            }
        }
        return instance;
    }

    // 从文件系统加载数据，包括电影数据、评分数据、链接数据以及模型数据（如嵌入向量）
    public void loadData(String movieDataPath, String linkDataPath, String ratingDataPath, String movieEmbPath, String userEmbPath, String movieRedisKey, String userRedisKey) throws Exception{
        loadMovieData(movieDataPath); // 加载电影数据
        loadLinkData(linkDataPath); // 加载链接数据（该方法未提供，但假设为加载电影与其他资源之间的关系数据）
        loadRatingData(ratingDataPath); // 加载评分数据（该方法未提供，但假设为加载用户对电影的评分信息）
        loadMovieEmb(movieEmbPath, movieRedisKey); // 加载电影嵌入向量数据
        if (Config.IS_LOAD_ITEM_FEATURE_FROM_REDIS){
            loadMovieFeatures("mf:"); // 加载电影的其他特征数据（如果配置要求从 Redis 加载）
        }

        loadUserEmb(userEmbPath, userRedisKey); // 加载用户嵌入向量数据
    }

    // 从 movies.csv 文件加载电影数据
    private void loadMovieData(String movieDataPath) throws Exception{
        System.out.println("Loading movie data from " + movieDataPath + " ...");
        boolean skipFirstLine = true; // 跳过文件中的表头
        try (Scanner scanner = new Scanner(new File(movieDataPath))) {
            while (scanner.hasNextLine()) {
                String movieRawData = scanner.nextLine(); // 读取每一行
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue; // 跳过文件的第一行（表头）
                }
                String[] movieData = movieRawData.split(","); // 按逗号分割每一行数据
                if (movieData.length == 3){ // 如果电影数据的长度为 3（符合格式）
                    Movie movie = new Movie();
                    movie.setMovieId(Integer.parseInt(movieData[0])); // 设置电影ID
                    int releaseYear = parseReleaseYear(movieData[1].trim()); // 解析电影发布年份
                    if (releaseYear == -1){
                        movie.setTitle(movieData[1].trim()); // 如果没有年份信息，直接设置电影标题
                    }else{
                        movie.setReleaseYear(releaseYear); // 设置电影年份
                        movie.setTitle(movieData[1].trim().substring(0, movieData[1].trim().length()-6).trim()); // 提取电影标题
                    }
                    String genres = movieData[2]; // 获取电影类型
                    if (!genres.trim().isEmpty()){
                        String[] genreArray = genres.split("\\|"); // 将类型按照分隔符“|”拆分为数组
                        for (String genre : genreArray){
                            movie.addGenre(genre); // 将电影类型添加到电影对象中
                            addMovie2GenreIndex(genre, movie); // 将电影添加到类型索引中
                        }
                    }
                    this.movieMap.put(movie.getMovieId(), movie); // 将电影对象放入电影映射表
                }
            }
        }
        System.out.println("Loading movie data completed. " + this.movieMap.size() + " movies in total.");
    }

    // 加载电影嵌入向量数据
    private void loadMovieEmb(String movieEmbPath, String embKey) throws Exception{
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_FILE)) { // 如果嵌入数据来自文件
            System.out.println("Loading movie embedding from " + movieEmbPath + " ...");
            int validEmbCount = 0;
            try (Scanner scanner = new Scanner(new File(movieEmbPath))) {
                while (scanner.hasNextLine()) {
                    String movieRawEmbData = scanner.nextLine(); // 读取每一行嵌入数据
                    String[] movieEmbData = movieRawEmbData.split(":"); // 按冒号分割电影ID和嵌入向量数据
                    if (movieEmbData.length == 2) {
                        Movie m = getMovieById(Integer.parseInt(movieEmbData[0])); // 获取对应ID的电影
                        if (null == m) {
                            continue; // 如果电影对象为空，跳过该条数据
                        }
                        m.setEmb(Utility.parseEmbStr(movieEmbData[1])); // 设置电影的嵌入向量
                        validEmbCount++; // 计数有效的嵌入向量
                    }
                }
            }
            System.out.println("Loading movie embedding completed. " + validEmbCount + " movie embeddings in total.");
        }else{
            System.out.println("Loading movie embedding from Redis ...");
            Set<String> movieEmbKeys = RedisClient.getInstance().keys(embKey + "*"); // 从 Redis 中获取所有电影嵌入的键
            int validEmbCount = 0;
            for (String movieEmbKey : movieEmbKeys){
                String movieId = movieEmbKey.split(":")[1]; // 从键中提取电影ID
                Movie m = getMovieById(Integer.parseInt(movieId)); // 获取对应ID的电影
                if (null == m) {
                    continue; // 如果电影对象为空，跳过该条数据
                }
                m.setEmb(Utility.parseEmbStr(RedisClient.getInstance().get(movieEmbKey))); // 从 Redis 获取嵌入向量并设置
                validEmbCount++; // 计数有效的嵌入向量
            }
            System.out.println("Loading movie embedding completed. " + validEmbCount + " movie embeddings in total.");
        }
    }

    // 加载电影特征数据
    private void loadMovieFeatures(String movieFeaturesPrefix) throws Exception{
        System.out.println("Loading movie features from Redis ...");
        // 获取所有电影特征的键
        Set<String> movieFeaturesKeys = RedisClient.getInstance().keys(movieFeaturesPrefix + "*");
        int validFeaturesCount = 0;
        // 遍历所有电影特征键
        for (String movieFeaturesKey : movieFeaturesKeys){
            // 提取电影ID
            String movieId = movieFeaturesKey.split(":")[1];
            Movie m = getMovieById(Integer.parseInt(movieId)); // 根据电影ID获取电影对象
            if (null == m) {
                continue; // 如果没有找到电影对象，则跳过
            }
            // 从 Redis 获取电影特征数据，并设置到电影对象中
            m.setMovieFeatures(RedisClient.getInstance().hgetAll(movieFeaturesKey));
            validFeaturesCount++; // 统计有效的电影特征数量
        }
        System.out.println("Loading movie features completed. " + validFeaturesCount + " movie features in total.");
    }

    // 加载用户嵌入向量数据
    private void loadUserEmb(String userEmbPath, String embKey) throws Exception{
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_FILE)) {
            System.out.println("Loading user embedding from " + userEmbPath + " ...");
            int validEmbCount = 0;
            try (Scanner scanner = new Scanner(new File(userEmbPath))) {
                while (scanner.hasNextLine()) {
                    String userRawEmbData = scanner.nextLine(); // 读取每一行嵌入数据
                    String[] userEmbData = userRawEmbData.split(":"); // 按冒号分割用户ID和嵌入向量数据
                    if (userEmbData.length == 2) {
                        User u = getUserById(Integer.parseInt(userEmbData[0])); // 根据用户ID获取用户对象
                        if (null == u) {
                            continue; // 如果没有找到用户对象，则跳过
                        }
                        // 设置用户的嵌入向量
                        u.setEmb(Utility.parseEmbStr(userEmbData[1]));
                        validEmbCount++; // 统计有效的用户嵌入向量数量
                    }
                }
            }
            System.out.println("Loading user embedding completed. " + validEmbCount + " user embeddings in total.");
        }
    }

    // 解析电影的发布年份
    private int parseReleaseYear(String rawTitle){
        if (null == rawTitle || rawTitle.trim().length() < 6){ // 如果标题为空或者长度小于6，返回-1
            return -1;
        }else{
            String yearString = rawTitle.trim().substring(rawTitle.length()-5, rawTitle.length()-1); // 提取年份字符串
            try{
                return Integer.parseInt(yearString); // 将年份字符串转换为整数
            }catch (NumberFormatException exception){
                return -1; // 如果转换失败，返回-1
            }
        }
    }

    // 从 links.csv 文件加载电影链接数据
    private void loadLinkData(String linkDataPath) throws Exception{
        System.out.println("Loading link data from " + linkDataPath + " ...");
        int count = 0;
        boolean skipFirstLine = true; // 跳过表头
        try (Scanner scanner = new Scanner(new File(linkDataPath))) {
            while (scanner.hasNextLine()) {
                String linkRawData = scanner.nextLine(); // 读取每一行链接数据
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue; // 跳过文件的第一行（表头）
                }
                String[] linkData = linkRawData.split(","); // 按逗号分割数据
                if (linkData.length == 3){ // 如果链接数据的长度为3（符合格式）
                    int movieId = Integer.parseInt(linkData[0]); // 获取电影ID
                    Movie movie = this.movieMap.get(movieId); // 根据电影ID获取电影对象
                    if (null != movie){
                        count++; // 有效的电影链接计数
                        movie.setImdbId(linkData[1].trim()); // 设置电影的 IMDb ID
                        movie.setTmdbId(linkData[2].trim()); // 设置电影的 TMDb ID
                    }
                }
            }
        }
        System.out.println("Loading link data completed. " + count + " links in total.");
    }

    // 从 ratings.csv 文件加载评分数据
    private void loadRatingData(String ratingDataPath) throws Exception{
        System.out.println("Loading rating data from " + ratingDataPath + " ...");
        boolean skipFirstLine = true; // 跳过表头
        int count = 0;
        try (Scanner scanner = new Scanner(new File(ratingDataPath))) {
            while (scanner.hasNextLine()) {
                String ratingRawData = scanner.nextLine(); // 读取每一行评分数据
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue; // 跳过文件的第一行（表头）
                }
                String[] linkData = ratingRawData.split(","); // 按逗号分割数据
                if (linkData.length == 4){ // 如果评分数据的长度为4（符合格式）
                    count ++;
                    Rating rating = new Rating(); // 创建新的评分对象
                    rating.setUserId(Integer.parseInt(linkData[0])); // 设置用户ID
                    rating.setMovieId(Integer.parseInt(linkData[1])); // 设置电影ID
                    rating.setScore(Float.parseFloat(linkData[2])); // 设置评分
                    rating.setTimestamp(Long.parseLong(linkData[3])); // 设置时间戳
                    Movie movie = this.movieMap.get(rating.getMovieId()); // 获取电影对象
                    if (null != movie){
                        movie.addRating(rating); // 将评分添加到电影对象中
                    }
                    // 如果用户ID不存在，创建新的用户对象并加入 userMap
                    if (!this.userMap.containsKey(rating.getUserId())){
                        User user = new User();
                        user.setUserId(rating.getUserId());
                        this.userMap.put(user.getUserId(), user);
                    }
                    this.userMap.get(rating.getUserId()).addRating(rating); // 将评分添加到用户对象中
                }
            }
        }

        System.out.println("Loading rating data completed. " + count + " ratings in total.");
    }

    // 将电影添加到电影类型的反向索引中
    private void addMovie2GenreIndex(String genre, Movie movie){
        if (!this.genreReverseIndexMap.containsKey(genre)){
            this.genreReverseIndexMap.put(genre, new ArrayList<>());
        }
        this.genreReverseIndexMap.get(genre).add(movie); // 将电影添加到对应类型的列表中
    }

    // 根据电影类型获取电影列表，并按指定的排序方式排序
    public List<Movie> getMoviesByGenre(String genre, int size, String sortBy){
        if (null != genre){
            List<Movie> movies = new ArrayList<>(this.genreReverseIndexMap.get(genre)); // 获取该类型下的电影列表
            switch (sortBy){
                case "rating": // 按评分排序
                    movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));
                    break;
                case "releaseYear": // 按发布年份排序
                    movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));
                    break;
                default:
            }

            // 如果电影列表大小超过指定的大小，截取前 `size` 部分
            if (movies.size() > size){
                return movies.subList(0, size);
            }
            return movies;
        }
        return null;
    }

    // 获取按排序方式排序的前 N 部电影
    public List<Movie> getMovies(int size, String sortBy){
            List<Movie> movies = new ArrayList<>(movieMap.values()); // 获取所有电影对象
            switch (sortBy){
                case "rating": // 按评分排序
                    movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));
                    break;
                case "releaseYear": // 按发布年份排序
                    movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));
                    break;
                default:
            }

            // 如果电影列表大小超过指定的大小，截取前 `size` 部分
            if (movies.size() > size){
                return movies.subList(0, size);
            }
            return movies;
    }

    // 根据电影ID获取电影对象
    public Movie getMovieById(int movieId){
        return this.movieMap.get(movieId); // 返回对应ID的电影对象
    }

    // 根据用户ID获取用户对象
    public User getUserById(int userId){
        return this.userMap.get(userId); // 返回对应ID的用户对象
    }

