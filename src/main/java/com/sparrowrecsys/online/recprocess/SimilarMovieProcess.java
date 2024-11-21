package com.sparrowrecsys.online.recprocess;

import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.Movie;
import java.util.*;

/**
 * 推荐相似电影的处理流程
 */
public class SimilarMovieProcess {

    /**
     * 获取推荐电影列表
     * <p>
     * 根据输入的电影ID、推荐列表的大小和用于计算相似度的模型，返回一个相似电影的列表。
     * </p>
     *
     * @param movieId 输入的电影 ID
     * @param size    相似项的大小
     * @param model   用于计算相似度的模型
     * @return 相似电影的列表
     */
    public static List<Movie> getRecList(int movieId, int size, String model){
        Movie movie = DataManager.getInstance().getMovieById(movieId);
        if (null == movie){
            return new ArrayList<>();
        }
        List<Movie> candidates = candidateGenerator(movie);
        List<Movie> rankedList = ranker(movie, candidates, model);

        if (rankedList.size() > size){
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    /**
     * 生成相似电影推荐的候选集
     * <p>
     * 根据输入的电影对象，生成相似电影推荐的候选集。对于电影的每个类型，获取该类型的电影作为候选，
     * 最后从候选集中移除输入的电影本身。
     * </p>
     *
     * @param movie 输入的电影对象
     * @return 电影候选集
     */
    public static List<Movie> candidateGenerator(Movie movie){
        HashMap<Integer, Movie> candidateMap = new HashMap<>();
        for (String genre : movie.getGenres()){
            List<Movie> oneCandidates = DataManager.getInstance().getMoviesByGenre(genre, 100, "rating");
            for (Movie candidate : oneCandidates){
                candidateMap.put(candidate.getMovieId(), candidate);
            }
        }
        candidateMap.remove(movie.getMovieId());
        return new ArrayList<>(candidateMap.values());
    }

    /**
     * 多检索候选生成方法
     * <p>
     * 根据输入的电影对象，从不同的数据源（类型、评分、最新）生成候选集。
     * </p>
     *
     * @param movie 输入的电影对象
     * @return 电影候选集
     */
    public static List<Movie> multipleRetrievalCandidates(Movie movie){
        if (null == movie){
            return null;
        }

        HashSet<String> genres = new HashSet<>(movie.getGenres());

        HashMap<Integer, Movie> candidateMap = new HashMap<>();
        for (String genre : genres){
            List<Movie> oneCandidates = DataManager.getInstance().getMoviesByGenre(genre, 20, "rating");
            for (Movie candidate : oneCandidates){
                candidateMap.put(candidate.getMovieId(), candidate);
            }
        }

        List<Movie> highRatingCandidates = DataManager.getInstance().getMovies(100, "rating");
        for (Movie candidate : highRatingCandidates){
            candidateMap.put(candidate.getMovieId(), candidate);
        }

        List<Movie> latestCandidates = DataManager.getInstance().getMovies(100, "releaseYear");
        for (Movie candidate : latestCandidates){
            candidateMap.put(candidate.getMovieId(), candidate);
        }

        candidateMap.remove(movie.getMovieId());
        return new ArrayList<>(candidateMap.values());
    }

    /**
     * 基于嵌入向量的候选生成方法
     * <p>
     * 根据输入的电影对象和候选池的大小，基于嵌入向量生成候选集。
     * 计算输入电影与所有电影的相似度，然后选择相似度最高的电影作为候选集。
     * </p>
     *
     * @param movie 输入的电影
     * @param size  候选池的大小
     * @return 电影候选集
     */
    public static List<Movie> retrievalCandidatesByEmbedding(Movie movie, int size){
        if (null == movie || null == movie.getEmb()){
            return null;
        }

        List<Movie> allCandidates = DataManager.getInstance().getMovies(10000, "rating");
        HashMap<Movie,Double> movieScoreMap = new HashMap<>();
        for (Movie candidate : allCandidates){
            double similarity = calculateEmbSimilarScore(movie, candidate);
            movieScoreMap.put(candidate, similarity);
        }

        List<Map.Entry<Movie,Double>> movieScoreList = new ArrayList<>(movieScoreMap.entrySet());
        movieScoreList.sort(Map.Entry.comparingByValue());

        List<Movie> candidates = new ArrayList<>();
        for (Map.Entry<Movie,Double> movieScoreEntry : movieScoreList){
            candidates.add(movieScoreEntry.getKey());
        }

        return candidates.subList(0, Math.min(candidates.size(), size));
    }

    /**
     * 对候选集进行排序
     * <p>
     * 根据输入的电影对象、候选集和用于排序的模型，对候选集进行排序。
     * 计算输入电影与每个候选电影的相似度，然后根据相似度对候选集进行排序。
     * </p>
     *
     * @param movie    输入的电影
     * @param candidates    电影候选集
     * @param model    用于排序的模型名称
     * @return 排序后的电影列表
     */
    public static List<Movie> ranker(Movie movie, List<Movie> candidates, String model){
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();
        for (Movie candidate : candidates){
            double similarity;
            switch (model){
                case "emb":
                    similarity = calculateEmbSimilarScore(movie, candidate);
                    break;
                default:
                    similarity = calculateSimilarScore(movie, candidate);
            }
            candidateScoreMap.put(candidate, similarity);
        }
        List<Movie> rankedList = new ArrayList<>();
        candidateScoreMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(m -> rankedList.add(m.getKey()));
        return rankedList;
    }

    /**
     * 计算相似度得分
     * <p>
     * 根据输入的电影对象和候选电影对象，计算它们之间的相似度得分。
     * 相似度得分基于它们共有的类型数量和候选电影的评分。
     * </p>
     *
     * @param movie     输入的电影
     * @param candidate 候选电影
     * @return 相似度得分
     */
    public static double calculateSimilarScore(Movie movie, Movie candidate){
        int sameGenreCount = 0;
        for (String genre : movie.getGenres()){
            if (candidate.getGenres().contains(genre)){
                sameGenreCount++;
            }
        }
        double genreSimilarity = (double)sameGenreCount / (movie.getGenres().size() + candidate.getGenres().size()) / 2;
        double ratingScore = candidate.getAverageRating() / 5;

        double similarityWeight = 0.7;
        double ratingScoreWeight = 0.3;

        return genreSimilarity * similarityWeight + ratingScore * ratingScoreWeight;
    }

    /**
     * 基于嵌入向量计算相似度得分
     * <p>
     * 根据输入的电影对象和候选电影对象，基于它们的嵌入向量计算相似度得分。
     * 如果输入的电影或候选电影为空，返回 -1 表示无效的相似度得分。
     * </p>
     *
     * @param movie     输入的电影
     * @param candidate 候选电影
     * @return 相似度得分
     */
    public static double calculateEmbSimilarScore(Movie movie, Movie candidate){
        if (null == movie || null == candidate){
            return -1;
        }
        return movie.getEmb().calculateSimilarity(candidate.getEmb());
    }
}
