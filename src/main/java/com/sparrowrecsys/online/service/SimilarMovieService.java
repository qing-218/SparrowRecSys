package com.sparrowrecsys.online.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparrowrecsys.online.datamanager.Movie;
import com.sparrowrecsys.online.recprocess.SimilarMovieProcess;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * SimilarMovieService 类，根据特定电影推荐相似电影
 */
public class SimilarMovieService extends HttpServlet {
    /**
     * 处理 GET 请求，返回与指定电影相似的电影列表
     * @param request HTTP 请求对象，包含客户端传递的参数
     * @param response HTTP 响应对象，用于返回推荐的相似电影
     * @throws IOException 当输入或输出操作发生异常时抛出
     */
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        try {
            // 设置响应内容类型为 JSON 格式
            response.setContentType("application/json");
            // 设置 HTTP 响应状态为 200 (OK)
            response.setStatus(HttpServletResponse.SC_OK);
            // 设置响应的字符编码为 UTF-8
            response.setCharacterEncoding("UTF-8");
            // 设置跨域访问头部，允许任意来源访问
            response.setHeader("Access-Control-Allow-Origin", "*");

            // 从请求中获取电影 ID 参数
            String movieId = request.getParameter("movieId");
            // 从请求中获取返回电影数量参数
            String size = request.getParameter("size");
            // 从请求中获取用于计算相似度的模型参数（例如 embedding 或 graph-embedding）
            String model = request.getParameter("model");

            // 使用 SimilarMovieProcess 类获取推荐的相似电影列表
            List<Movie> movies = SimilarMovieProcess.getRecList(Integer.parseInt(movieId), Integer.parseInt(size), model);

            // 将电影列表转换为 JSON 格式并返回
            ObjectMapper mapper = new ObjectMapper();
            String jsonMovies = mapper.writeValueAsString(movies);
            response.getWriter().println(jsonMovies);

        } catch (Exception e) {
            // 捕获异常并打印堆栈信息
            e.printStackTrace();
            // 返回空字符串作为响应
            response.getWriter().println("");
        }
    }
}
