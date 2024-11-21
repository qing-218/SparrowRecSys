package com.sparrowrecsys.online.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.Movie;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * RecommendationService 类，提供基于不同输入条件的推荐服务
 */

public class RecommendationService extends HttpServlet {
     /**
     * 处理 GET 请求，根据输入条件返回推荐电影列表
     * @param request HTTP 请求对象，包含客户端传递的参数
     * @param response HTTP 响应对象，用于返回推荐电影列表
     * @throws ServletException 当请求处理失败时抛出
     * @throws IOException 当输入或输出操作发生异常时抛出
     */
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException,
            IOException {
        try {
            // 设置响应内容类型为 JSON 格式
            response.setContentType("application/json");
            // 设置 HTTP 响应状态为 200 (OK)
            response.setStatus(HttpServletResponse.SC_OK);
            // 设置响应的字符编码为 UTF-8
            response.setCharacterEncoding("UTF-8");
            // 设置跨域访问头部，允许任意来源访问
            response.setHeader("Access-Control-Allow-Origin", "*");

            // 获取请求中的电影类别参数（genre）
            String genre = request.getParameter("genre");
            // 获取请求中返回的电影数量参数（size）
            String size = request.getParameter("size");
            // 获取请求中的排序算法参数（sortby）
            String sortby = request.getParameter("sortby");
            
            // 使用 DataManager 从指定类别中获取推荐的电影列表
            List<Movie> movies = DataManager.getInstance().getMoviesByGenre(genre, Integer.parseInt(size),sortby);

            // 将电影列表转换为 JSON 格式并返回
            ObjectMapper mapper = new ObjectMapper();// JSON 工具类
            String jsonMovies = mapper.writeValueAsString(movies);// 将电影列表转换为 JSON 字符串
            response.getWriter().println(jsonMovies);// 将 JSON 数据写入响应

        } catch (Exception e) {
             // 捕获异常并打印堆栈信息
            e.printStackTrace();
            // 如果发生异常，返回空字符串作为响应
            response.getWriter().println("");
        }
    }
}
