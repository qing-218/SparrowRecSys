package com.sparrowrecsys.online.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparrowrecsys.online.recprocess.RecForYouProcess;
import com.sparrowrecsys.online.util.ABTest;
import com.sparrowrecsys.online.datamanager.Movie;
import com.sparrowrecsys.online.util.Config;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * RecForYouService 类，提供个性化推荐服务
 */

public class RecForYouService extends HttpServlet {
     /**
     * 处理 GET 请求，为用户提供个性化推荐的电影列表
     * @param request HTTP 请求对象，包含客户端传递的参数
     * @param response HTTP 响应对象，用于返回推荐的电影列表
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

            // 获取请求中的用户 ID 参数
            String userId = request.getParameter("id");
            // 获取请求中返回的电影数量参数
            String size = request.getParameter("size");
             // 获取请求中的推荐模型参数
            String model = request.getParameter("model");

            // 如果启用 A/B 测试，根据用户 ID 获取相应的推荐模型
            if (Config.IS_ENABLE_AB_TEST){
                model = ABTest.getConfigByUserId(userId);
            }

             // 使用 RecForYouProcess 获取个性化推荐的电影列表
            List<Movie> movies = RecForYouProcess.getRecList(Integer.parseInt(userId), Integer.parseInt(size), model);

            // 将推荐的电影列表转换为 JSON 格式并返回
            ObjectMapper mapper = new ObjectMapper();
            String jsonMovies = mapper.writeValueAsString(movies);
            response.getWriter().println(jsonMovies);

        } catch (Exception e) {
            // 捕获异常并打印堆栈信息
            e.printStackTrace();
            // 如果发生异常，返回空字符串作为响应
            response.getWriter().println("");
        }
    }
}
