package com.sparrowrecsys.online.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.Movie;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * MovieService，返回特定电影的信息
 */
public class MovieService extends HttpServlet {
    /**
     * 处理GET请求
     * <p>
     * 这个方法处理HTTP GET请求，用于返回特定电影的详细信息。
     * 它首先设置响应的内容类型为JSON，然后从请求的URL参数中获取电影ID。
     * 使用DataManager获取电影对象，并将该对象转换为JSON格式返回给客户端。
     * 如果电影对象不存在，则返回一个空字符串。
     * </p>
     *
     * @param request  HTTP请求对象
     * @param response HTTP响应对象
     * @throws IOException 如果发生I/O错误
     */
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        try {
            // 设置响应的内容类型为JSON
            response.setContentType("application/json");
            // 设置HTTP响应状态码为200（OK）
            response.setStatus(HttpServletResponse.SC_OK);
            // 设置响应的字符编码为UTF-8
            response.setCharacterEncoding("UTF-8");
            // 设置响应头允许跨域请求
            response.setHeader("Access-Control-Allow-Origin", "*");

            // 通过URL参数获取电影ID
            String movieId = request.getParameter("id");

            // 从DataManager获取电影对象
            Movie movie = DataManager.getInstance().getMovieById(Integer.parseInt(movieId));

            // 将电影对象转换为JSON格式并返回
            if (null != movie) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonMovie = mapper.writeValueAsString(movie);
                response.getWriter().println(jsonMovie);
            } else {
                response.getWriter().println("");
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
