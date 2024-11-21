package com.sparrowrecsys.online.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.User;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * UserService 类，提供用户相关的服务，返回特定用户的信息
 */

public class UserService extends HttpServlet {
     /**
     * 处理 GET 请求，返回指定用户的信息
     * @param request HTTP 请求对象，包含用户的请求信息
     * @param response HTTP 响应对象，用于返回用户信息
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

             // 从 URL 参数中获取用户 ID
            String userId = request.getParameter("id");

            // 从 DataManager 获取对应的用户对象
            User user = DataManager.getInstance().getUserById(Integer.parseInt(userId));

            // 将用户对象转换为 JSON 格式并返回
            if (null != user) {
                ObjectMapper mapper = new ObjectMapper();// JSON 工具类
                String jsonUser = mapper.writeValueAsString(user);// 将用户对象转换为 JSON 字符串
                response.getWriter().println(jsonUser); // 将 JSON 数据写入响应
            }else{
                // 如果用户对象为 null，返回空字符串
                response.getWriter().println("");
            }

        } catch (Exception e) {
            // 捕获异常并打印堆栈信息
            e.printStackTrace();
            // 返回空字符串作为响应
            response.getWriter().println("");
        }
    }
}
