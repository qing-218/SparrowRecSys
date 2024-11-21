package com.sparrowrecsys.online;

import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.service.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;

/***
 * 推荐系统服务器，在线推荐服务的终端
 */

public class RecSysServer {

    public static void main(String[] args) throws Exception {
        new RecSysServer().run();
    }

    // 推荐系统服务器端口号
    private static final int DEFAULT_PORT = 6010;

    public void run() throws Exception{

        int port = DEFAULT_PORT;
        try {
            port = Integer.parseInt(System.getenv("PORT"));
        } catch (NumberFormatException ignored) {}

        // 设置 IP 和端口号
        InetSocketAddress inetAddress = new InetSocketAddress("0.0.0.0", port);
        Server server = new Server(inetAddress);

        // 获取 index.html 的路径
        URL webRootLocation = this.getClass().getResource("/webroot/index.html");
        if (webRootLocation == null)
        {
            throw new IllegalStateException("Unable to determine webroot URL location");
        }

        // 将 index.html 设置为根页面
        URI webRootUri = URI.create(webRootLocation.toURI().toASCIIString().replaceFirst("/index.html$","/"));
        System.out.printf("Web Root URI: %s%n", webRootUri.getPath());

       // 加载所有数据到 DataManager 中
        DataManager.getInstance().loadData(webRootUri.getPath() + "sampledata/movies.csv",
                webRootUri.getPath() + "sampledata/links.csv",webRootUri.getPath() + "sampledata/ratings.csv",
                webRootUri.getPath() + "modeldata/item2vecEmb.csv",
                webRootUri.getPath() + "modeldata/userEmb.csv",
                "i2vEmb", "uEmb");

       // 创建服务器上下文
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");// 设置上下文路径为 "/"，即根路径
        context.setBaseResource(Resource.newResource(webRootUri));// 将资源的根目录设置为 webRootUri
        context.setWelcomeFiles(new String[] { "index.html" });// 配置默认欢迎文件为 "index.html"
        context.getMimeTypes().addMimeMapping("txt","text/plain;charset=utf-8");// 添加文件的 MIME 类型映射

        // 绑定不同服务到相应的 Servlet
        context.addServlet(DefaultServlet.class,"/");
        context.addServlet(new ServletHolder(new MovieService()), "/getmovie");
        context.addServlet(new ServletHolder(new UserService()), "/getuser");
        context.addServlet(new ServletHolder(new SimilarMovieService()), "/getsimilarmovie");
        context.addServlet(new ServletHolder(new RecommendationService()), "/getrecommendation");
        context.addServlet(new ServletHolder(new RecForYouService()), "/getrecforyou");

        // 设置 URL 处理器
        server.setHandler(context);
        System.out.println("RecSys Server has started.");

         // 启动服务器
        server.start();
        // 保持服务器运行
        server.join();
    }
}
