package com.sparrowrecsys.online.util;

/**
 * A/B 测试工具类，用于根据用户 ID 分配模型版本
 */
public class ABTest {
    // A/B 测试流量划分的基数，表示将用户流量划分为 5 组
    final static int trafficSplitNumber = 5;

     // A 流量组对应的模型配置名称
    final static String bucketAModel = "emb";

    // B 流量组对应的模型配置名称
    final static String bucketBModel = "nerualcf";

    // 默认模型配置名称（用户未参与 A/B 测试时适用）
    final static String defaultModel = "emb";

    /**
     * 根据用户 ID 获取对应的模型配置
     * @param userId 用户 ID
     * @return 分配给用户的模型名称
     */
    public static String getConfigByUserId(String userId){
        // 如果用户 ID 为空或无效，则返回默认模型
        if (null == userId || userId.isEmpty()){
            return defaultModel;
        }

         // 使用用户 ID 的哈希值对流量组进行分配
        if(userId.hashCode() % trafficSplitNumber == 0){
             // 如果哈希值模 trafficSplitNumber 的结果为 0，分配到 A 组
            System.out.println(userId + " is in bucketA.");
            return bucketAModel;
        }else if(userId.hashCode() % trafficSplitNumber == 1){
            // 如果哈希值模 trafficSplitNumber 的结果为 1，分配到 B 组
            System.out.println(userId + " is in bucketB.");
            return bucketBModel;
        }else{
            // 其他情况，用户未参与 A/B 测试，使用默认模型
            System.out.println(userId + " isn't in AB test.");
            return defaultModel;
        }
    }
}
