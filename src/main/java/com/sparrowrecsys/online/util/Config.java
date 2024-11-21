package com.sparrowrecsys.online.util;

/**
 * 配置类，用于定义系统运行时的配置参数
 */
public class Config {
    // 数据来源配置：可以选择从 Redis 或文件加载数据
    public static final String DATA_SOURCE_REDIS = "redis"; // 数据来源为 Redis
    public static final String DATA_SOURCE_FILE = "file"; // 数据来源为文件

     // 当前嵌入数据的加载来源，默认为文件
    public static String EMB_DATA_SOURCE = Config.DATA_SOURCE_FILE;

    // 是否从 Redis 加载用户特征数据，默认为 false
    public static boolean IS_LOAD_USER_FEATURE_FROM_REDIS = false;

    // 是否从 Redis 加载物品特征数据，默认为 false
    public static boolean IS_LOAD_ITEM_FEATURE_FROM_REDIS = false;

    // 是否启用 A/B 测试功能，默认为 false
    public static boolean IS_ENABLE_AB_TEST = false;
}
