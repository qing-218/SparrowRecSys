package com.sparrowrecsys.online.util;

import com.sparrowrecsys.online.model.Embedding;

public class Utility {
     /**
     * 将嵌入向量的字符串解析为 Embedding 对象
     * @param embStr 表示嵌入向量的字符串，每个维度用空格分隔
     * @return 解析后的 Embedding 对象
     */
    public static Embedding parseEmbStr(String embStr){
        // 将嵌入向量字符串按照空格分割为字符串数组
        String[] embStrings = embStr.split("\\s");
        // 创建一个新的 Embedding 对象
        Embedding emb = new Embedding();
        // 遍历每个字符串元素，将其转换为浮点数并添加到 Embedding 对象中
        for (String element : embStrings) {
            emb.addDim(Float.parseFloat(element));
        }
        // 返回解析后的 Embedding 对象
        return emb;
    }
}
