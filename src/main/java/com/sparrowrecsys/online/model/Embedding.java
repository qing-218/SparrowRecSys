package com.sparrowrecsys.online.model;

import java.util.ArrayList;

/**
 * Embedding Class, contains embedding vector and related calculation
 * 这个类表示一个嵌入向量（Embedding），它包含了嵌入向量本身以及与之相关的计算方法。
 */
public class Embedding {
    // embedding vector
    // 嵌入向量，使用浮点数的ArrayList来存储向量的各个维度。
    ArrayList<Float> embVector;

    /**
     * 默认构造函数
     * 创建一个空的嵌入向量。
     */
    public Embedding(){
        this.embVector = new ArrayList<>();
    }

    /**
     * 带参数的构造函数
     * 根据给定的嵌入向量创建Embedding对象。
     * @param embVector 嵌入向量
     */
    public Embedding(ArrayList<Float> embVector){
        this.embVector = embVector;
    }

    /**
     * 添加向量维度
     * 向嵌入向量中添加一个新的维度。
     * @param element 要添加的维度值
     */
    public void addDim(Float element){
        this.embVector.add(element);
    }

    /**
     * 获取嵌入向量
     * @return 嵌入向量
     */
    public ArrayList<Float> getEmbVector() {
        return embVector;
    }

    /**
     * 设置嵌入向量
     * 设置Embedding对象的嵌入向量。
     * @param embVector 要设置的嵌入向量
     */
    public void setEmbVector(ArrayList<Float> embVector) {
        this.embVector = embVector;
    }

    /**
     * 计算两个嵌入向量之间的余弦相似度
     * 根据两个嵌入向量的点积和模长计算余弦相似度。
     * @param otherEmb 另一个嵌入向量
     * @return 两个嵌入向量之间的余弦相似度
     */
    public double calculateSimilarity(Embedding otherEmb){
        // 检查输入是否有效
        if (null == embVector || null == otherEmb || null == otherEmb.getEmbVector()
                || embVector.size() != otherEmb.getEmbVector().size()){
            return -1;
        }
        double dotProduct = 0; // 点积
        double denominator1 = 0; // 第一个向量的模长的平方
        double denominator2 = 0; // 第二个向量的模长的平方
        for (int i = 0; i < embVector.size(); i++){
            // 计算点积和模长的平方
            dotProduct += embVector.get(i) * otherEmb.getEmbVector().get(i);
            denominator1 += embVector.get(i) * embVector.get(i);
            denominator2 += otherEmb.getEmbVector().get(i) * otherEmb.getEmbVector().get(i);
        }
        // 计算余弦相似度
        return dotProduct / (Math.sqrt(denominator1) * Math.sqrt(denominator2));
    }
}
