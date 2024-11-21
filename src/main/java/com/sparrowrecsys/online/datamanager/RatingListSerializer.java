package com.sparrowrecsys.online.datamanager;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.List;

/**
 * 自定义的 Rating 列表序列化器，用于将 Rating 对象列表序列化成 JSON 格式。
 * 该类继承自 Jackson 的 JsonSerializer 类，专门用于自定义如何序列化 Rating 对象列表。
 */
public class RatingListSerializer extends JsonSerializer<List<Rating>> {

    /**
     * 将 Rating 对象列表序列化为 JSON 格式
     * 
     * @param ratingList 要序列化的 Rating 列表
     * @param jsonGenerator 用于生成 JSON 的 JsonGenerator 对象
     * @param provider 序列化上下文提供器
     * @throws IOException 如果序列化过程中发生 I/O 错误
     */
    @Override
    public void serialize(List<Rating> ratingList, JsonGenerator jsonGenerator,
                          SerializerProvider provider) throws IOException {
        // 开始写 JSON 数组
        jsonGenerator.writeStartArray();
        
        // 遍历 Rating 列表中的每个 Rating 对象
        for (Rating rating : ratingList) {
            // 对于每个 Rating 对象，开始写一个 JSON 对象
            jsonGenerator.writeStartObject();
            // 将 Rating 对象作为字段写入 JSON
            jsonGenerator.writeObjectField("rating", rating);
            // 结束当前的 JSON 对象
            jsonGenerator.writeEndObject();
        }
        
        // 结束 JSON 数组
        jsonGenerator.writeEndArray();
    }
}
