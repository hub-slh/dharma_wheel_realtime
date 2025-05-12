package com.slh.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.constant.Constant;
import com.slh.utils.FlinkSourceUtil;
import com.slh.utils.HBaseUtil1;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.slh.app.dwd.six_title
 * @Author song.lihao
 * @Date 2025/5/12 21:28
 * @description:
 */
public class six_title {
    public static void main(String[] args) throws Exception {
        // 初始化流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // 从 Kafka 中读取源数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dwd_user_profile");
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 数据清洗和过滤
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(value);
                    String table = jsonObj.getJSONObject("source").getString("table");
                    String op = jsonObj.getString("op");

                    // 只处理用户相关表的数据变化
                    if (("user_info".equals(table) || "user_behavior".equals(table))
                        && ("c".equals(op) || "u".equals(op) || "r".equals(op))) {
                        out.collect(jsonObj);
                    }
                } catch (Exception e) {
                    // 记录异常数据
                    System.out.println("数据解析异常 ：" + value);
                }
            }
        });

        // 用户年龄标签处理
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> ageTagDS = jsonDS.process(new AgeTagProcessFunction());
        // 用户性别标签处理
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> genderTagDS = jsonDS.process(new GenderTagProcessFunction());
        // 用户身高标签处理
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> heightTagDS = jsonDS.process(new HeightTagProcessFunction());
        // 用户体重标签处理
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> weightTagDS = jsonDS.process(new WeightTagProcessFunction());
        // 用户年代标签处理
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> yearTagDS = jsonDS.process(new YearTagProcessFunction());
        // 用户星座标签处理
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> zodiacTagDS = jsonDS.process(new ZodiacTagProcessFunction());
        // 合并所以标签流
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> mergedTagsDS = (SingleOutputStreamOperator<Tuple2<String, Map<String, String>>>) ageTagDS.union(genderTagDS, heightTagDS, weightTagDS, yearTagDS, zodiacTagDS);        // 写入 HBase
        mergedTagsDS.addSink((SinkFunction<Tuple2<String, Map<String, String>>>) new HBaseUserTagsSink());

        env.execute();
    }
}
class AgeTagProcessFunction extends ProcessFunction<JSONObject, Tuple2<String, Map<String, String>>> {
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
        String userId = jsonObj.getJSONObject("after").getString("user_id");
        JSONObject after = jsonObj.getJSONObject("after");

        // 模拟年龄计算逻辑
        String agrTag = clacylateAgeTag(after);

        Map<String, String> tags = new HashMap<>();
        tags.put("age_tag", agrTag);

        out.collect(Tuple2.of(userId,tags));
    }

    private String clacylateAgeTag(JSONObject userData) {
        // 优先使用直接年龄数据
        if (userData.containsKey("birth_year")) {
            int birthYear = userData.getIntValue("birth_year");
            int currentYear = 2025;
            int age = currentYear - birthYear;
            if (age >= 18 && age <= 24) return "18-24";
            else if (age <= 29) return "25-29";
            else if (age <= 34) return "30-34";
            else if (age <= 39) return "35-39";
            else if (age <= 44) return "40-44";
            else if (age >= 50) return "50+";
        }
        // 无直接年龄数据时，根据行为数据计算
        return "unknown";
    }
}

// 性别标签处理函数
class GenderTagProcessFunction extends ProcessFunction<JSONObject, Tuple2<String, Map<String, String>>> {
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
        String userId = jsonObj.getJSONObject("after").getString("user_id");
        JSONObject after = jsonObj.getJSONObject("after");
        String genderTag = calculateGenderTag(after);
        Map<String, String> tags = new HashMap<>();
        tags.put("gender_tag", genderTag);
        out.collect(Tuple2.of(userId, tags));
    }
    private String calculateGenderTag(JSONObject userData) {
        // 优先使用直接性别数据
        if (userData.containsKey("gender")) {
            String gender = userData.getString("gender");
            if ("male".equals(gender)) return "male";
            else if ("female".equals(gender)) return "female";
        }
        // 无直接性别数据时，根据行为数据计算
        return "unknown";
    }
}

// 身高标签处理函数
class HeightTagProcessFunction extends ProcessFunction<JSONObject, Tuple2<String, Map<String, String>>> {
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
        String userId = jsonObj.getJSONObject("after").getString("user_id");
        JSONObject after = jsonObj.getJSONObject("after");
        String heightTag = calculateHeightTag(after);
        Map<String, String> tags = new HashMap<>();
        tags.put("height_tag", heightTag);
        out.collect(Tuple2.of(userId, tags));
    }
    private String calculateHeightTag(JSONObject userData) {
        // 优先使用直接身高数据
        if (userData.containsKey("height")) {
            int height = userData.getIntValue("height");
            if (height >= 100 && height <= 250) {
                return String.valueOf(height);
            }
        }
        return "unknown";
    }
}

// 体重标签处理函数
class WeightTagProcessFunction extends ProcessFunction<JSONObject, Tuple2<String, Map<String, String>>> {

    @Override
    public void processElement(JSONObject jsonobj, Context ctx, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
        String userId = jsonobj.getJSONObject("after").getString("user_id");
        JSONObject after = jsonobj.getJSONObject("after");
        String weightTag = calculateWeightTag(after);
        Map<String, String> tags = new HashMap<>();
        tags.put("weight_tag", weightTag);
        out.collect(Tuple2.of(userId, tags));
    }
    private String calculateWeightTag(JSONObject userData) {
        // 优先使用直接体重数据
        if (userData.containsKey("weight")) {
            double weight = userData.getDouble("weight");
            if (weight >= 30 && weight <= 200) {
                return String.format("%.2f", weight);
            }
        }
        return "unknown";
    }
}

// 年代标签处理函数
class YearTagProcessFunction extends ProcessFunction<JSONObject, Tuple2<String, Map<String, String>>> {

    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
        String userId = jsonObj.getJSONObject("after").getString("user_id");
        JSONObject after = jsonObj.getJSONObject("after");
        String yearTag = calculateYearTag(after);
        Map<String, String> tags = new HashMap<>();
        tags.put("year_tag", yearTag);
        out.collect(Tuple2.of(userId, tags));
    }
    private String calculateYearTag(JSONObject userData) {
        // 优先使用直接年龄数据
        if (userData.containsKey("birth_year")) {
            int birthYear = userData.getIntValue("birth_year");
            if (birthYear >= 1991 && birthYear <= 2000) return "90后";
            else if (birthYear >= 2001 && birthYear <= 2010) return "00后";
            else if (birthYear >= 2011 && birthYear <= 2020) return "10后";
            else if (birthYear >= 2021) return "20后";
        }
        return "unknown";
    }
}

// 星座标签处理函数
class ZodiacTagProcessFunction extends ProcessFunction<JSONObject, Tuple2<String, Map<String, String>>> {
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
        String userId = jsonObj.getJSONObject("after").getString("user_id");
        JSONObject after = jsonObj.getJSONObject("after");

        String zodiacTag = calculateZodiacTag(after);

        Map<String, String> tags = new HashMap<>();
        tags.put("zodiac_tag", zodiacTag);

        out.collect(Tuple2.of(userId, tags));
    }

    private String calculateZodiacTag(JSONObject userData) {
        if (userData.containsKey("birth_month") && userData.containsKey("birth_day")) {
            int month = userData.getInteger("birth_month");
            int day = userData.getInteger("birth_day");

            if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) return "水瓶座";
            if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) return "双鱼座";
            if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) return "白羊座";
            if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) return "金牛座";
            if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) return "双子座";
            if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) return "巨蟹座";
            if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) return "狮子座";
            if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) return "处女座";
            if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) return "天秤座";
            if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) return "天蝎座";
            if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) return "射手座";
            if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        }

        return "unknown";
    }
}


// HBase Sink函数
class HBaseUserTagsSink extends RichMapFunction<Tuple2<String, Map<String, String>>, Void> {
    private Connection hbaseConn;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil1.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil1.closeHBaseConnection(hbaseConn);
    }
    @Override
    public Void map(Tuple2<String, Map<String, String>> value) throws Exception {
        String userId = value.f0;
        Map<String, String> tagsMap = value.f1;
        Put put = new Put(Bytes.toBytes(userId));
        for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
            String  tagName = entry.getKey();
            String tagValue = entry.getValue();
            put.addColumn(Bytes.toBytes("tags"), Bytes.toBytes(tagName), Bytes.toBytes(tagValue));
        }
        HBaseUtil1.putRow(hbaseConn, "dwd_user_profile_tags", put);
        return null;
    }
}