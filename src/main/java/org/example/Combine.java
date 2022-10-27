package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public  class Combine extends Reducer<Text, Text, Text, Text> {
    /*<k1 string "单词：文件"
    v1 string 1
    k2 string "单词"
    v2 string "文件+词频">
    */
    private Text info = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 遍历集合，统计每个单词出现次数
        int sum = 0;
        for (Text value : values) {
            sum += Integer.parseInt(value.toString());
        }
        //截取：后内容  获得文件名
        int splitIndex = key.toString().indexOf(":");
        // value=文件+词频
        info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
        // 截取：之前内容，设置为key
        key.set(key.toString().substring(0, splitIndex));
        context.write(key, info);
    }
}

