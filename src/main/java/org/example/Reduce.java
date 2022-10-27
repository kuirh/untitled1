package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public  class Reduce extends Reducer<Text, Text, Text, Text> {
    /*<v1 string "单词"
    k1 string "文件+词频">
    到
    <k2 string "单词"
    v2 string "文件+词频"；"文件+词频">
    */
    private Text result = new Text();
    //values存储相同key相同value
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 生成文档列表
        String fileList = new String();//v2
        for (Text value : values) {
            fileList += value.toString() + ";";
        }
        result.set(fileList);
        context.write(key, result);
    }
}
