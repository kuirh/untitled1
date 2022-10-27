package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public  class Map extends Mapper<LongWritable, Text, Text, Text> {
    //<k1行偏移量,v1行文本数据>----><"单词+文件名，1">
    private Text keyInfo = new Text(); // K2
    private FileSplit split; // 每个文件的存储Split对象
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 一行文本拆分分割v1
        String line=value.toString();
        String[] fields=line.split(" ");
        // StringTokenizer itr = new StringTokenizer(line);
        for (String word:fields) {
            // key值由单词和URL组成，如"MapReduce：file1.txt"。
            //得到文件所在split和文件名
            split = (FileSplit) context.getInputSplit();
            int splitIndex = split.getPath().toString().indexOf("file");
            keyInfo.set(word + ":" + split.getPath().toString().substring(splitIndex));
            // 词频初始化为1
            context.write(keyInfo, new Text("1"));//k2,v2
        }
    }
}

