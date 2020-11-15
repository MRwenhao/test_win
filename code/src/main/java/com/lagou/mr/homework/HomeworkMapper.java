package com.lagou.mr.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HomeworkMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    //存放key值
    IntWritable num = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取每一行数字
        String numString = value.toString();
        //string转int
        num.set(Integer.parseInt(numString));
        //key为读取的每行数字，value置null
        context.write(num,NullWritable.get());
    }
}

