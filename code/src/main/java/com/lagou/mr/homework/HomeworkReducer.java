package com.lagou.mr.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HomeworkReducer extends Reducer<IntWritable, NullWritable,IntWritable,IntWritable> {
    //存放排名
    IntWritable rank = new IntWritable(1);
    int rn = 1;
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) { //避免有相同数字出现 还是要做一遍遍历
            //写入名次和具体数字
            context.write(rank,key);
            System.out.println(rank + "\t"+ key);
            rn += 1;
            rank.set(rn);
        }
    }
}
