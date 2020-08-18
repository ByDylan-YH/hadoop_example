package mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        //通过value1获取字符串,I am YH
        String data = value1.toString();
        //切分字符串
        String[] words = data.split(" ");
        //输出这些字符
        for (String w : words) {
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
