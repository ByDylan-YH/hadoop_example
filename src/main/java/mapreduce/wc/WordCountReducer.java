package mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key3, Iterable<IntWritable> values3, Context context) throws IOException, InterruptedException {
        //初始化
        int total = 0;
        for (IntWritable v : values3) {
            total = total + v.get();
        }
        //输出k4,v4
        context.write(key3, new IntWritable(total));
    }
}
