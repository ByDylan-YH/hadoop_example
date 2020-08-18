package mapreduce.salary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author:BY
 * Date:2019/3/13
 * Description:
 */
class SalaryTotalMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        String data = key1.toString();
        String[] words = data.split(",");
        //输出k2:部分号,v2:员工工资
        //注意,字符串要转换成整数
        context.write(new IntWritable(Integer.parseInt(words[7])), new IntWritable(Integer.parseInt(words[5])));
    }
}
