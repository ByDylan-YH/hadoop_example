package mapreduce.distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Author:BY
 * Date:2019/3/13
 * Description:
 */
class DistinctMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建一个任务
        Job job = Job.getInstance(new Configuration());
        //指定任务的入口
        job.setJarByClass(DistinctMain.class);

        //2.指定map
        job.setMapperClass(DistinctMapper.class);
        job.setMapOutputKeyClass(Text.class);//k1
        job.setOutputValueClass(NullWritable.class);//k2

        //3.指定reduce
        job.setReducerClass(DistinctReducer.class);
        job.setOutputKeyClass(Text.class);//k4
        job.setOutputValueClass(NullWritable.class);//v4

        //4.指定任务的输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5.执行任务
        job.waitForCompletion(true);
    }
}
