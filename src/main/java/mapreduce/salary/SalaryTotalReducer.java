package mapreduce.salary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author:BY
 * Date:2019/3/13
 * Description:
 */
class SalaryTotalReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key3, Iterable<IntWritable> values3, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable v : values3) {
            total = total + v.get();
        }
        context.write(key3, new IntWritable(total));
    }
}
