package uberReducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class uberReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outValue = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws InterruptedException, IOException {
        int sum = 0;
        for (IntWritable val : value
                ) {
            sum += val.get();
        }
        outValue.set(sum);
        context.write(key, outValue);
    }
}
