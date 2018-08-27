package reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class uberReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable results = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val: values){
            sum += val.get();
        }
        results.set(sum);
        //context.write(new Text(key), new IntWritable(sum));
        context.write(key, results);
    }
}
