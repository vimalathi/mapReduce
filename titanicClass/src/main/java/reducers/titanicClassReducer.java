package reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class titanicClassReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {
    public void reduce(Text keyIn, Iterable<IntWritable> valueIn, Context context) throws InterruptedException, IOException {
        int sum = 0;
        for (IntWritable val: valueIn){
            sum =+ val.get();
        }
        context.write(keyIn, new IntWritable(sum));
    }
}
