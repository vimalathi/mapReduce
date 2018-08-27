package reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class titanicDiedAvgAgeByGenderReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {
    public void reduce(Text keyIn, Iterable<IntWritable> valueIn, Context context) throws InterruptedException, IOException {
        int ageTotal = 0;
        int count = 0;
        for (IntWritable val : valueIn) {
            count += 1;
            ageTotal += val.get();
        }
        IntWritable ageAvg = new IntWritable(ageTotal / count);
        context.write(keyIn, ageAvg);
    }
}
