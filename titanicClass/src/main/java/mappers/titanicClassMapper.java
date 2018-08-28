package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class titanicClassMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//    private Text keyOut = new Text();
//    private IntWritable valueOut = new IntWritable();
    public void map(LongWritable keyIn, Text valueIn, Context context)throws IOException, InterruptedException {
        if (keyIn.get() == 0)
            return;
        String line = valueIn.toString();
        String[] splits = line.split(",");
        String keyOut = splits[2] + splits[4] + splits[5];
        int val = 1;
        context.write(new Text(keyOut), new IntWritable(val));
    }
}
