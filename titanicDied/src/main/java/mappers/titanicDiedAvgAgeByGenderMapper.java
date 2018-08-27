package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Integer.parseInt;

public class titanicDiedAvgAgeByGenderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text gender = new Text();
    private IntWritable age = new IntWritable();
    public void map(LongWritable keyIn, Text valueIn, Context context) throws InterruptedException, IOException {
        if (keyIn.get() == 0)
            return;
        String line = valueIn.toString();
        String[] splits = line.split(",");
        if(splits[1] == "1") {
            gender.set(splits[4]);
            age.set(parseInt(splits[5]));
        }
        context.write(gender, age);
    }
}
