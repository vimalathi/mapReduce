package reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class twitterSentimentAnalysisReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Text value, Context context) throws InterruptedException, IOException {
        context.write(key, value);
    }
}
