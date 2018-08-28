package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class syslogAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String keyOut;
    private String valueOut;
    public void map(LongWritable keyIn, Text valueIn, Context context) throws InterruptedException, IOException {
        String Line = valueIn.toString();
        String[] splits = Line.split(" ");
        if (Pattern.matches(splits[4], "sshd*")) {
            keyOut = splits[0] + " " + splits[1] + " " + splits[4];
            valueOut = valueIn.toString();
            context.write(new Text(keyOut), new Text(valueOut));
        }
        else return;
    }
}
