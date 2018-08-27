package uberMappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class uberMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    String basement = null;
    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
    Date date = null;
    String[] days = {"sun", "mon", "tue", "wed", "thu", "fri", "sat"};
    private int activeVehicles;
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        if (key.get() == 0)
            return;
        String line = value.toString();
        String[] splits = line.split(",");
        basement = splits[0];
        try{
            date = format.parse(splits[1]);
        }
        catch (ParseException e){
            e.printStackTrace();
        }
        String keys = basement + " " + days[date.getDay()];
        activeVehicles = new Integer(splits[2]);
        context.write(new Text(keys), new IntWritable(activeVehicles));
    }
}

