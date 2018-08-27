package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
//import java.util.Date;

public class uberMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private DateFormat format = new SimpleDateFormat("MM/dd/yyyy");//DateFormat.getDateInstance(DateFormat.SHORT);
    Date date = null;
    //Calendar date = Calendar.getInstance();
    String[] days = {"sun", "mon", "tue", "wed", "thu", "fri", "sat"};
    String basement = null;
    private int trips;
    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) //to eliminate header row
            return;
        String line = record.toString();
        String[] splits = line.split(",");
        basement = splits[0];
        try {
            date = format.parse(splits[1]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        trips = new Integer(splits[3]);
        String keys = basement+" "+days[date.getDay()];
        context.write(new Text(keys), new IntWritable(trips));
    }
}

