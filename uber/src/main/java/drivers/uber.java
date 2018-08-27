package drivers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import mappers.uberMapper;
import reducers.uberReducer;

public class uber extends Configured implements Tool{
    public static void main(String args[]) throws Exception {
        System.exit(ToolRunner.run(new uber(), args));
//        int exitCode = ToolRunner.run(new uber(), args);
//        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception{
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "days on which each basement has more number of trips");

        job.setJarByClass(getClass());

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.setInputPaths(job, in);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(uberMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(uberReducer.class);

        job.setReducerClass(uberReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, out);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true)? 0 : 1;
    }
}
