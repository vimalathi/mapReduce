package drivers;

import mappers.titanicClassMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.titanicClassReducer;

import java.io.IOException;

public class titanicClassDriver extends Configured implements Tool {
    public static void main(String args[])throws Exception{
        System.exit(ToolRunner.run(new titanicClassDriver(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration(getConf());
        Job job = Job.getInstance(conf, "No of peoples in each class with gender and age");
        job.setJarByClass(getClass());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(titanicClassMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(titanicClassReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)? 0 : 1;
    }
}
