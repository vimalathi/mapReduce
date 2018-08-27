package drivers;

import mappers.titanicDiedAvgAgeByGenderMapper;
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
import reducers.titanicDiedAvgAgeByGenderReducer;

public class titanicDiedAvgAgeByGender extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        System.exit(ToolRunner.run(new titanicDiedAvgAgeByGender(), args));
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration(getConf());
        Job job = Job.getInstance(conf, "Average age by gender who died in titanic");
        job.setJarByClass(getClass());

        Path in = new Path(strings[0]);
        Path out = new Path(strings[1]);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(titanicDiedAvgAgeByGenderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(titanicDiedAvgAgeByGenderReducer.class);

        job.setReducerClass(titanicDiedAvgAgeByGenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)? 0 : 1;
    }
}
