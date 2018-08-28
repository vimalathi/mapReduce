package drivers;

import mappers.twitterSentimentAnalysisMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.twitterSentimentAnalysisReducer;

import java.net.URI;

public class twitterSentimentAnalysisDriver extends Configured implements Tool {
    public static void main(String args[])throws Exception{
        System.exit(ToolRunner.run(new twitterSentimentAnalysisDriver(), args));
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2){
            System.err.println("Usage: Parse <in><out>");
        }
        Configuration conf = new Configuration(getConf());
        DistributedCache.addCacheFile(new URI("/AFINN.txt"), conf);
        Job job = Job.getInstance(conf, "sentiment analysis on twitter data");
        job.setJarByClass(getClass());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(twitterSentimentAnalysisMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(twitterSentimentAnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true)? 0 : 1;
    }

}

