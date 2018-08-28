package mappers;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;


public class twitterSentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Configuration conf;
    private URI[] files;
    //private URI[] url = ("/AFINN.txt");
    private HashMap<String, String> Affinn_map = new HashMap<String, String>();
//    public void configure(Job job){
//        //files = job.addCacheFile(job.getConfiguration());
//        job.setCacheFiles(new URI[]);
//    }
    public void setup(Mapper.Context context) throws IOException {
        conf = context.getConfiguration();
        //files = DistributedCache.addFileToClassPath(new Path("/"), context.getConfiguration());
        //files = DistributedCache.addCacheFile(new URI("/AFINN.txt"), context.getConfiguration());
        files = Job.getInstance(conf).getCacheFiles();
        System.out.println("files: " + files);
        Path path = new Path(files[0].getPath());
        FileSystem fs = FileSystem.get(files[0], conf);
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = br.readLine()) != null) {
            String[] splits = line.split("\t");
            Affinn_map.put(splits[0], splits[1]);
        }
        br.close();
        in.close();
    }

    public void map(LongWritable keyIn, Text valueIn, Context context){
        String line = valueIn.toString();
        String[] tuple = line.split("\\n");
        JSONParser jsonParser;
        try {
            for (int i = 0; tuple.length > i; i++) {
                JSONObject jsonObject = new JSONObject(tuple[i]);
                String tweet_id = (String) jsonObject.get("id_str");
                String tweet_text = (String) jsonObject.get("text");
                String[] splits = tuple.toString().split(" ");
                int sentimentSum = 0;
                for (String word : splits) {
                    if (Affinn_map.containsKey(word)) {
                        Integer x = new Integer(Affinn_map.get(word));
                        sentimentSum =+ x;
                    }
                }
                context.write(new Text(tweet_id), new Text(tweet_text + "\t ------> \t " + new Text(Integer.toString(sentimentSum))));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
