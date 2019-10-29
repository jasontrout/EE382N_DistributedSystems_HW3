import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TextAnalyzer extends Configured implements Tool {

    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = noPunc(value.toString());
            String[] allWords = line.split(" +");
            HashSet<String> unique = new HashSet<>();
            for(String s : allWords){
                if(s.length()!=0) {
                    unique.add(s.toLowerCase());
                }
            }
            ArrayList<String> uniq = new ArrayList<>(unique);
            for(int i = 0;i < uniq.size();i ++){
                for(int j = i+1;j < uniq.size();j ++){
                    String first = uniq.get(i);
                    String second = uniq.get(j);
                    Text outputKey = new Text(first);
                    Text outputVal = new Text(second);
                    context.write(outputKey, outputVal);
                    context.write(outputVal, outputKey);
                }
            }
        }

        public static String noPunc(String s) {
            Pattern pattern = Pattern.compile("[^0-9 a-z A-Z]");
            Matcher matcher = pattern.matcher(s);
            String result = matcher.replaceAll(" ");
            return result;
        }
    }

    public static class TextCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            HashMap<String, Integer> count = new HashMap<>();
            for(Text t : values){
                if(count.containsKey(t.toString())){
                    count.put(t.toString(),count.get(t.toString())+1);
                }else{
                    count.put(t.toString(), 1);
                }
            }
            for(String s : count.keySet()){
                context.write(key, new Text(s+" "+count.get(s)));
            }
        }
    }

    public static class TextReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<Text> queryTuples, Context context)
                throws IOException, InterruptedException
        {
            HashMap<String, Integer> count = new HashMap<>();
            for(Text t : queryTuples){
                String s = t.toString().split(" ")[0];
                String i = t.toString().split(" ")[1];
                if(count.containsKey(s)){
                    count.put(s,count.get(s)+Integer.valueOf(i));
                }else{
                    count.put(s, Integer.valueOf(i));
                }
            }
            Text value = new Text();
            for (String neighbor : count.keySet()) {
                String weight = count.get(neighbor).toString();
                value.set(" " + neighbor + " " + weight);
                context.write(key, value);
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // Create job
        Job job = new Job(conf, "hw3"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);
        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        // set local combiner class
        job.setCombinerClass(TextCombiner.class);
        // set reducer class
        job.setReducerClass(TextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }
}

