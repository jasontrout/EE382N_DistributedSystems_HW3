
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.StringBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Do not change the signature of this class
public class TextAnalyzer2 extends Configured implements Tool {

    public static class TextMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // To lower case.
            line = line.toLowerCase();

            // Replace characters that are not a-z, A-Z, or 0-9 with
            // a single whitespace.
            line = line.replaceAll("[^a-zA-Z0-9]", " ");

            // Create set of unique tokens.
            Set<String> uniqueTokens = new HashSet<>();
            StringTokenizer st = new StringTokenizer(line);
            while (st.hasMoreTokens()) {
                uniqueTokens.add(st.nextToken());
            }

            IntWritable one = new IntWritable(1);

            for (String t1 : uniqueTokens) {
                for (String t2 : uniqueTokens) {
                    if (!t2.equals(t1)) {
                        context.write(new Text(t1), new Text(t2 + ",1"));
                    }
                }
            }
        }
    }

    public static class TextCombiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> map = new HashMap<>();
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                String neighbor = tokens[0];
                int weight = Integer.parseInt(tokens[1]);
                if (!map.containsKey(neighbor)) {
                    map.put(neighbor, weight);
                } else {
                    map.put(neighbor, map.get(neighbor) + weight);
                }
            }
            
            for (String occurrenceKey : map.keySet()) {
                context.write(key, new Text(occurrenceKey + "," + map.get(occurrenceKey)));
            }
            
        }
    }

    public static class TextReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> map = new HashMap<>();
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                String neighbor = tokens[0];
                int weight = Integer.parseInt(tokens[1]);
                if (!map.containsKey(neighbor)) {
                    map.put(neighbor, weight);
                } else {
                    map.put(neighbor, map.get(neighbor) + weight);
                }
            }
            
            for (String occurrenceKey : map.keySet()) {
                context.write(new Text(key.toString() + " " + occurrenceKey), new Text(Integer.toString(map.get(occurrenceKey))));
            }
            context.write(new Text(""), new Text(""));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "JET3238_CW37657"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer2.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);

        // Set combiner class
        job.setCombinerClass(TextCombiner.class);

        // Set reducer class        
        job.setReducerClass(TextReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer2(), args);
        System.exit(res);
    }

}
