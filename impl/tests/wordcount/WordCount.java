package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
  private static final Log LOG = LogFactory.getLog(WordCount.class);

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private int count = 0;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      System.out.println("calling map");
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }

    public void cleanup(Context context) throws IOException {
        System.out.println("finish map " + count++);
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private int count = 0;

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      System.out.println("calling reduce");
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }

    public void cleanup(Context context) throws IOException {
        System.out.println("finish reduce " + count++);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

/*
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
*/

    String input = "";
    String output = "";
    int numRun = 10;

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-in")) {
        input = args[++i];
      } else if (args[i].equals("-out")) {
        output = args[++i];
      } else if (args[i].equals("-numRun")) {
        numRun = Integer.parseInt(args[++i]);
      }
    }

    ArrayList<Job> jobs = new ArrayList<Job>();

    for (int i = 0; i < numRun; i++) {
        System.out.println("Running job...");
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output + Integer.toString(i)));

        job.submit();
        jobs.add(job);
    }

    int count = 0;
    while (count != numRun) {
        count = 0;
        for (Job job: jobs) {
            if (job.isComplete()) {
                count++;
            }
        }
    }

    System.exit(0);
  }
}
