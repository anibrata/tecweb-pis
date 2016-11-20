package br.edu.ufam.anibrata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfStrings;
import cern.colt.Arrays;

public class PairsPMI extends Configured implements Tool 
{
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  private static double N = 0;

  private static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
  {
	  private final static Text KEY = new Text();
	  private final static IntWritable ONE = new IntWritable(1);

	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	  {
		  String line = ((Text) value).toString();		// Enter input Text
		  StringTokenizer t = new StringTokenizer(line);

		  while (t.hasMoreTokens()) 
		  {
			  String token = t.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""); // Text cleanup
			  if (token.length() == 0) continue;
		      KEY.set(token);
		      context.write(KEY, ONE);
		  }
	  }
  }
  
  private static class WordCountMapperIMC extends Mapper<LongWritable, Text, Text, IntWritable> 
  {
	  private final HashMap<String, Integer> tempMap = new HashMap<String, Integer>();

	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	  {
		  String line = ((Text) value).toString();		// Enter input Text
		  StringTokenizer t = new StringTokenizer(line);
		  int tsum = 0;

		  while (t.hasMoreTokens()) 
		  {
			  String token = t.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""); // Text cleanup
			  if (token.length() == 0) continue;
			  
			  if (tempMap.containsKey(token))
			  {
				  tsum = tempMap.get(token).intValue() + 1;
				  tempMap.put(token, tsum);
			  }
			  else
				  tempMap.put(token, 1);
		  }
	  }
	  
	  @Override
	  public void cleanup(Context context) throws IOException, InterruptedException
	  {
		  IntWritable count = new IntWritable();
	      Text token = new Text();

	      for (Map.Entry<String, Integer> entry : tempMap.entrySet())
	      {
	    	  token.set(entry.getKey());
	    	  count.set(entry.getValue());
	    	  context.write(token, count);
	      }
	    }
  }

  // First stage Reducer: Totals counts for each Token
  private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
  {
    // Reuse objects
    private final static IntWritable TOTAL = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
    	int sum = 0;
    	for (IntWritable value : values) 
    	{
    		sum += value.get();
    	}
    	TOTAL.set(sum);
    	context.write(key, TOTAL);
    }
  }

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, DoubleWritable> 
  {
    private final static PairOfStrings COOCCUR = new PairOfStrings();
    private final static DoubleWritable ONE = new DoubleWritable(1.0);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {    	
    	String line = ((Text) value).toString();
        List<String> tokens = new ArrayList<String>();
        StringTokenizer itr = new StringTokenizer(line);
        
        while (itr.hasMoreTokens()) 
        {
        	String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        	if (w.length() == 0) continue;
        	tokens.add(w);
        }

        for (int i = 0; i < tokens.size(); i++) 
        {
        	for (int j = i+1; j < tokens.size(); j++)
        	{
        		if (i == j) /* Do not compare words with the same indices  */
        		{
        			continue;
        		}
        		else /* Compares words and removes pairs of repeated words  */
        		{
        			String temp1, temp2; // Variables to hold the values of pairs of strings from the list
        			temp1 = tokens.get(i);
        			temp2 = tokens.get(j);
        			if (!temp1.equals(temp2))
        			{
        				COOCCUR.set(temp1, temp2);
        				context.write(COOCCUR, ONE);
        			}
        		}
        	}
        }
    }
  }

  private static class MyCombiner extends Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable>
  {
	  private static DoubleWritable SUM = new DoubleWritable();

	  @Override
	  public void reduce(PairOfStrings pair, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
	  {
		  double sum = 0;
		  Iterator<DoubleWritable> iter = values.iterator();
		  while (iter.hasNext()) 
		  {
			  sum += iter.next().get();
		  }
		  SUM.set(sum);
		  context.write(pair, SUM);
	  }
  	}

  private static class MyReducer extends Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> 
  {
	 private static DoubleWritable PMI = new DoubleWritable();
	 private static PairOfStrings PAIR = new PairOfStrings();
	 private static Map<String, Integer> dictionary = new HashMap<String, Integer>();

	 @Override
	 public void setup(Context context) throws IOException 
	 {
		 Configuration configuration = context.getConfiguration(); /* Read the output of the first job and load it into a MAP */
		 FileSystem fileSystem = FileSystem.get(configuration);

		 String path = System.getProperty("user.dir");
		 Path pathToFile = new Path("pairs/part-r-00000");

		 BufferedReader bufferedReader = null;
		 FSDataInputStream fsdis = null;
		 InputStreamReader isr = null;
		 try 
		 {
			 fsdis = fileSystem.open(pathToFile);
			 isr = new InputStreamReader(fsdis);
			 bufferedReader = new BufferedReader(isr);
		 } 
		 catch (Exception e) 
		 {
			 e.printStackTrace();
		 }
		 
		 try 
		 {
			 String line = "";
			 while ((line = bufferedReader.readLine()) != null) 
			 {
				 String[] arr = line.split("\\s+");
				 if (arr.length == 2) 
				 {
					 dictionary.put(arr[0], Integer.parseInt(arr[1]));
					 N += Integer.parseInt(arr[1]);
				 } 
				 else 
				 {
					 LOG.info("Some error while creating dictionary.");
				 }
			 }
		 } 
		 catch (Exception e) 
		 {
			 LOG.info("Some error occured while reading the file");
		 }
		 bufferedReader.close();
		 LOG.info("Value of N: " + N);
	 }
	 
	
	 @Override
	 public void reduce(PairOfStrings key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
	 {
		 double number_of_occ = 0.0;
		 Iterator<DoubleWritable> iter = values.iterator();
		 while (iter.hasNext()) 
		 {
			 number_of_occ += iter.next().get();
		 }

		 if (number_of_occ >= 10.0) 
		 {
			 String word1 = key.getLeftElement();
			 String word2 = key.getRightElement();
			 double p_x_y = number_of_occ / N;
			 double p_x = (double) dictionary.get(word1) / N;
			 double p_y = (double) dictionary.get(word2) / N;
			 double pmi = Math.log10(p_x_y / (p_x * p_y));
			 PMI.set(pmi);
			 context.write(key, PMI);
		 }
	 }
  }
  
  public PairsPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "reducers";
  private static final String IMC = "imc";

  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception 
  {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of reducers").create(NUM_REDUCERS));
    options.addOption(OptionBuilder.withArgName("flag(y/n)").hasArg().withDescription("in mapper combining").create(IMC));
      
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try 
    {
      cmdline = parser.parse(options, args);
    } 
    catch (ParseException exp) 
    {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) 
    {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    /* JOB ONE CONFIGURATION STARTS */

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    String imcFlag = cmdline.getOptionValue(IMC);
    String path = System.getProperty("user.dir");
    String mapperOneOutputPath = "pairs";

    Path path2 = new Path(mapperOneOutputPath);

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(path2)) 
    {
    	fs.delete(new Path(mapperOneOutputPath), true);
    }

    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
    
    String flag = cmdline.hasOption(IMC) ? cmdline.getOptionValue(IMC) : "n";

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + mapperOneOutputPath);
    LOG.info(" - number of reducers: " + 1);
    LOG.info(" - in mapper combining: " + imcFlag);

    Configuration customConfiguration = getConf();
    customConfiguration.set("mapOneOutput", mapperOneOutputPath);

    Job firstJob = Job.getInstance(customConfiguration);
    firstJob.setJobName(PairsPMI.class.getSimpleName());
    firstJob.setJarByClass(PairsPMI.class);

    firstJob.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(firstJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(firstJob, new Path(mapperOneOutputPath));

    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);

    if (flag == "y")
    	firstJob.setMapperClass(WordCountMapperIMC.class);
    else
    	firstJob.setMapperClass(WordCountMapper.class);
    
    firstJob.setCombinerClass(WordCountReducer.class);
    firstJob.setReducerClass(WordCountReducer.class);

    Path firstMapperOutputDir = new Path(mapperOneOutputPath);
    FileSystem.get(customConfiguration).delete(firstMapperOutputDir, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // JOB ONE CONFIGURATION ENDS

    // JOB TWO CONFIGURATION STARTS 

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);
    LOG.info(" - in mapper combining: " + imcFlag);

    Job secondJob = new Job(customConfiguration, "Use Word Frequency and calculate PMI");
    secondJob.setJobName(PairsPMI.class.getSimpleName());
    secondJob.setJarByClass(PairsPMI.class);

    secondJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));

    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(DoubleWritable.class);

    secondJob.setMapperClass(MyMapper.class);
    secondJob.setCombinerClass(MyCombiner.class);
    secondJob.setReducerClass(MyReducer.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);

    Path outputDir = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception 
  {
	  ToolRunner.run(new PairsPMI(), args);
  }
}