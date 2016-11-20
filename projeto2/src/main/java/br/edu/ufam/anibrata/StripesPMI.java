package br.edu.ufam.anibrata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Map;
import java.util.*;

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

import tl.lin.data.map.HMapStIW;
import tl.lin.data.pair.PairOfStrings;

public class StripesPMI extends Configured implements Tool 
{
	private static final Logger LOG = Logger.getLogger(StripesPMI.class);
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


  private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapStIW> 
  {
    private final static HMapStIW hashMap = new HMapStIW();
    private final static Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
    	String line = ((Text) value).toString();
    	StringTokenizer tokenizer = new StringTokenizer(line);
    	Set<String> sortedTokens = new TreeSet<String>();
    	while (tokenizer.hasMoreTokens()) 
    	{
    		sortedTokens.add(tokenizer.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""));
    	}
    	
    	String[] lineWords = new String[sortedTokens.size()];
    	sortedTokens.toArray(lineWords);

    	int length = lineWords.length;
    	for (int i = 0; i < length; i++) 
    	{
    		String keyTerm = lineWords[i];
    		if (keyTerm.length() == 0) 
    		{
    			continue;
    		}
    		
    		hashMap.clear();
    		for (int j = (i + 1); j < length; j++) 
    		{
    			if (lineWords[j].length() == 0) 
    			{
    				continue;
    			}
    			if (!hashMap.containsKey(lineWords[j])) 
    			{
    				hashMap.put(lineWords[j], 1);
    			}
    		}
    		KEY.set(keyTerm);
    		context.write(KEY, hashMap);
    	}
    }
  }

  private static class MyCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> 
  {
	  @Override
	  public void reduce(Text key, Iterable<HMapStIW> values, Context context) throws IOException, InterruptedException 
	  {
		  Iterator<HMapStIW> iter = values.iterator();
		  HMapStIW map = new HMapStIW();
		  while (iter.hasNext()) 
		  {
			  map.plus(iter.next());
		  }
		  context.write(key, map);
	  }
  }

  private static class MyReducer extends Reducer<Text, HMapStIW, PairOfStrings, DoubleWritable> 
  {
	  private static final PairOfStrings PAIR_OF_WORDS = new PairOfStrings();
	  private static final DoubleWritable PMI = new DoubleWritable();
	  private static HashMap<String, Integer> dictionary = new HashMap<String, Integer>();

	  @Override
	  public void setup(Context context) throws IOException
	  {
		  Configuration configuration = context.getConfiguration(); /* Read the output of the first job and load it into a MAP */
		  FileSystem fileSystem = FileSystem.get(configuration);

		  String path = System.getProperty("user.dir");
		  Path pathToFile = new Path("stripes/part-r-00000");

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
	  public void reduce(Text key, Iterable<HMapStIW> values, Context context) throws IOException, InterruptedException 
	  {
		  Iterator<HMapStIW> iter = values.iterator();
		  HMapStIW map = new HMapStIW();
		  while (iter.hasNext()) 
		  {
			  map.plus(iter.next());
		  }
		  
		  String leftWordOfPair = key.toString();
      
		  for (String rightWordOfPair : map.keySet()) 
		  {
			  double p_x_y = map.get(rightWordOfPair);
			  if (p_x_y >= 10) 
			  {
				  PAIR_OF_WORDS.set(leftWordOfPair, rightWordOfPair);
				  double numerator = p_x_y / N;
				  double prob_occ_x = (double) dictionary.get(leftWordOfPair) / N;
				  double prob_occ_y = (double) dictionary.get(rightWordOfPair) / N;
				  double pmi = Math.log10((numerator) / (prob_occ_x * prob_occ_y));
				  PMI.set(pmi);
				  context.write(PAIR_OF_WORDS, PMI);
			  }
		  }
	  }
  }

  public StripesPMI() {}
  
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "reducers";
  private static final String IMC = "imc";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
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
	  String mapperOneOutputPath = "stripes";

	  Path path2 = new Path(mapperOneOutputPath);

	  FileSystem fs = FileSystem.get(getConf());
	  if (fs.exists(path2)) 
	  {
		  fs.delete(new Path(mapperOneOutputPath), true);
	  }

	  int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
	  
	  String flag = cmdline.hasOption(IMC) ? cmdline.getOptionValue(IMC) : "n";

	  LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
	  LOG.info(" - input path: " + inputPath);
	  LOG.info(" - output path: " + mapperOneOutputPath);
	  LOG.info(" - num reducers: " + 1);
	  LOG.info(" - in mapper combining: " + imcFlag);

    Configuration customConfiguration = getConf();
    customConfiguration.set("mapOneOutput", mapperOneOutputPath);

    Job firstJob = Job.getInstance(customConfiguration);
    firstJob.setJobName(StripesPMI.class.getSimpleName());
    firstJob.setJarByClass(StripesPMI.class);

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

    /* JOB ONE CONFIGURATION ENDS */

    /* JOB TWO CONFIGURATION STARTS */

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);
    LOG.info(" - in mapper combining: " + imcFlag);

    Job secondJob = new Job(customConfiguration, "Use Word Frequency and calculate PMI");
    secondJob.setJobName(StripesPMI.class.getSimpleName());
    secondJob.setJarByClass(StripesPMI.class);

    secondJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));

    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(HMapStIW.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(DoubleWritable.class);

    secondJob.setMapperClass(MyMapper.class);
    secondJob.setCombinerClass(MyCombiner.class);
    secondJob.setReducerClass(MyReducer.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);


    Path outputDir = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputDir, true);

    long startTime2 = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception 
  {
	  ToolRunner.run(new StripesPMI(), args);
  }

}