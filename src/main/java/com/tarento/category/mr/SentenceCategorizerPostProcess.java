package com.tarento.category.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.tarento.category.io.SimilarityScoreWritable;

/**
 * This post process MR is used to read the best similarity scores within each
 * category and identifies the best category for the given sentence.
 * 
 * @author aravinth
 * @since Jul 31, 2019
 *
 */
public class SentenceCategorizerPostProcess {

	public static class BestCategoryMapper extends Mapper<LongWritable, Text, Text, SimilarityScoreWritable> {

		private static final Log LOGGER = LogFactory.getLog(BestCategoryMapper.class);

		public static final String TAB_DELIM = "\t";

		enum Counter {
			INPUT_SENTENCES, CATEGORIZED_SENTENCES, UNKNOWN, ERROR
		}

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (value == null) {
				return;
			}

			SimilarityScoreWritable ssOut = new SimilarityScoreWritable();
			String[] categorizedSentenceParts = value.toString().split(TAB_DELIM);
			ssOut.setCategory(categorizedSentenceParts[1]);
			ssOut.setSimilarityScore(Double.valueOf(categorizedSentenceParts[2]));

			// Sentence would be the key here
			context.write(new Text(categorizedSentenceParts[0]), ssOut);
		}

	}

	/**
	 * Reducer class which reads the sentence with the best scores within each
	 * category and spits out the single best category for each sentence.
	 * 
	 * @author aravinth
	 *
	 */
	public static class BestCategoryReducer extends Reducer<Text, SimilarityScoreWritable, Text, Text> {

		private static final Log LOGGER = LogFactory.getLog(BestCategoryReducer.class);

		@Override
		public void reduce(Text key, Iterable<SimilarityScoreWritable> values, Context context)
				throws IOException, InterruptedException {

			Map<Double, String> categoryScoreMaps = new HashMap<Double, String>();

			for (SimilarityScoreWritable val : values) {
				categoryScoreMaps.put(val.getSimilarityScore(), val.getCategory());
			}

			String bestCategory = findBestSimilarityScore(categoryScoreMaps);

			context.write(key, new Text(bestCategory));
		}

	}

	/**
	 * Uses Higher Order Function to find the best category.
	 * 
	 * @param categoryScoreMaps
	 * @return bestCategory
	 */
	public static String findBestSimilarityScore(Map<Double, String> categoryScoreMaps) {

		Set<Double> similarityScoreValues = categoryScoreMaps.keySet();

		Double bestScore = similarityScoreValues.stream().max(Comparator.comparing(i -> i)).get();

		return categoryScoreMaps.get(bestScore);
	}

	/**
	 * Main Driver Class for the post processing.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

		String[] remainingArgs = optionParser.getRemainingArgs();
		if ((remainingArgs.length < 2)) {
			System.err.println("Usage: SentenceCategorizerPostProcess <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Sentence Cateogorizer Post Process");
		job.setJarByClass(SentenceCategorizerPostProcess.class);
		job.setMapperClass(BestCategoryMapper.class);
		job.setReducerClass(BestCategoryReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SimilarityScoreWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < remainingArgs.length; ++i) {
			if ("-category".equals(remainingArgs[i])) {
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
			} else {
				otherArgs.add(remainingArgs[i]);
			}
		}

		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
