package com.tarento.category.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.tarento.category.io.CategoryOutWritable;
import com.tarento.category.io.SimilarityScoreWritable;

public class SentenceCategorizer {

	/**
	 * Mapper Class for picking the best score within each category.
	 * 
	 * @author aravinth
	 * @since Jul 31, 2019
	 *
	 */
	public static class CategorizerMapper extends Mapper<LongWritable, Text, Text, CategoryOutWritable> {

		private static final Log LOGGER = LogFactory.getLog(CategorizerMapper.class);

		public static final Pattern FILE_PATTERN = Pattern.compile("(.*)/(input|categorized)/(.*)");

		public static final String DELIM = "\\^";

		private static String inputType = null;

		private Configuration conf;

		enum Counter {
			INPUT_SENTENCES, CATEGORIZED_SENTENCES, UNKNOWN, ERROR
		}

		private static Set<String> categorySet = new HashSet<String>();

		private BufferedReader br;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			String filePathString = (filePath == null) ? null : filePath.toString();

			Matcher m = FILE_PATTERN.matcher(filePathString);

			if (m.matches()) {
				inputType = m.group(2);
			}

			conf = context.getConfiguration();

			// Add the input Category set to the Dist Cache
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			for (URI patternsURI : patternsURIs) {
				Path patternsPath = new Path(patternsURI.getPath());
				String categorySetInputFile = patternsPath.getName().toString();
				addCategorySet(categorySetInputFile);
			}
		}

		/**
		 * Adds the category list from the Distributed Cache.
		 * 
		 * @param fileName
		 */
		private void addCategorySet(String fileName) {
			try {
				br = new BufferedReader(new FileReader(fileName));
				String sentenceCategory = null;
				while ((sentenceCategory = br.readLine()) != null) {
					categorySet.add(sentenceCategory);
				}

			} catch (IOException ex) {
				System.err.println(
						"Caught exception while parsing the cached file '" + StringUtils.stringifyException(ex));
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (value == null) {
				return;
			}

			String[] categorizedSentenceParts;
			CategoryOutWritable out = new CategoryOutWritable();

			switch (inputType) {

			case "input":
				context.getCounter(Counter.INPUT_SENTENCES).increment(1);

				for (String category : categorySet) {
					out.setActualInput(true);
					out.setSentence(value.toString());
					context.write(new Text(category), out);
				}

				break;

			case "categorized":
				context.getCounter(Counter.CATEGORIZED_SENTENCES).increment(1);

				categorizedSentenceParts = value.toString().split(DELIM);

				if (categorizedSentenceParts.length != 2) {
					context.getCounter(Counter.ERROR).increment(1);
				}

				// Write <Category, Word>
				out.setActualInput(false);
				out.setSentence(categorizedSentenceParts[1]);
				context.write(new Text(categorizedSentenceParts[0]), out);

				break;

			default:
				context.getCounter(Counter.UNKNOWN).increment(1);
			}

		}

	}

	/**
	 * Reducer Class for picking the best score within each category.
	 * 
	 * @author aravinth
	 * @since Jul 31, 2019
	 *
	 */
	public static class CategorizerReducer extends Reducer<Text, CategoryOutWritable, Text, SimilarityScoreWritable> {

		private static final Log LOGGER = LogFactory.getLog(CategorizerReducer.class);

		@Override
		public void reduce(Text key, Iterable<CategoryOutWritable> values, Context context)
				throws IOException, InterruptedException {

			List<String> actualSentences = new ArrayList<String>();
			SimilarityScoreWritable ssOut = new SimilarityScoreWritable();
			Map<String, Double> sampleCategorizedSentences = new HashMap<String, Double>();

			// Identify the actual input Sentence from the values.
			for (CategoryOutWritable val : values) {
				if (val.isActualInput()) {
					actualSentences.add(val.getSentence());
				} else {
					sampleCategorizedSentences.put(val.getSentence(), Double.valueOf(0));
				}
			}

			// Update the Similarity Score for each of the input sentences.
			for (String actualSentence : actualSentences) {

				for (String str : sampleCategorizedSentences.keySet()) {
					double simScore = getSimilarityScore(actualSentence, str);
					sampleCategorizedSentences.put(str, simScore);
				}

				LOGGER.info("Inside Reduce.. Updated values.. " + sampleCategorizedSentences);

				// Find the best Similarity Score from the categorized sentence
				Double bestSimScore = findBestSimilarityScore(sampleCategorizedSentences);

				// result.set(sum);

				ssOut.setCategory(key.toString());
				ssOut.setSimilarityScore(bestSimScore);

				context.write(new Text(actualSentence), ssOut);
			}

		}

		/**
		 * Assume this is the Similarity Algorithm that returns the score between 0 & 1.
		 * 
		 * @param String 1
		 * @param String 2
		 * @return Similarity Score
		 */
		public static double getSimilarityScore(String s1, String s2) {
			// Returns a double value between 0 & 1.
			return Math.random();
		}

		/**
		 * Uses Higher Order Function to find the best score within a category.
		 * 
		 * @param sampleCategorizedSentences
		 * @return bestScore
		 */
		public static Double findBestSimilarityScore(Map<String, Double> sampleCategorizedSentences) {

			List<Double> similarityScoreValues = new ArrayList<Double>(sampleCategorizedSentences.values());

			Double bestScore = similarityScoreValues.stream().max(Comparator.comparing(i -> i)).get();

			return bestScore;
		}
	}

	/**
	 * Main Driver Class for Sentence Categorizer. Uses Distributed Cache for the
	 * Category list.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

		String[] remainingArgs = optionParser.getRemainingArgs();
		if ((remainingArgs.length < 4)) {
			System.err.println("Usage: SentenceCategorizer <in1> <in2 <out> -category <categoryFile>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Sentence Cateogorizer");
		job.setJarByClass(SentenceCategorizer.class);
		job.setMapperClass(CategorizerMapper.class);
		job.setReducerClass(CategorizerReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CategoryOutWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SimilarityScoreWritable.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < remainingArgs.length; ++i) {
			if ("-category".equals(remainingArgs[i])) {
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
			} else {
				otherArgs.add(remainingArgs[i]);
			}
		}

		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileInputFormat.addInputPath(job, new Path(otherArgs.get(1)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(2)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}