package com.tarento.category.mr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class TestMe {

	public static void main(String[] args) {
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("tigers are wild", Double.valueOf(0.016133867116918443));
		map.put("cats are pet", Double.valueOf(0.8034245941146673));
		
		Double bestSimScore = findBestSimilarityScore(map);
		
		System.out.println(bestSimScore);
		
	}
	
	public static Double findBestSimilarityScore(Map<String, Double> sampleCategorizedSentences) {

		List<Double> similarityScoreValues = new ArrayList<Double>(sampleCategorizedSentences.values());

		Double bestScore = similarityScoreValues.stream().max(Comparator.comparing(i -> i)).get();

		return bestScore;
	}

}
