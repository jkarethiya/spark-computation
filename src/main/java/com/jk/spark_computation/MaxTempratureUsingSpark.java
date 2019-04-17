package com.jk.spark_computation;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MaxTempratureUsingSpark {

	public static void main(String[] args) throws Exception {
		String inputPath = "weather_without_header.csv";

		SparkConf conf = new SparkConf().setAppName("MaxTempratureUsingSpark").setMaster("local[4]");

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<String> weatherData = sc.textFile(inputPath);
			JavaPairRDD<String, Float> tempratureByCountry = weatherData
					.mapToPair(new PairFunction<String, String, Float>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Float> call(String line) throws Exception {
							String[] split = line.split(",");
							String country = split[0];
							Float temprature = Float.valueOf(split[3]);

							if (temprature == null)
								temprature = 0.0f;

							return new Tuple2<String, Float>(country, temprature);
						}

					});

			JavaPairRDD<String, Float> maxTempratureByCountry = tempratureByCountry
					.reduceByKey(new Function2<Float, Float, Float>() {

						@Override
						public Float call(Float v1, Float v2) throws Exception {
							return Math.max(v1, v2);
						}
					});

			maxTempratureByCountry.saveAsHadoopFile("C:/Users/Jitendra/test.csv", String.class, Float.class,
					TextOutputFormat.class);
		}
	}

}
