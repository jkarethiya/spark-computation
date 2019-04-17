package com.jk.spark_computation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MaxTemprature {

	public static void main(String[] args) throws Exception {
		Map<String, Float> maxTemp = new HashMap<>();
		String inputPath = "weather.csv";

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath)))) {

			String line = null;

			reader.readLine(); // Skipping header
			
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(",");
				String country = split[0];
				Float temprature = Float.valueOf(split[3]);

				if (temprature != null) {
					Float prevTemprature = maxTemp.get(country);

					if (prevTemprature == null) {
						maxTemp.put(country, temprature);
					} else if (prevTemprature != null && temprature > prevTemprature) {
						maxTemp.put(country, temprature);
					}
				}
			}
			
			System.out.println(maxTemp);
		}
	}

}
