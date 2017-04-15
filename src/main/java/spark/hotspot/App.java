package spark.hotspot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));

		// Load our input data.
		JavaRDD<String> input = sc.textFile(args[0]);
		
		// Split up into lines.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String x) {
				ArrayList<String> output = new ArrayList<String>();
				StringTokenizer s = new StringTokenizer(x, ",");
				String time = s.nextToken();
				int index = time.indexOf("/", 2);
				time = time.substring(2, index);
				int date = Integer.parseInt(time);
				String longitude = s.nextToken();
				String latitude = s.nextToken();
				
				if (longitude.length() > 6 && latitude.length() > 6) {
					try {
						double lon = Double.parseDouble(longitude.substring(0, 6));
						double lat = Double.parseDouble(latitude.substring(0, 5));
						// log1.info("input : " + line);
						String[] strs = neighborhoods(date, lon, lat);
						for (int i = 0; i < 27; i++) {
							output.add(strs[i]);
						}
					} catch (java.lang.NumberFormatException e) {
					}
				}
				return output.iterator();
			}
		});
		
		// Transform into line and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
	            new PairFunction<String, String, Integer>(){
	                public Tuple2<String, Integer> call(String s){
	                        return new Tuple2(s, 1);
	                    }
	            } );

	        // count the pickup
	        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
	            new Function2<Integer, Integer, Integer>(){
	                public Integer call(Integer x, Integer y){ return x + y; }
	            } );
	        JavaPairRDD<Integer, String> swappedPair = reducedCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	                return item.swap();
	            }

	         });
	        List<Tuple2<Integer,String>> first50 = swappedPair.sortByKey(false).take(50);
	        JavaPairRDD<Integer, String> finalOut = sc.parallelizePairs(first50);
	    // Save the 50 hottest spot back out to a text file
	        finalOut.saveAsTextFile(args[1]);
	}
	


	
	private static String[] neighborhoods(int time, double lon, double lat) {
		String[] out = new String[27];
		for (int a = -1; a < 2; a++) {
			for (int b = -1; b < 2; b++) {
				for (int c = -1; c < 2; c++) {
					int t = time + a;
					double lo = lon + b * 0.01;
					double la = lat + c * 0.01;
					String str = t + "@" + String.format("%.2f", lo) + "@" + String.format("%.2f", la);
					out[(a + 1) * 9 + (b + 1) * 3 + c + 1] = str;
				}
			}
		}
		return out;
	}
}
