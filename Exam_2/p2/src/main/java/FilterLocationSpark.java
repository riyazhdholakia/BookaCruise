import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;


public class FilterLocationSpark {
	public static class MapMaker {

	      public static Map<Integer, String> transformCSVToMap(String csv) {

	         Map<Integer, String> map = new HashMap<Integer, String>();
	         try {
	               boolean flag = true;
	               for (int i = 0; i < csv.length(); i++) {
	                     if (csv.charAt(i) == '\"') {
	                        flag = !flag;
	                     }
	                     else if ((csv.charAt(i) == ',') && (flag == false)) {
	                        StringBuilder sb = new StringBuilder(csv);
	                        sb.setCharAt(i, '%');
	                        csv = sb.toString();
	                     }
	                }
	              String[] tokens = csv.trim().split(",");
	              int key = 0;
	              String value = new String();
	              for (int j = 0; j < tokens.length; j++) {
	                 key = j;
	                 if (tokens[j] == null || tokens[j].isEmpty())
	                     tokens[j] = "0";

	                 value = tokens[j];
	                 map.put(key, value);
	              }
	           }  catch (StringIndexOutOfBoundsException e) {
	                                System.err.println("OutOfBoundsException");
	              }
	              return map;
	        }
	   }

	public static void main(String[] args) {
		//check correct usage
		if(args.length != 5) {
			System.err.println("usage: <jar-file> <Latitute> <Longitute> <Tweet-directory> <User-dir> <output-dir>");
			return;
		}
		//grab input and output directories
		final String latitute=args[0];
		final String longitute= args[1];
		String inputFile1 = args[2];
		String inputFile2 = args[3];
		String outputDir = args[4];

		/*if(	isParsable(latitute) && 	isCreatable(longitute)){
			System.err.println("The Longitute/Latitute format should be numbers.");
			return;
		}*/

		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("StackExchange users from GA");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//create RDD for input file
		JavaRDD<String> input1 = sc.textFile("hdfs://localhost:9000/" + inputFile1 + "/*");
		JavaRDD<String> input2 = sc.textFile("hdfs://localhost:9000/" + inputFile2 + "/*");

		//Function that returns the Boolean in the Tuple2, used to filter

		//
		Function<Tuple2<String, Boolean>, Boolean> LFilter = new
				Function<Tuple2<String, Boolean>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Boolean> keyvalue) {
				return keyvalue._2();	//._2() returns the second element, the langilati Boolean
			}
		};
		//
		JavaRDD<String> LocationTweets = input1.mapToPair(new PairFunction<String, String, Boolean>() {
			@Override
			public Tuple2<String, Boolean> call(String line) {
				Boolean matchLocation = false;

				Map<Integer, String> map = MapMaker.transformCSVToMap(line);

				String text = map.get(4)+"::"+map.get(5)+"::"+map.get(1)+"::"+map.get(0)
				+"::"+map.get(2)+"::"+map.get(3);	//grab all the feilds

				String latituteFile = map.get(4);
				String longituteFile=map.get(5);

				String text1=latitute.trim()+" "+longitute.trim();
				String text2=latituteFile.trim()+" "+longituteFile.trim();

				//grab Location
				if(map.get(0) == null || latituteFile == null || longituteFile==null)
					return new Tuple2<String, Boolean>("", false);	//zero value
				//check location is Georgia
				if(text1.equals(text2))
					matchLocation = true;
				//return Tuple2 with DisplayName and Boolean for whether the user is from Georgia
				return new Tuple2<String, Boolean>(text, matchLocation);
			}
		}).filter(LFilter).keys();
		//write RDD to hdfs output-dir

		if(LocationTweets.count()==0){
			System.out.println("**********\n");
			System.out.println("No user With That LongiTute and Latitue Found");
			System.out.println("   ");
			System.out.println("*********");
			sc.close();
			return;
		}

		JavaPairRDD<String,String> LocationMaps = LocationTweets.mapToPair(
		new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String line) {

				String parsed[] = line.split("::");

				String UserId = parsed[3].trim().replace("\"", "");	//grab DisplayName
				if(UserId == null || line == null)
					return new Tuple2<String, String>("", "");

				return new Tuple2<String, String>(UserId, line);
			}
		});



		JavaPairRDD<String,String> userInfo = input2.mapToPair(
		new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String line) {

				Map<Integer, String> map = MapMaker.transformCSVToMap(line);

				String text = "::"+map.get(3)+"::"+map.get(1)+"::"+map.get(2)
				+"::"+map.get(5)+"::"+map.get(6);	//grab the required feilds

				String userId = map.get(0).trim().replace("\"", "");

					//grab DisplayName
				if(userId == null || text == null)
					return new Tuple2<String, String>("", "");

				return new Tuple2<String, String>(userId, text);
			}
		});

		JavaPairRDD<String,Tuple2<String, String>> JoinedData=LocationMaps.join(userInfo);//

		JavaRDD<Tuple2<String,String>> ReducedData= JoinedData.values();

		JavaRDD<String> ReducedSorted = ReducedData.mapToPair(
		new PairFunction<Tuple2<String,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String,String> tup) {
				String line=tup._1();

				String parsed[] = line.split("::");

				String rt = parsed[5].trim().replace("\"", "");
				if(rt == null || line == null)
					return new Tuple2<String, String>("", "");
					line=line+tup._2();
				return new Tuple2<String, String>(rt, line);
			}
		}).sortByKey(false).values();

		ReducedSorted.saveAsTextFile("hdfs://localhost:9000" + outputDir);

		int counter=LocationMaps.countByValue().size();
		System.out.println("**********\n");
		System.out.println("Number of tweets: "+counter);
		System.out.println("   ");
		System.out.println("*********");
		//close sparkContext
		sc.close();
	}
}
