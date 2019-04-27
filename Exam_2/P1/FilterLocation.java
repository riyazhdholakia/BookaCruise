import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.NullWritable;

public class FilterLocation {

	public static int total_count = 0;
	public static int count=0;
	public static Integer j=1;

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


	public static class UsersFilter extends Mapper<Object, Text, Text, Text> {

		private Text outhandle = new Text();
		private Text outTuple = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String longitute = conf.get("longitute");
			String latitute =conf.get("latitute");

			Map<Integer, String> parsed = MapMaker.transformCSVToMap(value.toString());


			if(parsed == null){
				return;
			}

			String text1=latitute.trim()+" "+longitute.trim();

			count++;

			String userId= parsed.get(0).replace("\"", "");

			String latituteFile = parsed.get(4);
			String longituteFile = parsed.get(5);
			String tweets=parsed.get(1);
			String favs=parsed.get(2);
			String rt=parsed.get(3);

			String text2=latituteFile.trim()+" "+longituteFile.trim();


			if(text1.equals(text2))
				System.out.println("found");

			if(userId == null || latituteFile == null || longituteFile == null || !text1.equals(text2) ) {
				return;
			}

			if(tweets == null)
				tweets="";
			if(favs == null)
				favs="0";
			if(rt == null)
				rt="0";



			String text="\""+latituteFile+"\","+longituteFile+","+tweets+",\""+userId+"\",\""+favs+"\",\""+rt+"\"";
			total_count++;
			outhandle.set(userId);
			outTuple.set("A" + text);


			context.write(outhandle, outTuple);

		}

	}


	//minning the datas from the second input file.


	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outTuple = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String s = value.toString();
			String parsed[] = s.split("\",");

			String skip[] = new String[1];
			skip[0] = "Handle,Name,Bio,Country,Location,Followers,Following";
			if(parsed[0].equals(skip[0]))
				return;

			String userId = parsed[0].replace("\"", "");


			String text = ",\""+parsed[3].replace("\"", "")+"\",\""+parsed[1].replace("\"", "")+"\",\""+parsed[2].replace("\"", "")+
					"\",\""+parsed[5].replace("\"", "")+"\"";


			if (userId == null || text == null){
				return;
			}


			outkey.set(userId);
			outTuple.set("B" + text);

			context.write(outkey, outTuple);

		}
	}
		//reducer joins both the input file to get the desired informations
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

			private static final Text EMPTY_TEXT = new Text("");
			private Text tmp = new Text();
			private ArrayList<Text> listA = new ArrayList<Text>();
			private ArrayList<Text> listB = new ArrayList<Text>();


			@Override
			public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				listA.clear();
				listB.clear();

				for(Text tmp : values) {
					if(tmp.charAt(0) == 'A'){
						listA.add(new Text(tmp.toString().substring(1)));
					} else if(tmp.charAt(0) == 'B') {
						listB.add(new Text(tmp.toString().substring(1)));
					}
				}

				if(!listA.isEmpty() && !listB.isEmpty()) {
					for (Text A : listA) {
						for(Text B : listB) {
							context.write(A, B);
						}
					}
				}
			}
	}

	//maps the intermediate output file and lists the output by RT
	public static class topRTMapper extends Mapper<Object, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> favToRecordMap = new TreeMap<Integer, Text>();
		private Integer i=0;


		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Map<Integer, String> parsed = MapMaker.transformCSVToMap(value.toString());


			if(parsed == null){
				return;
			}


			String rt = parsed.get(5);
			rt=rt.trim().replace("\"","");

			if(rt == null) {
				return;
			}

			if(Integer.parseInt(rt)<=0){
				i=Integer.parseInt(rt)-j;
				rt=""+i;
				j++;

			}

			favToRecordMap.put(Integer.parseInt(rt), new Text(value));


		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Text t : favToRecordMap.values()){
				context.write(NullWritable.get(), t);
			}
		}


	}

//reduces by RT... decending order
	public static class topRtReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> favToRecordMap = new TreeMap<Integer, Text>();
		private Integer i=0;

		@Override
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value : values) {
				Map<Integer, String> parsed = MapMaker.transformCSVToMap(value.toString());

				String rt = parsed.get(5);
				rt=rt.trim().replace("\"","");


				if(rt == null || rt.isEmpty()) {
					continue;

				}

				if(Integer.parseInt(rt)<=0){
					i=Integer.parseInt(rt)-j;
					rt=""+i;
					j++;

				}


				favToRecordMap.put(Integer.parseInt(rt), new Text(value));


			}

			for(Text t : favToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

//driver class

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


		if(otherArgs.length != 5) {
			System.err.println("Usage: filter location <latitute> <longitute> <input> <input> <out>");
			System.exit(2);
		}

		conf.set("longitute", otherArgs[1]);
		conf.set("latitute", otherArgs[0]);

		Path output_Inter = new Path(otherArgs[4] + "_int");

		Job job2 = new Job(conf, "Merge the files With common longi and lati");
		job2.setJarByClass(FilterLocation.class);


		MultipleInputs.addInputPath(job2,new Path(otherArgs[2]), TextInputFormat.class, UsersFilter.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[3]), TextInputFormat.class, UserJoinMapper.class);
		job2.setReducerClass(JoinReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job2, output_Inter);

		int code=job2.waitForCompletion(true) ? 0 : 2;

		Job job = new Job(conf, "Filter info by location");
		job.setJarByClass(FilterLocation.class);
		job.setMapperClass(topRTMapper.class);
		job.setReducerClass(topRtReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, output_Inter);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

		code=job.waitForCompletion(true) ? 0 : 1;


		System.out.println("Total Number of matching tweets as follows: "+total_count);

		System.exit(code);
	}

}
