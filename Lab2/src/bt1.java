import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bt1 {

    public static class CleanMapper extends Mapper<Object, Text, NullWritable, Text> {

        private Set<String> stopWords = new HashSet<>();
        private Text cleanedLine = new Text();

        @Override
        protected void setup(Context context) throws IOException {

            Configuration conf = context.getConfiguration();
            Path pt = new Path("/input/stopwords.txt");
            FileSystem fs = FileSystem.get(conf);

            BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(pt))
            );

            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String[] tokens = line.split("\\s+");

            StringBuilder result = new StringBuilder();

            for (String token : tokens) {

                if (!token.isEmpty() && !stopWords.contains(token)) {
                    result.append(token).append(" ");
                }
            }

            String output = result.toString().trim();

            if (!output.isEmpty()) {
                cleanedLine.set(output);
                context.write(NullWritable.get(), cleanedLine);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: bt1 <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clean Text");

        job.setJarByClass(bt1.class);

        job.setMapperClass(CleanMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}