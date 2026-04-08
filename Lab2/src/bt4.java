import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;

public class bt4 {

    // ════════════════════════════════════════════════════════════════
    // JOB 1 – Đếm (category#sentiment#word → count)
    // ════════════════════════════════════════════════════════════════

    public static class WordMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private final Text outputKey = new Text();
        private final Set<String> stopwords = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    if (new Path(uri).getName().equals("stopwords.txt")) {
                        try (BufferedReader br = new BufferedReader(
                                new InputStreamReader(
                                        new FileInputStream("stopwords.txt"), "UTF-8"))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                line = line.trim().toLowerCase();
                                if (!line.isEmpty()) stopwords.add(line);
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("id;")) return;

            String[] parts = line.split(";", 5);
            if (parts.length < 5) return;

            String comment   = parts[1].toLowerCase();
            String entity    = parts[3].trim();
            String sentiment = parts[4].trim().toLowerCase();

            if (!sentiment.equals("positive") && !sentiment.equals("negative")) return;

            String[] words = comment
                    .replaceAll("[^\\p{L}0-9 ]", " ")
                    .split("\\s+");

            for (String w : words) {
                w = w.trim();
                if (!w.isEmpty() && w.length() > 1 && !stopwords.contains(w)) {
                    // Key: category#sentiment#word
                    outputKey.set(entity + "#" + sentiment + "#" + w);
                    context.write(outputKey, one);
                }
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // JOB 2 – Lấy Top 5 từ theo từng (category + sentiment)
    // Input:  "HOTEL#positive#phòng  \t  120"
    // Output: "HOTEL#positive  \t  phòng=120, sạch=98, ..."
    // ════════════════════════════════════════════════════════════════

    public static class Top5Mapper
            extends Mapper<Object, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input: "HOTEL#positive#phòng\t120"
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] tab = line.split("\t");
            if (tab.length < 2) return;

            String[] keyParts = tab[0].split("#", 3);
            if (keyParts.length < 3) return;

            String category  = keyParts[0]; // HOTEL
            String sentiment = keyParts[1]; // positive
            String word      = keyParts[2]; // phòng
            String count     = tab[1];      // 120

            // Group key: category#sentiment
            outKey.set(category + "#" + sentiment);
            outVal.set(word + "=" + count);
            context.write(outKey, outVal);
        }
    }

    public static class Top5Reducer
            extends Reducer<Text, Text, Text, Text> {

        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Min-heap giữ Top 5 (loại phần tử nhỏ nhất khi size > 5)
            PriorityQueue<int[]> pq = new PriorityQueue<int[]>(11, new Comparator<int[]>() {
                @Override
                public int compare(int[] a, int[] b) {
                    return a[0] - b[0]; // min-heap
                }
            });
            Map<Integer, String> countToWord = new LinkedHashMap<Integer, String>();

            for (Text val : values) {
                String[] parts = val.toString().split("=", 2);
                if (parts.length < 2) continue;
                String word  = parts[0];
                int    count = Integer.parseInt(parts[1]);

                pq.offer(new int[]{count});
                countToWord.put(count, word);
                if (pq.size() > 5) pq.poll(); // loại min
            }

            // Sắp xếp giảm dần
            List<int[]> top5 = new ArrayList<int[]>(pq);
            Collections.sort(top5, new Comparator<int[]>() {
                @Override
                public int compare(int[] a, int[] b) {
                    return b[0] - a[0];
                }
            });

            StringBuilder sb = new StringBuilder();
            for (int[] entry : top5) {
                String word = countToWord.get(entry[0]);
                sb.append(word).append("=").append(entry[0]).append(", ");
            }

            outVal.set(sb.toString().replaceAll(", $", ""));
            context.write(key, outVal);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // DRIVER
    // ════════════════════════════════════════════════════════════════
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: bt4 <input> <tmp_output> <final_output>");
            System.exit(1);
        }

        // ── JOB 1: Đếm số lần xuất hiện ─────────────────────────────
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Word Count By Category Sentiment");
        job1.setJarByClass(bt4.class);
        job1.setMapperClass(WordMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        DistributedCache.addCacheFile(
            new URI("hdfs:///input/stopwords.txt#stopwords.txt"),
            conf1
        );

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ── JOB 2: Lấy Top 5 ─────────────────────────────────────────
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Top 5 Words By Category Sentiment");
        job2.setJarByClass(bt4.class);
        job2.setMapperClass(Top5Mapper.class);
        job2.setReducerClass(Top5Reducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}