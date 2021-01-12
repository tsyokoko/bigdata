package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;


import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Animation {
    public static class AnimationMapper extends Mapper<LongWritable, Text, Text, AnimationBean> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            if (key.toString().equals("0")){return;}
            String line = value.toString();
            String[] tokens = StringUtils.split(line, ',');

            int AQI = Integer.parseInt(tokens[10]);
            int year = Integer.parseInt(tokens[12]);
            int month = Integer.parseInt(tokens[13]);
            int day = Integer.parseInt(tokens[14]);
            String city = tokens[16];

            Calendar c = Calendar.getInstance();
            c.set(year,month-1, day, 0, 0);
            Date date = c.getTime();
            if (AQI != 0){
                context.write(new Text(city),new AnimationBean(AQI,date));
            }
            }
        }
    public static class AnimationReducer extends Reducer<Text, AnimationBean, Text, Text> {
        protected void reduce(Text key, Iterable<AnimationBean> values , Context context)
                throws IOException, InterruptedException {
            Map<Date,Integer> dayAQI = new TreeMap<>(Comparator.naturalOrder());
            Map<Date,Integer> dayCount = new TreeMap<>(Comparator.naturalOrder());
            for (AnimationBean value:values){
                dayCount.merge(value.getDate(),1,Integer::sum);
                dayAQI.merge(value.getDate(), value.getAQI(), Integer::sum);
            }
            for (Map.Entry<Date,Integer> entry : dayCount.entrySet()){
                Date date = entry.getKey();
                int AQI = dayAQI.get(date) / dayCount.get(date);
                String ds = new SimpleDateFormat("yyyy-MM-dd").format(date);
                context.write(key,new Text(ds+'\t'+AQI));
            }
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);
        Job job = Job.getInstance(conf, "每日AQI");
        job.setJarByClass(Animation.class);

        job.setMapperClass(AnimationMapper.class);
        job.setReducerClass(AnimationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AnimationBean.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out, "task3"));

        job.waitForCompletion(true);

        final java.nio.file.Path path = Paths.get("output/task3");
        final java.nio.file.Path txt = path.resolve("part-r-00000");
        final java.nio.file.Path csv = path.resolve("animation.csv");
        try (
                final Stream<String> lines = Files.lines(txt);
                final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(csv, StandardOpenOption.CREATE_NEW)))
        {
            lines.map((line) -> line.split("\t")).
                    map((line) -> Stream.of(line).collect(Collectors.joining(","))).
                    forEach(pw::println);
        }


    }
}
