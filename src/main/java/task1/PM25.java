package task1;

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

public class PM25 {
    public static class PM25Mapper extends Mapper<LongWritable, Text, Text, Text>{
            protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException{
            if (key.toString().equals("0")){return;}
            String line = value.toString();
            String[] tokens = StringUtils.split(line, ',');
                 double PM25 = Double.parseDouble(tokens[3]);
                 String city = tokens[16];
                 context.write(new Text(city),new Text(String.valueOf(PM25)));
            }

    }
    public static class PM25Reducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for (Text val:values){
                sum += Double.parseDouble(String.valueOf(val));
                count++;
            }
            context.write(key,new Text(String.valueOf(sum / count)));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);

        Job job = Job.getInstance(conf, "task2.Level");
        job.setJarByClass(PM25.class);

        job.setMapperClass(PM25.PM25Mapper.class);
        job.setReducerClass(PM25.PM25Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out,"task1"));

        job.waitForCompletion(true);
    }
}
