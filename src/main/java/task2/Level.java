package task2;

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
import java.util.HashMap;

public class Level {
    public static class LevelMapper extends Mapper<LongWritable, Text, Text, WeatherBean> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            if (key.toString().equals("0")){return;}
            String line = value.toString();
            String[] tokens = StringUtils.split(line, ',');
                   int id = Integer.parseInt(tokens[0]);
                   int AQI = Integer.parseInt(tokens[10]);
                   int year = Integer.parseInt(tokens[12]);
                   int month = Integer.parseInt(tokens[13]);
                   int day = Integer.parseInt(tokens[14]);
                   int hour = Integer.parseInt(tokens[15]);
                   String city = tokens[16];
                   if (year == 2019 && month == 2){
                       if (city.equals("北京")||city.equals("上海")||city.equals("成都")){
                           context.write(new Text(city),new WeatherBean(id,day,hour,AQI));
                       }
                   }
        }

    }
    public static class LevelReducer extends Reducer<Text, WeatherBean, Text, Text> {
        protected void reduce(Text key, Iterable<WeatherBean> values , Context context)
                throws IOException, InterruptedException {
            int level_1 = 0,level_2 = 0,level_3 = 0,level_4 = 0,level_5 = 0;
            HashMap<Integer,Integer>DayCounter = new HashMap<>();
            HashMap<Integer,Integer>DayAQI = new HashMap<>();
            for (WeatherBean value:values){
                DayCounter.merge(value.getDay(), 1, Integer::sum);
                DayAQI.merge(value.getDay(), value.getAQI(), Integer::sum);
            }
            for(int i = 1 ;i<=28;i++) {
                int AQI = (int) (DayAQI.get(i) * 1.0 / DayCounter.get(i));
                if (AQI <= 50) {
                    level_1++;
                } else if (51 <= AQI && AQI <= 100) {
                    level_2++;
                } else if (101 <= AQI && AQI <= 200) {
                    level_3++;
                } else if (201 <= AQI && AQI <= 300) {
                    level_4++;
                } else {
                    level_5++;
                }
            }
                context.write(key,new Text('\t'+String.valueOf(level_1)+'\t'+String.valueOf(level_2)
                        +'\t'+String.valueOf(level_3)+'\t'+String.valueOf(level_4)+'\t'+String.valueOf(level_5)));
            }
        }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);
        Job job = Job.getInstance(conf, "2019年2月份的北京、上海和成都的空气质量天数");
        job.setJarByClass(Level.class);

        job.setMapperClass(LevelMapper.class);
        job.setReducerClass(LevelReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WeatherBean.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out,"task2"));

        job.waitForCompletion(true);


    }
}
