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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import task2.Level;


import java.io.IOException;
import java.util.HashMap;

public class WorstLevel {
    public static class WorstLevelMapper extends Mapper<LongWritable, Text, Text, LevelBean> {
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
            if (year == 2019){
                if (city.equals("北京")||city.equals("上海")||city.equals("成都")){
                    context.write(new Text(city),new LevelBean(id,month,day,hour,AQI));
                }
            }
        }

    }
    public static class WorstLevelReducer extends Reducer<Text, LevelBean, Text, Text> {
        protected void reduce(Text key, Iterable<LevelBean> values , Context context)
                throws IOException, InterruptedException {

            HashMap<Integer,Integer>MonthAQI = new HashMap<>();
            HashMap<Integer,Integer>MonthDay = new HashMap<>();
            for (LevelBean value:values){
                MonthAQI.merge(value.getMonth(), value.getAQI(), Integer::sum);
                MonthDay.merge(value.getMonth(),1,Integer::sum);
            }
            int maxAQI_month = 1;
            int minAQI_month = 1;
            int maxAQI = 0;
            int minAQI = Integer.MAX_VALUE;
            for(int i = 1 ;i<=6;i++) {
                int avgMonthAQI = MonthAQI.get(i)/MonthDay.get(i);
                if(avgMonthAQI>maxAQI){
                    maxAQI = MonthAQI.get(i)/MonthDay.get(i);
                    maxAQI_month = i;
                }
                if (avgMonthAQI<minAQI){
                    minAQI = MonthAQI.get(i)/MonthDay.get(i);
                    minAQI_month = i;
                }
            }
            context.write(key,new Text('\t'+String.valueOf(maxAQI_month)+'\t'+String.valueOf(minAQI_month)));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);
        Job job = Job.getInstance(conf, "北京、上海和成都在2019年上半年空气质量最坏和最好的月份");
        job.setJarByClass(Level.class);

        job.setMapperClass(WorstLevelMapper.class);
        job.setReducerClass(WorstLevelReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LevelBean.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out,"task3"));

        job.waitForCompletion(true);


    }
}
