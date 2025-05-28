import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageAge {  // Class name matches filename
    
    public static class AgeMapper 
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        private final static Text keyText = new Text("average_age");
        private DoubleWritable ageValue = new DoubleWritable();
        
        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
            
            if (value.toString().contains("subject_id")) return;
            
            String[] parts = value.toString().split(",");
            if (parts.length >= 4) {
                try {
                    double age = Double.parseDouble(parts[3].trim());
                    ageValue.set(age);
                    context.write(keyText, ageValue);
                } catch (NumberFormatException e) {
                    // Skip malformed values
                }
            }
        }
    }
    
    public static class AverageReducer 
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        private DoubleWritable result = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
            
            double sum = 0;
            int count = 0;
            
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            
            if (count > 0) {
                result.set(sum / count);
                context.write(new Text("Average Patient Age"), result);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average age calculation");
        
        job.setJarByClass(AverageAge.class);  // Updated to match new class name
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AverageReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
