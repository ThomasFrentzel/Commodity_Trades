package TDE.EX2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class TransactionsFlowYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "flow_year");

        j.setJarByClass(TransactionsFlowYear.class);
        j.setMapperClass(MapForFlowYear.class);
        j.setReducerClass(ReduceForFlowYear.class); 

        j.setCombinerClass(CombineForFlowYear.class); 

        j.setMapOutputKeyClass(FlowYearWritable.class); 
        j.setMapOutputValueClass(IntWritable.class); 

        j.setOutputKeyClass(FlowYearWritable.class);
        j.setOutputValueClass(IntWritable.class); 

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);

    }

    public static class MapForFlowYear extends Mapper<LongWritable, Text, FlowYearWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String ano = colunas[1];
                String flow = colunas[4];

                FlowYearWritable keys = new FlowYearWritable(ano, flow);
                IntWritable valor = new IntWritable(1);

                con.write(keys, valor);
            }

        }
    }

    public static class CombineForFlowYear extends Reducer<FlowYearWritable, IntWritable, FlowYearWritable, IntWritable> {

        private int count = 0;

        public void reduce(FlowYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont = 0;

            for (IntWritable v : values) {
                cont += v.get();
            }

            if (count <= 4) {
                con.write(key, new IntWritable(cont));
                count++;
            }
        }
    }

    public static class ReduceForFlowYear extends Reducer<FlowYearWritable, IntWritable, FlowYearWritable, IntWritable> {

        private int count = 0;

        public void reduce(FlowYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont = 0;

            for (IntWritable v : values) {
                cont += v.get();
            }

            if (count <= 4) {
                con.write(key, new IntWritable(cont));
                count++;
            }
        }
    }


}
