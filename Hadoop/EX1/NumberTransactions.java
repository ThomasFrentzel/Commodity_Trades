package TDE.EX1;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class NumberTransactions {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "number_transactions");

        j.setJarByClass(NumberTransactions.class); 
        j.setMapperClass(MapForNumberTransactions.class);
        j.setReducerClass(ReduceForNumberTransactions.class);

        j.setCombinerClass(CombineForNumberTransactions.class); 

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class); 
        j.setOutputValueClass(IntWritable.class);

       
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);

    }

    public static class MapForNumberTransactions extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String pais = colunas[0];

                if (pais.equals("Brazil")) {
                    Text chave = new Text(pais);
                    IntWritable valor = new IntWritable(1);

                    con.write(chave, valor);
                }
        }
    }
}

    public static class CombineForNumberTransactions extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int contagem = 0;

            for(IntWritable v : values) {
                contagem += v.get();
            }
            con.write(key, new IntWritable(contagem));
        }
    }

    public static class ReduceForNumberTransactions extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int contagem = 0;

            for(IntWritable v : values) {
                contagem += v.get();
            }

            con.write(key, new IntWritable(contagem));
        }
    }


}
