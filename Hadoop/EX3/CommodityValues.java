package TDE.EX3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class CommodityValues {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "value");

        j.setJarByClass(CommodityValues.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

   
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CommodValueWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, CommodValueWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String ano = colunas[1];

                double valor = Double.parseDouble(colunas[5]);
                int qtd = 1;

                con.write(new Text(ano),
                        new CommodValueWritable(valor, qtd));
            }
        }
    }
    public static class CombineForAverage extends Reducer<Text, CommodValueWritable, Text, CommodValueWritable>{

        public void reduce(Text key, Iterable<CommodValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaVals = 0;
            int somaQtds = 0;
            for(CommodValueWritable o : values){
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }
            con.write(key, new CommodValueWritable(somaVals, somaQtds));
        }
    }
    public static class ReduceForAverage extends Reducer<Text, CommodValueWritable, Text, DoubleWritable> {

        private int count = 0;

        public void reduce(Text key, Iterable<CommodValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaVals = 0;
            int somaQtds = 0;
            for (CommodValueWritable o : values){
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            double media = somaVals / somaQtds;

            if (count <= 4) {
                con.write(key, new DoubleWritable(media));
                count++;
            }
        }
    }
}
