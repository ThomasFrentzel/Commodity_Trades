package TDE.EX7;

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


public class CommercializedCommodity {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/ex7.tmp");

        Path output = new Path(files[1]);

        Job j1 = new Job(c, "commodity1");
        j1.setJarByClass(CommercializedCommodity.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);
        j1.setCombinerClass(CombineEtapaA.class);
        j1.setMapOutputKeyClass(CommercializedCommodityKeyWritable.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(CommercializedCommodityKeyWritable.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        j1.waitForCompletion(false);

        Job j2 = new Job(c, "commodity2");
        j2.setJarByClass(CommercializedCommodity.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setCombinerClass(CombineEtapaB.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(CommercializedCommodityValueWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(CommercializedCommodityValueWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);


        j2.waitForCompletion(false);


    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, CommercializedCommodityKeyWritable, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String ano = colunas[1];

                if (ano.equals("2016")) {

                    String flow = colunas[4];
                    String commodity = colunas[3];

                    double amount = Double.parseDouble(colunas[8]);

                    CommercializedCommodityKeyWritable chaves = new CommercializedCommodityKeyWritable(flow, commodity);
                    DoubleWritable valor = new DoubleWritable(amount);

                    con.write(chaves, valor);
                }
            }
        }
    }

    public static class CombineEtapaA extends Reducer<CommercializedCommodityKeyWritable, DoubleWritable, CommercializedCommodityKeyWritable, DoubleWritable> {
        public void reduce(CommercializedCommodityKeyWritable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double sum = 0.0;

            for (DoubleWritable d : values) {
                sum += d.get();
            }

            con.write(key, new DoubleWritable(sum));
        }
    }

    public static class ReduceEtapaA extends Reducer<CommercializedCommodityKeyWritable, DoubleWritable, CommercializedCommodityKeyWritable, DoubleWritable> {
        public void reduce(CommercializedCommodityKeyWritable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double sum = 0.0;

            for (DoubleWritable d : values) {
                sum += d.get();
            }

            con.write(key, new DoubleWritable(sum));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, CommercializedCommodityValueWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String linhas[] = linha.split("\t");

            String flow = linhas[0];

   
            String commodity = linhas[1];
            double qtd = Double.parseDouble(linhas[2]);

            Text chave = new Text(flow);
            CommercializedCommodityValueWritable valores = new CommercializedCommodityValueWritable(commodity, qtd);

            con.write(chave, valores);

        }
    }

    public static class CombineEtapaB extends Reducer<Text, CommercializedCommodityValueWritable, Text, CommercializedCommodityValueWritable> {
        public void reduce(Text key, Iterable<CommercializedCommodityValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double largest = 0.0;
            String commodity = "";


            for (CommercializedCommodityValueWritable c : values) {
                if (c.getQtd() > largest) {
                    largest = c.getQtd();
                    commodity = c.getComm();
                }
            }

            CommercializedCommodityValueWritable valores = new CommercializedCommodityValueWritable(commodity, largest);

            con.write(key, valores);
        }
    }

    public static class ReduceEtapaB extends Reducer<Text, CommercializedCommodityValueWritable, Text, CommercializedCommodityValueWritable> {
        public void reduce(Text key, Iterable<CommercializedCommodityValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double largest = 0.0;
            String commodity = "";


            for (CommercializedCommodityValueWritable c : values) {
                if (c.getQtd() > largest) {
                    largest = c.getQtd();
                    commodity = c.getComm();
                }
            }

            CommercializedCommodityValueWritable valores = new CommercializedCommodityValueWritable(commodity, largest);

            con.write(key, valores);
        }
    }
}
