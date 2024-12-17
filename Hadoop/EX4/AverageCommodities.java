package TDE.EX4;

import TDE.EX3.CommodValueWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;

public class AverageCommodities {


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "averageCommodity");

        j.setJarByClass(AverageCommodities.class);
        j.setMapperClass(MapForAverageCommodities.class);
        j.setReducerClass(ReduceForAverageCommodities.class);

        j.setCombinerClass(CombineForAverageCommodities.class);

  
        j.setMapOutputKeyClass(AverageCommodityKeyWritable.class);
        j.setMapOutputValueClass(AverageCommodityValueWritable.class);

        j.setOutputKeyClass(AverageCommodityKeyWritable.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class MapForAverageCommodities extends Mapper<LongWritable, Text, AverageCommodityKeyWritable, AverageCommodityValueWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String pais = colunas[0];
                String flow = colunas[4];

                if (pais.equals("Brazil")) {

                    if (flow.equals("Export")) {

                        String ano = colunas[1];
                        String unitType = colunas[7];
                        String categoria = colunas[9];

                        double valor = Double.parseDouble(colunas[5]);
                        int qtd = 1;

                        AverageCommodityKeyWritable chaves = new AverageCommodityKeyWritable(ano, unitType, categoria);
                        AverageCommodityValueWritable valores = new AverageCommodityValueWritable(valor, qtd);

                        con.write(chaves, valores);
                    }
                }
                }
            }
        }

    public static class CombineForAverageCommodities extends Reducer<AverageCommodityKeyWritable, AverageCommodityValueWritable, AverageCommodityKeyWritable, AverageCommodityValueWritable> {

        public void reduce(AverageCommodityKeyWritable key, Iterable<AverageCommodityValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaVals = 0.0;
            int somaQtds = 0;
            for(AverageCommodityValueWritable o : values){
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            con.write(key, new AverageCommodityValueWritable(somaVals, somaQtds));
        }
    }
    public static class ReduceForAverageCommodities extends Reducer<AverageCommodityKeyWritable, AverageCommodityValueWritable, AverageCommodityKeyWritable, DoubleWritable> {

        private int count = 0;

        public void reduce(AverageCommodityKeyWritable key, Iterable<AverageCommodityValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaVals = 0.0;
            int somaQtds = 0;
            for (AverageCommodityValueWritable o : values){
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
