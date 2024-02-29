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
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "number_transactions");

        // Registro de Classes
        j.setJarByClass(NumberTransactions.class); // classe que contem o método MAIN
        j.setMapperClass(MapForNumberTransactions.class); // classe que contem o método MAP
        j.setReducerClass(ReduceForNumberTransactions.class); // classe que contem o método REDUCE

        j.setCombinerClass(CombineForNumberTransactions.class); // classe que contem o método COMBINER

        // Definir os tipos de saída
        j.setMapOutputKeyClass(Text.class); // tipo de chave de saída do MAP
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saída do MAP
        j.setOutputKeyClass(Text.class); // tipo de chave de saida do REDUCE
        j.setOutputValueClass(IntWritable.class); // tipo de valor de saída do REDUCE

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);

    }

    public static class MapForNumberTransactions extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Covertendo a linha de entrada em uma string
            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                // quebrando em colunas
                String colunas[] = linha.split(";");

                // getting the key
                String pais = colunas[0];

                // checking if the country of the transaction is Brazil
                if (pais.equals("Brazil")) {
                    Text chave = new Text(pais);
                    IntWritable valor = new IntWritable(1);

                    // chave e valor pro reduce
                    con.write(chave, valor);
                }
        }
    }
}

    public static class CombineForNumberTransactions extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // Criando varíavel de contagem
            int contagem = 0;

            // Varrendo a lista e incrementando a contagem
            for(IntWritable v : values) {
                contagem += v.get();
            }
            // Salvando os resultados em disco
            con.write(key, new IntWritable(contagem));
        }
    }

    public static class ReduceForNumberTransactions extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // Criando varíavel de contagem
            int contagem = 0;

            // Varrendo a lista e incrementando a contagem
            for(IntWritable v : values) {
                contagem += v.get();
            }

            // Salvando os resultados em disco
            con.write(key, new IntWritable(contagem));
        }
    }


}
