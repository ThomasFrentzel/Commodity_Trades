package TDE.EX6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LargestAveragePriceValueWritable implements WritableComparable<LargestAveragePriceValueWritable> {

    private double somaValores;
    private int qtd;

    public LargestAveragePriceValueWritable() {

    }

    public LargestAveragePriceValueWritable(double somaValores, int qtd) {
        this.somaValores = somaValores;
        this.qtd = qtd;
    }

    public double getSomaValores() {
        return somaValores;
    }

    public void setSomaValores(double somaValores) {
        this.somaValores = somaValores;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(LargestAveragePriceValueWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(somaValores);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaValores = dataInput.readDouble();
        qtd = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LargestAveragePriceValueWritable that = (LargestAveragePriceValueWritable) o;
        return Double.compare(that.somaValores, somaValores) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaValores, qtd);
    }
}
