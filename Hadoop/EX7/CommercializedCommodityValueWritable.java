package TDE.EX7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommercializedCommodityValueWritable implements WritableComparable<CommercializedCommodityValueWritable> {

    private String comm;
    private double qtd;

    public CommercializedCommodityValueWritable() {

    }

    public CommercializedCommodityValueWritable(String comm, double qtd) {
        this.comm = comm;
        this.qtd = qtd;
    }

    public String getComm() {
        return comm;
    }

    public void setComm(String comm) {
        this.comm = comm;
    }

    public double getQtd() {
        return qtd;
    }

    public void setQtd(double qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(CommercializedCommodityValueWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(comm);
        dataOutput.writeDouble(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        comm = dataInput.readUTF();
        qtd = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommercializedCommodityValueWritable that = (CommercializedCommodityValueWritable) o;
        return Double.compare(that.qtd, qtd) == 0 && Objects.equals(comm, that.comm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comm, qtd);
    }

    @Override
    public String toString() {
        return comm + '\t' + qtd + '\t';
    }
}
