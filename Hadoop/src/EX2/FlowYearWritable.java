package TDE.EX2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowYearWritable implements WritableComparable<FlowYearWritable> {

    private String ano;
    private String flow;

    public FlowYearWritable() {

    }

    public FlowYearWritable(String ano, String flow) {
        this.ano = ano;
        this.flow = flow;
    }

    public String getAno() {
        return ano;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowYearWritable that = (FlowYearWritable) o;
        return Objects.equals(flow, that.flow) && Objects.equals(ano, that.ano);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ano, flow);
    }

    @Override
    public int compareTo(FlowYearWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ano);
        dataOutput.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ano = dataInput.readUTF();
        flow = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return ano + " - " + flow + " -";
    }
}
