package tde;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class NumberOfTransactionsPerFlowTypeAndYearWritable
        implements WritableComparable<NumberOfTransactionsPerFlowTypeAndYearWritable> {
    private String flowType;
    private Integer year;

    public NumberOfTransactionsPerFlowTypeAndYearWritable(String flowType, Integer year) {
        this.flowType = flowType;
        this.year = year;
    }

    public NumberOfTransactionsPerFlowTypeAndYearWritable() {
    }

    public String getFlowType() {
        return flowType;
    }

    public void setFlowType(String flowType) {
        this.flowType = flowType;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NumberOfTransactionsPerFlowTypeAndYearWritable that = (NumberOfTransactionsPerFlowTypeAndYearWritable) o;
        return Objects.equals(flowType, that.flowType) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowType, year);
    }

    @Override
    public int compareTo(NumberOfTransactionsPerFlowTypeAndYearWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.flowType);
        dataOutput.writeInt(this.year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.flowType = dataInput.readUTF();
        this.year = dataInput.readInt();
    }
}
