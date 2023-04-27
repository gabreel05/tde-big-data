
package tde;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionWritable implements WritableComparable<TransactionWritable> {

    private int year;
    private String unitType;
    double average;
    double min;
    double max;

    public TransactionWritable(int year, String unitType) {
        this.year = year;
        this.unitType = unitType;
    }

    public TransactionWritable() {
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }

    public double getAverage(){
        return average;
    }

    public void setAverage(Double average){
        this.average = average;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionWritable that = (TransactionWritable) o;
        return year == that.year && Objects.equals(unitType, that.unitType) && average == that.average;
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, unitType);
    }

    @Override
    public int compareTo(TransactionWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeUTF(unitType);
        dataOutput.writeDouble(average);
    }

    @Override
    public String toString() {
        return "TransactionWritable{" +
                "year=" + year +
                ", unit type='" + unitType + '\'' +
                ", Minimo ='" + min + '\'' +
                ", Maximo ='" + max + '\'' +
                '}';
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        unitType = dataInput.readUTF();
    }
}

