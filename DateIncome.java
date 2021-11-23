/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pm10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Alessandro
 */
public class DateIncome implements Writable {

    String date;
    Float income;

    public DateIncome() {
        this.date = "";
        this.income = Float.MIN_VALUE;
    }

    public DateIncome(String date, Float income) {
        this.date = date;
        this.income = income;
    }

    public String getDate() {
        return date;
    }

    public Float getIncome() {
        return income;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setIncome(Float income) {
        this.income = income;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(income);
        out.writeUTF(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        income = in.readFloat();
        date = in.readUTF();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + Objects.hashCode(this.income);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DateIncome other = (DateIncome) obj;
        return Objects.equals(this.income, other.income);
    }

    @Override
    public String toString() {
        return "DateIncome{" + "date=" + date + ", income=" + income + '}';
    }

}
