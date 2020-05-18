//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class lab4_myWritable implements Writable {
    private int value;
    private int file_name;

    public lab4_myWritable() {
    }

    public lab4_myWritable(int value) {
        this.set(value);
    }

    public void set(int value) {
        this.value = value;
    }
    public void setB(int temp) {
        this.file_name = temp;
    }

    public int get() {
        return this.value;
    }

    public void readFields(DataInput in) throws IOException {
        this.value = in.readInt();
        this.file_name = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.value);
//        out.writeChars(this.file_name);
        out.writeInt(this.file_name);
    }

    public boolean equals(Object o) {
        if (!(o instanceof lab4_myWritable)) {
            return false;
        } else {
            lab4_myWritable other = (lab4_myWritable)o;
            return this.value == other.value;
        }
    }
//
//    public int hashCode() {
//        return this.value;
//    }
//
//    public int compareTo(lab4_myWritable o) {
//        int thisValue = this.value;
//        int thatValue = o.value;
//        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
//    }

    public String toString() {
        return Integer.toString(this.value);
    }

//    static {
//        WritableComparator.define(lab4_myWritable.class, new lab4_myWritable.Comparator());
//    }

//    public static class Comparator extends WritableComparator {
//        public Comparator() {
//            super(lab4_myWritable.class);
//        }
//
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            int thisValue = readInt(b1, s1);
//            int thatValue = readInt(b2, s2);
//            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
//        }
//    }
}
