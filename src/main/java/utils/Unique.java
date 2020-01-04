package utils;

public class Unique implements Comparable<Unique> {

    private long key;

    public Unique(long key) {
        this.key = key;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public int compareTo(Unique o) {
        return new UniqueComparator().compare(this, o);
    }
}

