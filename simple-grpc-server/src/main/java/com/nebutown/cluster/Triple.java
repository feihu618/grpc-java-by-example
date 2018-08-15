package com.nebutown.cluster;

import java.util.Objects;

public class Triple<First,Second,Third> {

    private  Third third;
    private  Second second;
    private  First first;

    public Triple(First first, Second second, Third third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public Third getThird() {
        return third;
    }

    public Second getSecond() {
        return second;
    }

    public First getFirst() {
        return first;
    }

    public void setThird(Third third) {
        this.third = third;
    }

    public void setSecond(Second second) {
        this.second = second;
    }

    public void setFirst(First first) {
        this.first = first;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
        return Objects.equals(third, triple.third) &&
                Objects.equals(second, triple.second) &&
                Objects.equals(first, triple.first);
    }

    @Override
    public int hashCode() {
        return Objects.hash(third, second, first);
    }

    @Override
    public String toString() {
        return "Triple{" +
                "third=" + third +
                ", second=" + second +
                ", first=" + first +
                '}';
    }
}
