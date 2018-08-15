package com.example.grpc.server;

import java.util.Objects;

public class Tuple<S, T> {
    public static <S, T> Tuple<S, T> of(S s, T t) {
        return new Tuple<>(s, t);
    }
    private S s;
    private T t;

    public Tuple() {

    }

    public Tuple(S s, T t) {
        this.s = s;
        this.t = t;
}

    public S getS() {
        return s;
    }

    public void setS(S s) {
        this.s = s;
    }

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "s=" + s +
                ", t=" + t +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return Objects.equals(s, tuple.s) &&
                Objects.equals(t, tuple.t);
    }

    @Override
    public int hashCode() {
        return Objects.hash(s, t);
    }
}
