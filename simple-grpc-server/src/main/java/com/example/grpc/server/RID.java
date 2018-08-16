package com.example.grpc.server;

public class RID implements Comparable<RID> {

    private String id;

    public RID(String id) {
        this.id = id;
    }

    public String getValue() {
        return id;
    }

    public RID copy() {
        return new RID(this.id);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof RID) {
            return ((RID) other).id.equals(this.id);
        }
        return false;
    }

    @Override
    public int compareTo(RID other) {
        return this.id.compareTo(other.id);
    }

    @Override
    public String toString() {
        return "RID: " + this.id;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }
}
