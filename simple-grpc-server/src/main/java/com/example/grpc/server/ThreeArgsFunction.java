package com.example.grpc.server;

import java.util.function.BiFunction;

/**
 * Created by ginkgo on 1/12/17.
 */
public interface ThreeArgsFunction <A, B, C, R> {
    R apply(A a, B b, C c);
    default BiFunction<B,C,R> curryA(A a){
        return (b, c)->apply(a, b, c);
    }
    default BiFunction<A,C,R> curryB(B b){
        return ((a, c) -> apply(a,b,c));
    }
    default BiFunction<A,B,R> curryC(C c){
        return (a, b) -> apply(a,b,c);
    }
}