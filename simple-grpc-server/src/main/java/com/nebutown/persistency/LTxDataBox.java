package com.nebutown.persistency;

import com.example.grpc.server.RID;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.nebutown.cluster.Json;
import com.nebutown.grpc.RecordServiceGrpc;
import com.nebutown.grpc.TRequest;
import com.nebutown.grpc.TResponse;
import io.grpc.stub.StreamObserver;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.WeakHashMap;


public class LTxDataBox extends RecordServiceGrpc.RecordServiceImplBase {

    private static final String TableName = "holdAllTABLE";

    private static LTxDataBox inst = new LTxDataBox();

    private static LTxDataBox get() {
        return inst;
    }

    private ITable table;
    private final WeakHashMap<String, Snapshot<? extends Object>> cache;
    private final WeakHashMap<RID, Map<String, Snapshot<? extends Object>>> updates;


    private LTxDataBox() {


        cache = new WeakHashMap<>();
        updates = new WeakHashMap<>();

        //TODO: init table

    }

    @Override
    public void exec(TRequest request, StreamObserver<TResponse> responseObserver) {
        //TODO:
        throw new UnsupportedOperationException();
    }

    public static boolean tryCommit(RID request) {

        throw new UnsupportedOperationException();
    }

    public static void commit(RID request) {

        final LTxDataBox txDataBox = get();
        synchronized (txDataBox.cache) {
        final Map<String, Snapshot<?>> stringSnapshotMap = txDataBox.updates.remove(request);



            if (!mvccCollisionDetected0(txDataBox, stringSnapshotMap)){

                flushAndSyncLocalCache0(txDataBox, stringSnapshotMap);
            }else {
                throw new RuntimeException("commit failed for version conflict"+request.getValue());
            }

            System.out.println("---"+Thread.currentThread()+" flush result:"+ Joiner.on(",").withKeyValueSeparator("=").join(stringSnapshotMap));
        }
    }

    private static boolean mvccCollisionDetected0(LTxDataBox txDataBox, Map<String, Snapshot<?>> stringSnapshotMap) {

        return stringSnapshotMap.entrySet()
                .parallelStream()
                .anyMatch(e -> {

                    final Snapshot data0 = txDataBox.cache.get(e.getKey());
                    return !e.getValue().isGreaterThan(data0);
                });
    }

    private static void flushAndSyncLocalCache0(LTxDataBox txDataBox, Map<String, Snapshot<?>> stringSnapshotMap) {

        stringSnapshotMap.forEach((key, value) -> txDataBox.table.putRecord(key, Json.toJson(value)));

        txDataBox.cache.putAll(stringSnapshotMap);

        //TODO:
//        txDataBox.DBEnv.sync();
    }


    public static <T> T getData(RID request, Class<T> clazz, UUID key) {
        return getData(request, clazz, key);
    }

    @SuppressWarnings("all")
    public static <T> T getData(RID request, Class<T> clazz, String key) {

        T v = tryGetUpdates0(request, key);//read forward

        if (v != null) return v;
        final LTxDataBox txDataBox = get();
        final Map<String, Snapshot<?>> cache = txDataBox.cache;
        synchronized (txDataBox.cache){

            if (cache.containsKey(key)) {

                Snapshot<?> snapshot = cache.get(key);

                do{

                    if (snapshot.version.compareTo(request) <= 0)
                        return (T) snapshot.value;
                    else
                        snapshot = snapshot.getParent();

                }while (snapshot != null);

                return null;
            }


            String raw = getRaw(key);

            if (raw == null || raw.length() == 0) {
                return null;
            }
            T result = Json.fromJson(clazz, raw);
            cache.put(key, Snapshot.of(result, request));


            return result;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T tryGetUpdates0(RID request, String key) {

        //need not thread-safe
        return (T) Optional.ofNullable(get().updates.get(request)).map(map -> map.get(key)).map(s -> s.value).orElse(null);
    }

    public static String getRaw(String key) {
        return get().table.getRecord(key);
    }


    public static <T> void updateData(RID request, UUID key, T data) {
        LTxDataBox.updateData(request, key, data);
    }

    @SuppressWarnings("unchecked")
    public static <T> void updateData(RID request, String key, T data) {

        final Snapshot<T> snapshot ;

        final Map<String, Snapshot<?>> cache = get().cache;
        synchronized (get().cache) {

            final Snapshot<T> snapshot1 = (Snapshot<T>) cache.get(key);
            snapshot = Snapshot.of(snapshot1, data, request);

        }
        get().updates.compute(request, (k, v) -> {


            final Map<String, Snapshot<? extends Object>> stringObjectMap = Optional.ofNullable(v).orElseGet(Maps::newHashMap);
            stringObjectMap.put(key, snapshot);
            return stringObjectMap;
        });
    }

    public static void printDebugInfo() {

        System.out.println("Printing stack trace:");
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (int i = 2; i < elements.length; i++) {
            StackTraceElement s = elements[i];
            System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
        }
    }

    public static <T> void updateData(RID request, String key) {
        updateData(request, key, null);
    }



    static class Snapshot<T>{

        public static <T> Snapshot of(T value, RID version) {

            Snapshot<T> snapshot = new Snapshot<>();
            snapshot.parent = null;
            snapshot.version = version;
            snapshot.value = value;

            return snapshot;
        }

        public static <T> Snapshot of(Snapshot<T> parent, T value, RID version) {

            Snapshot<T> snapshot = new Snapshot<>();
            snapshot.parent = new WeakReference<>(parent);
            snapshot.version = version;
            snapshot.value = value;

            return snapshot;
        }

        private WeakReference<Snapshot<T>> parent;
        private RID version;
        private T value;

        private Snapshot() {

        }

        public Snapshot<T> getParent() {

            return Optional.ofNullable(parent).map(WeakReference::get).orElse(null);
        }

        public void setParent(WeakReference<Snapshot<T>> parent) {
            this.parent = parent;
        }

        public RID getVersion() {
            return version;
        }

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }

        private String parentStr() {

            if (parent == null || parent.get() == null)
                return "null";
            else {

                final Snapshot<T> parentS = parent.get();
                return parentS.version.getValue()+"->"+parentS.parentStr();
            }
        }

        @Override
        public String toString() {
            return "Snapshot{" +
                    "parent["+parentStr()+"]"+
                    ", version=" + version.getValue() +
                    ", value=" + value +
                    '}';
        }

        /**
         *
         *
         * @param other
         * @return
         */
        public boolean isGreaterThan(Snapshot other) {
            if (other == null) return true;

            return version.compareTo(other.version) >= 0;

        }
    }
}
