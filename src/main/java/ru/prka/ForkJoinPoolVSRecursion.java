package ru.prka;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.IntStream;

import static java.nio.file.Files.list;

/**
 * Created by abalyshev on 04.09.17.
 */
public class ForkJoinPoolVSRecursion {
    private static List<Long> recAvg = new CopyOnWriteArrayList<>();
    private static List<Long> fjpAvg = new CopyOnWriteArrayList<>();
    private static ForkJoinPool pool = new ForkJoinPool();

    public static void main(String[] args) throws Exception {
        System.out.println("ForkJoinPool vs Recursion comparison...");

        if (args.length < 1)
            throw new IllegalStateException("Scan path not found!");

        String scanPath = args[0];
        Path path = Paths.get(scanPath);
        final int limit = 25;

        Thread t1 = new Thread(() -> IntStream.range(0, limit).forEach(ndx -> {
            long before = System.currentTimeMillis();
            compute(makeDataNode(path));
            long takes = System.currentTimeMillis() - before;
            recAvg.add(takes);
            System.out.printf("[Step::%s] Recursion takes %sms\n", ndx, takes);
        }));

        Thread t2 = new Thread(() -> IntStream.range(0, limit).forEach(ndx -> {
            long before = System.currentTimeMillis();
            pool.invoke(new DataTask(makeDataNode(path)));
            long takes = System.currentTimeMillis() - before;
            fjpAvg.add(takes);
            System.out.printf("[Step::%s] ForkJoinPool takes %sms\n", ndx, takes);
        }));

        t1.start();
        t1.join();

        t2.start();
        t2.join();

        double recResult = recAvg.stream().mapToLong(v -> v).average().orElse(-1);
        double fjpResult = fjpAvg.stream().mapToLong(v -> v).average().orElse(-1);
        System.out.printf("takes with Recursion method %sms\n", recResult);
        System.out.printf("takes with ForkJoinPool %sms\n", fjpResult);
        System.out.printf("Winner is %s with result %sms\n",
                recResult < fjpResult ? "Recursion" : "ForkJoinPool",
                recResult < fjpResult ? recResult : fjpResult);
    }

    public static void printNode(DataNode node, int depth) {
        Path path = node.getPath();
        System.out.printf("%s%s%s\n",
                indent(depth), path.getFileName(), node.isLeaf() ? String.format("%skb", path.toFile().length() / 1000) : "/");
        if (!node.isLeaf()) {
            node.getChilds().forEach(sub -> printNode(sub, depth+1));
        }
    }

    public static String indent(int depth) {
        final String[] indent = {""};
        IntStream.range(0, depth).forEach(ndx -> indent[0] += "  ");
        return indent[0];
    }

    public static class DataTask extends RecursiveTask<DataNode> {
        private DataNode parent;
        public DataTask(DataNode parent) {
            this.parent = parent;
        }
        @Override
        protected DataNode compute() {
            List<DataTask> subTasks = new LinkedList<>();
            try {
                if (!parent.isLeaf()) {
                    list(parent.getPath()).forEach(p -> {
                        if (p.toFile().isDirectory()) {
                            DataTask task = new DataTask(makeDataNode(p));
                            task.fork();
                            subTasks.add(task);
                        } else if (p.toFile().isFile()) {
                            parent.getChilds().add(makeDataNode(p));
                        }
                    });
                }
                for(DataTask task : subTasks) {
                    parent.getChilds().add(task.join());
                }
                return parent;
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }
    }

    public static class DataNode {
        private Path path;
        private boolean leaf;
        private byte[] data;
        private Collection<DataNode> childs;
        public DataNode(Path path, boolean leaf, byte[] bytes, Collection<DataNode> childs) {
            this.path = path;
            this.leaf = leaf;
            this.childs = childs;
        }
        public Path getPath() { return path; }
        public boolean isLeaf() { return leaf; }
        public byte[] getData() { return data; }
        public Collection<DataNode> getChilds() { return childs; }
    }

    public static DataNode makeDataNode(Path path) {
        File file = path.toFile();
        byte[] bytes = {};
        if (file.isFile()) {
            try {
                //bytes = readAllBytes(path); // todo: шоб не убивать jvm
                //readAllBytes(path);
                bytes = new byte[] {};
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }
        return new DataNode(path, file.isFile(), bytes, new LinkedList<>());
    }

    public static DataNode compute(DataNode parent) {
        try {
            if (!parent.isLeaf()) {
                list(parent.getPath()).forEach(p -> {
                    if (p.toFile().isDirectory()) {
                        DataNode subNode = compute(makeDataNode(p));
                        parent.getChilds().add(subNode);
                    } else {
                        parent.getChilds().add(makeDataNode(p));
                    }
                });
            }
            return parent;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
