package io.indexr.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class PairList<F, S> {
    private final List<Pair<F, S>> list;

    public PairList(List<Pair<F, S>> list) {
        this.list = list;
    }

    public PairList(int cap) {
        list = new ArrayList<>(cap);
    }

    public PairList() {
        list = new ArrayList<>();
    }

    public static <F, S> PairList<F, S> ofFList(List<F> fList) {
        List<Pair<F, S>> list = new ArrayList<>(fList.size());
        for (F f : fList) {
            list.add(Pair.of(f, null));
        }
        return new PairList<F, S>(list);
    }

    public static <F, S> PairList<F, S> ofSList(List<S> sList) {
        List<Pair<F, S>> list = new ArrayList<>(sList.size());
        for (S s : sList) {
            list.add(Pair.of(null, s));
        }
        return new PairList<F, S>(list);
    }

    public int size() {
        return list.size();
    }

    public void append(F f, S s) {
        list.add(new Pair<F, S>(f, s));
    }

    public void appendAll(PairList<F, S> newList) {
        list.addAll(newList.list);
    }

    public F firstAt(int index) {
        return list.get(index).first;
    }

    public void updateFirstAt(int index, F f) {
        list.get(index).first = f;
    }

    public S secondAt(int index) {
        return list.get(index).second;
    }

    public void updateSecondAt(int index, S s) {
        list.get(index).second = s;
    }

    public void update(int index, F f, S s) {
        list.set(index, Pair.of(f, s));
    }

    public F endFirst() {
        return list.size() == 0 ? null : list.get(list.size() - 1).first;
    }

    public S endSecond() {
        return list.size() == 0 ? null : list.get(list.size() - 1).second;
    }

    public void removeEnd() {
        list.remove(list.size() - 1);
    }

    public void forEachFist(Consumer<? super F> action) {
        Objects.requireNonNull(action);
        for (Pair<F, S> t : list) {
            action.accept(t.first);
        }
    }

    public void forEachSecond(Consumer<? super S> action) {
        Objects.requireNonNull(action);
        for (Pair<F, S> t : list) {
            action.accept(t.second);
        }
    }

    public void clear() {
        list.clear();
    }
}
