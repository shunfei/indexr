package io.indexr.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

public class Trick {
    public static <T> T on(T t, Consumer<T> op) {
        op.accept(t);
        return t;
    }

    public static <T, R> T on(T t, BiConsumer<T, R> op, R r) {
        op.accept(t, r);
        return t;
    }

    public static <T, R1, R2> T on(T t, BiConsumer<T, R1> op1, R1 r1, BiConsumer<T, R2> op2, R2 r2) {
        op1.accept(t, r1);
        op2.accept(t, r2);
        return t;
    }

    public static <T> Function<T, T> identity() {
        return x -> x;
    }

    public static <T> T find(Collection<T> c, Predicate<T> where) {
        for (T t : c) {
            if (where.test(t)) {
                return t;
            }
        }
        return null;
    }

    public static <T> int indexWhere(List<T> list, Predicate<T> where) {
        return indexFirst(list, where);
    }

    public static <T> int indexFirst(List<T> list, Predicate<T> where) {
        int i = 0;
        for (T t : list) {
            if (where.test(t)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public static <T> int indexLast(List<T> list, Predicate<T> where) {
        int i = 0;
        int index = -1;
        for (T t : list) {
            if (where.test(t)) {
                index = i;
            }
            i++;
        }
        return index;
    }

    public static <T> boolean forAll(Collection<T> c, Predicate<T> p) {
        for (T t : c) {
            if (!p.test(t)) {
                return false;
            }
        }
        return true;
    }

    @SafeVarargs
    public static <E> List<E> concatToList(Collection<? extends E> c, Collection<? extends E>... others) {
        ArrayList<E> list = new ArrayList<>();
        list.addAll(c);
        for (Collection<? extends E> oc : others) {
            list.addAll(oc);
        }
        return list;
    }

    @SafeVarargs
    public static <E> List<E> concatToList(Collection<? extends E> c, E... e) {
        ArrayList<E> list = new ArrayList<>();
        list.addAll(c);
        Collections.addAll(list, e);
        return list;
    }

    @SafeVarargs
    public static <E> Set<E> concatToSet(Collection<? extends E> c, Collection<? extends E>... others) {
        HashSet<E> set = new HashSet<>();
        set.addAll(c);
        for (Collection<? extends E> oc : others) {
            set.addAll(oc);
        }
        return set;
    }

    @SafeVarargs
    public static <E> Set<E> concatToSet(Collection<? extends E> c, E... e) {
        HashSet<E> set = new HashSet<>();
        set.addAll(c);
        Collections.addAll(set, e);
        return set;
    }

    private static boolean is(boolean... v) {
        return v.length > 0 && v[0];
    }

    public static <T, R> List<R> mapToList(Collection<T> c, Function<? super T, ? extends R> mapper, boolean... ignoreNull) {
        if (is(ignoreNull)) {
            return c.stream().map(mapper).filter(r -> r != null).collect(Collectors.toList());
        } else {
            return c.stream().map(mapper).collect(Collectors.toList());
        }
    }

    public static <T, R> Set<R> mapToSet(Collection<T> c, Function<? super T, ? extends R> mapper, boolean... ignoreNull) {
        if (is(ignoreNull)) {
            return c.stream().map(mapper).filter(r -> r != null).collect(Collectors.toSet());
        } else {
            return c.stream().map(mapper).collect(Collectors.toSet());
        }
    }

    public static <T> List<T> filterToList(Collection<T> c, Predicate<? super T> f) {
        return c.stream().filter(f).collect(Collectors.toList());
    }

    public static <T> Set<T> filterToSet(Collection<T> c, Predicate<? super T> f) {
        return c.stream().filter(f).collect(Collectors.toSet());
    }

    public static <T, R> List<R> flatMapToList(Collection<T> c, Function<? super T, ? extends Collection<? extends R>> mapper, boolean... ignoreNull) {
        if (is(ignoreNull)) {
            return c.stream().flatMap(e -> mapper.apply(e).stream()).filter(r -> r != null).collect(Collectors.toList());
        } else {
            return c.stream().flatMap(e -> mapper.apply(e).stream()).collect(Collectors.toList());
        }
    }

    public static <T, R> Set<R> flatMapToSet(Collection<T> c, Function<? super T, ? extends Collection<? extends R>> mapper, boolean... ignoreNull) {
        if (is(ignoreNull)) {
            return c.stream().flatMap(e -> mapper.apply(e).stream()).filter(r -> r != null).collect(Collectors.toSet());
        } else {
            return c.stream().flatMap(e -> mapper.apply(e).stream()).collect(Collectors.toSet());
        }
    }

    public static <T, R> R foldLeft(Collection<T> c, R o, BiFunction<? super R, ? super T, ? extends R> f) {
        R res = o;
        for (T t : c) {
            res = f.apply(res, t);
        }
        return res;
    }

    public static <E> List<E> up(List<? extends E> c) {
        return new ArrayList<>(c);
    }

    public static <E> Set<E> up(Set<? extends E> c) {
        return new HashSet<>(c);
    }

    public static <E> List<E> upList(Collection<? extends E> c) {
        return new ArrayList<>(c);
    }

    public static <E> Set<E> upSet(Collection<? extends E> c) {
        return new HashSet<>(c);
    }

    public static <E> List<E> one(E e) {
        return Collections.singletonList(e);
    }

    public static <E> List<E> empty() {
        return Collections.emptyList();
    }

    public static <E1, E2> boolean compareList(List<E1> l1, List<E2> l2, BiPredicate<E1, E2> p) {
        Iterator<E1> it1 = l1.iterator();
        Iterator<E2> it2 = l2.iterator();
        while (true) {
            boolean hasNext1 = it1.hasNext();
            boolean hasNext2 = it2.hasNext();
            if (!hasNext1 && !hasNext2) {
                return true;
            }
            if (!(hasNext1 && hasNext2)) {
                return false;
            }
            if (!p.test(it1.next(), it2.next())) {
                return false;
            }
        }
    }

    /**
     * {@link java.util.function.Function} to {@link com.google.common.base.Function}.
     */
    public static <F, T> com.google.common.base.Function<? super F, ? extends T> jfTogf(Function<? super F, ? extends T> f) {
        return new com.google.common.base.Function<F, T>() {
            @Nullable
            @Override
            public T apply(@Nullable F input) {
                return f.apply(input);
            }
        };
    }

    public static <T, K> void notRepeated(List<T> list, F<T, K> f) {
        if (list == null || list.size() == 0) {
            return;
        }
        HashSet<K> set = new HashSet<>(list.size());
        for (T t : list) {
            K k = f.f(t);
            if (!set.add(k)) {
                throw new IllegalStateException(String.format("Duplicated %s", k));
            }
        }
    }

    public static <T, K> int repeatIndex(List<T> list, F<T, K> f) {
        if (list == null || list.size() == 0) {
            return -1;
        }
        HashSet<K> set = new HashSet<>(list.size());
        int i = 0;
        for (T t : list) {
            K k = f.f(t);
            if (!set.add(k)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public static interface F<A, B> {
        B f(A a);
    }

    public static boolean equals(Object a, Object b) {
        return a == null ? b == null : (b != null) && a.equals(b);
    }
}
