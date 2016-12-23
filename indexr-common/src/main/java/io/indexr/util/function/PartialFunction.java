package io.indexr.util.function;

import java.util.List;
import java.util.function.Function;

/**
 * Work exactly same as scala's PartialFunction.
 *
 * @param <A> argument type
 * @param <B> result type
 */
public interface PartialFunction<A, B> extends Function<A, B> {
    boolean isDefinedAt(A x);

    default PartialFunction<A, B> orElse(PartialFunction<A, B> that) {
        return new OrElse<>(this, that);
    }

    @Override
    default <V> PartialFunction<A, V> andThen(Function<? super B, ? extends V> after) {
        AndThen pf = new AndThen<>(this, after);
        return pf;
    }

    default Function<A, B> lift() {
        return new Lifted<>(this);
    }

    default B applyOrElse(A x, Function<? super A, ? extends B> that) {
        return isDefinedAt(x) ? apply(x) : that.apply(x);
    }

    public static <A, B> PartialFunction<A, B> fromFunction(Function<? super A, ? extends B> f) {
        return new PartialFunction<A, B>() {
            @Override
            public boolean isDefinedAt(A x) {
                return true;
            }

            @Override
            public B apply(A a) {
                return f.apply(a);
            }
        };
    }
}

class OrElse<A, B> implements PartialFunction<A, B> {
    public PartialFunction<A, B> f1, f2;

    public OrElse(PartialFunction<A, B> f1, PartialFunction<A, B> f2) {
        this.f1 = f1;
        this.f2 = f2;
    }

    @Override
    public boolean isDefinedAt(A x) {return f1.isDefinedAt(x) || f2.isDefinedAt(x);}

    @Override
    public B apply(A x) {return f1.applyOrElse(x, f2);}

    @Override
    public B applyOrElse(A x, Function<? super A, ? extends B> that) {
        if (f1.isDefinedAt(x)) {
            return f1.apply(x);
        } else {
            return f2.applyOrElse(x, that);
        }
    }
}

class OrElses<A, B> implements PartialFunction<A, B> {
    public List<PartialFunction<A, B>> pfs;

    public OrElses(List<PartialFunction<A, B>> pfs) {
        this.pfs = pfs;
    }

    @Override
    public boolean isDefinedAt(A x) {
        for (PartialFunction<A, B> pf : pfs) {
            if (pf.isDefinedAt(x)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public B apply(A x) {
        for (PartialFunction<A, B> pf : pfs) {
            if (pf.isDefinedAt(x)) {
                return pf.apply(x);
            }
        }
        throw new MatchError(x);
    }

    @Override
    public B applyOrElse(A x, Function<? super A, ? extends B> that) {
        for (PartialFunction<A, B> pf : pfs) {
            if (pf.isDefinedAt(x)) {
                return pf.apply(x);
            }
        }
        return that.apply(x);
    }
}

class AndThen<A, B extends B1, B1, C> implements PartialFunction<A, C> {
    public PartialFunction<A, B> pf;
    public Function<B1, C> k;

    public AndThen(PartialFunction<A, B> pf, Function<B1, C> k) {
        this.pf = pf;
        this.k = k;
    }

    @Override
    public boolean isDefinedAt(A x) {return pf.isDefinedAt(x);}

    @Override
    public C apply(A x) {return k.apply(pf.apply(x));}

    @Override
    public C applyOrElse(A x, Function<? super A, ? extends C> that) {
        if (pf.isDefinedAt(x)) {
            return k.apply(pf.apply(x));
        }
        return that.apply(x);
    }
}

class Lifted<A, B> implements Function<A, B> {
    public PartialFunction<A, B> pf;

    public Lifted(PartialFunction<A, B> pf) {
        this.pf = pf;
    }

    @Override
    public B apply(A x) {
        if (pf.isDefinedAt(x)) {
            return pf.apply(x);
        } else {
            return null;
        }
    }
}