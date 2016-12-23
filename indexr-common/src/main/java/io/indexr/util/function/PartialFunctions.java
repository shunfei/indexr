package io.indexr.util.function;


import com.google.common.collect.Lists;

import java.util.function.Function;

public class PartialFunctions {
    static PartialFunction<Object, Object> empty_pf = new PartialFunction<Object, Object>() {
        // @formatter:off
        @Override
        public boolean isDefinedAt(Object x) {return false;}
        @Override
        public Object apply(Object x) {throw new MatchError(x);}
        @Override
        public PartialFunction<Object, Object> orElse(PartialFunction<Object, Object> that) {return that;}
        @Override
        public <V> PartialFunction<Object, V> andThen(Function<? super Object, ? extends V> after) {return (PartialFunction<Object, V>)this;}
        @Override
        public Function<Object, Object> lift() {return null;}
        // @formatter:on
    };

    public static <A, B> PartialFunction<A, B> applyOrElse(PartialFunction<A, B>... fs) {
        if (fs.length == 0) {
            return (PartialFunction<A, B>) PartialFunctions.empty_pf;
        }
        return new OrElses<>(Lists.newArrayList(fs));
    }
}
