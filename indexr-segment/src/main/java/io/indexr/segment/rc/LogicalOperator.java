package io.indexr.segment.rc;

import java.util.Collection;
import java.util.Collections;

public interface LogicalOperator extends RCOperator {
    @Override
    default Collection<Attr> attr() {
        return Collections.emptySet();
    }
}
