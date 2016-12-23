package io.indexr.query.plan.physical;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.InterpretedPredicate;
import io.indexr.query.expr.SortOrder;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.project.InterpretedMutableProjection;
import io.indexr.query.expr.project.MutableProjection;
import io.indexr.query.plan.QueryPlan;
import io.indexr.query.row.InternalRow;
import io.indexr.query.util.InterpretedOrdering;

public abstract class PhysicalPlan extends QueryPlan<PhysicalPlan> {
    private AtomicBoolean prepareCalled = new AtomicBoolean(false);

    public final Iterator<InternalRow> execute() {
        prepare();
        return doExecute();
    }

    public final void prepare() {
        if (prepareCalled.compareAndSet(false, true)) {
            doPrepare();
            children().stream().forEach(PhysicalPlan::prepare);
        }
    }

    protected void doPrepare() {}

    protected abstract Iterator<InternalRow> doExecute();

    protected Supplier<MutableProjection> newMutableProjection(
            List<Expression> expressions, List<Attribute> inputSchema) {
        return () -> new InterpretedMutableProjection(expressions, inputSchema);
    }

    protected Predicate<InternalRow> newPredicate(
            Expression expression, List<Attribute> inputSchema) {
        return InterpretedPredicate.create(expression, inputSchema);
    }

    protected Comparator<InternalRow> newOrdering(
            List<SortOrder> order, List<Attribute> inputSchema) {
        return new InterpretedOrdering(order, inputSchema);
    }

    @Override
    public String nodeName() {
        return "physical." + getClass().getSimpleName();
    }
}



