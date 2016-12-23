package io.indexr.query.plan.physical;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.execution.aggregate.TungstenAggregationIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import io.indexr.query.expr.agg.AggregateExpression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.row.UnsafeRow;

import static io.indexr.util.Trick.concatToSet;
import static io.indexr.util.Trick.flatMapToList;
import static io.indexr.util.Trick.mapToList;

public class TungstenAggregate extends PPUnaryNode {
    public List<NamedExpression> groupingExpressions;
    public List<AggregateExpression> aggregateExpressions;
    public List<Attribute> aggregateAttributes;
    public int initialInputBufferOffset;
    public List<NamedExpression> resultExpressions;

    private List<Attribute> aggregateBufferAttributes;

    public TungstenAggregate(List<NamedExpression> groupingExpressions,
                             List<AggregateExpression> aggregateExpressions,
                             List<Attribute> aggregateAttributes,
                             int initialInputBufferOffset,
                             List<NamedExpression> resultExpressions,
                             PhysicalPlan child) {
        super(child);
        this.groupingExpressions = groupingExpressions;
        this.aggregateExpressions = aggregateExpressions;
        this.aggregateAttributes = aggregateAttributes;
        this.initialInputBufferOffset = initialInputBufferOffset;
        this.resultExpressions = resultExpressions;

        this.aggregateBufferAttributes = flatMapToList(aggregateExpressions, e -> e.aggregateFunction.aggBufferAttributes());
    }

    @Override
    public List<Attribute> output() {
        return mapToList(resultExpressions, NamedExpression::toAttribute);
    }

    @Override
    public Set<Attribute> producedAttributes() {
        LinkedList<NamedExpression> resultExpressionsCopy = new LinkedList<>(resultExpressions);
        resultExpressionsCopy.removeAll(groupingExpressions);
        return concatToSet(aggregateAttributes,
                mapToList(resultExpressionsCopy, NamedExpression::toAttribute),
                aggregateBufferAttributes);
    }

    @Override
    public PhysicalPlan withNewChildren(List<PhysicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new TungstenAggregate(
                groupingExpressions,
                aggregateExpressions,
                aggregateAttributes,
                initialInputBufferOffset,
                resultExpressions,
                newChildren.get(0));
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(
                groupingExpressions,
                aggregateExpressions,
                aggregateAttributes,
                resultExpressions,
                child
        );
    }

    @Override
    public String simpleString() {
        String keyString = "[" + StringUtils.join(groupingExpressions, ',') + "]";
        String functionString = "[" + StringUtils.join(aggregateExpressions, ',') + "]";
        String inputAttrsString = "[" + StringUtils.join(aggregateAttributes, ",") + "]";
        String outputString = "[" + StringUtils.join(output(), ',') + "]";
        return String.format("physical.TungstenAggregate(key=%s, functions=%s, input=%s, output=%s)", keyString, functionString, inputAttrsString, outputString);
    }

    @Override
    protected Iterator<InternalRow> doExecute() {
        Iterator<InternalRow> chidlResIter = child.execute();

        //List<InternalRow> childResList = Helper.toList(chidlResIter);

        //Iterator<InternalRow> iter = childResList.iterator();
        boolean hasInput = chidlResIter.hasNext();
        if (!hasInput && !groupingExpressions.isEmpty()) {
            // This is a grouped aggregate and the input iterator is empty,
            // so return an empty iterator.
            return Iterators.emptyIterator();
        } else {
            TungstenAggregationIterator aggregationIterator =
                    new TungstenAggregationIterator(
                            groupingExpressions,
                            aggregateExpressions,
                            aggregateAttributes,
                            initialInputBufferOffset,
                            resultExpressions,
                            this::newMutableProjection,
                            child.output(),
                            chidlResIter);
            if (!hasInput && groupingExpressions.isEmpty()) {
                UnsafeRow singleRowResult = aggregationIterator.outputForEmptyGroupingKeyWithoutInput();
                return Iterators.singletonIterator(singleRowResult);
            } else {

                List<InternalRow> aggResList = toList(aggregationIterator);

                return aggResList.iterator();
                //return StreamSupport.stream(Spliterators.spliteratorUnknownSize(aggregationIterator, 0), false);
            }
        }
    }

    private static List<InternalRow> toList(Iterator<? extends InternalRow> it) {
        List<InternalRow> list = new ArrayList<>();
        while (it.hasNext()) {
            InternalRow r = it.next();
            if (r instanceof UnsafeRow) {
                list.add(((UnsafeRow) r).copy());
            } else {
                list.add(it.next());
            }
        }
        return list;
    }
}
