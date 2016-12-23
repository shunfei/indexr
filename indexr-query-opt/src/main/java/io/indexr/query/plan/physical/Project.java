package io.indexr.query.plan.physical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;
import io.indexr.query.expr.project.UnsafeProjection;
import io.indexr.query.row.InternalRow;

public class Project extends PPUnaryNode {
    public List<NamedExpression> projectList;

    public Project(List<NamedExpression> projectList, PhysicalPlan child) {
        super(child);
        this.projectList = projectList;
    }

    @Override
    public List<Attribute> output() {
        return projectList.stream().map(NamedExpression::toAttribute).collect(Collectors.toList());
    }

    @Override
    protected Iterator<InternalRow> doExecute() {
        projectList.forEach(e -> Preconditions.checkState(e.resolved()));
        UnsafeProjection projection = UnsafeProjection.create(projectList, child.output());


        Iterator<InternalRow> childResItr = child.execute();

        return new Iterator<InternalRow>() {
            @Override
            public boolean hasNext() {
                return childResItr.hasNext();
            }

            @Override
            public InternalRow next() {
                return projection.apply(childResItr.next());
            }
        };
    }

    @Override
    public PhysicalPlan withNewChildren(List<PhysicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Project(projectList, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(projectList, child);}
}
