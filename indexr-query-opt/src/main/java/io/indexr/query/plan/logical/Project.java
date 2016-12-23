package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;

public class Project extends LPUnaryNode {
    public List<NamedExpression> projectList;

    public Project(List<NamedExpression> projectList, LogicalPlan child) {
        super(child);
        this.projectList = projectList;
    }

    @Override
    public LogicalPlan child() {
        return child;
    }

    @Override
    public List<Attribute> output() {
        return projectList.stream().map(e -> e.toAttribute()).collect(Collectors.toList());
    }

    //@Override
    //public List<Expression> expressions() {
    //    return new ArrayList<>(projectList);
    //}

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Project(projectList, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(projectList, child);}
}
