package io.indexr.query.expr.attr;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.UnresolvedException;
import io.indexr.query.expr.Expression;
import io.indexr.query.types.DataType;

public class UnresolvedAttribute extends Attribute {
    public String nameParts;

    public UnresolvedAttribute(String nameParts) {
        this.nameParts = nameParts;
    }

    @Override
    public String name() {return nameParts;}

    @Override
    public String toString() {return name();}

    @Override
    public Attribute withNewChildren(List<Expression> newChildren) {return new UnresolvedAttribute(nameParts);}

    @Override
    public List<Object> args() {return Lists.newArrayList(nameParts);}

    @Override
    public Attribute newInstance() {return this;}

    @Override
    public Attribute withName(String newName) {
        return new UnresolvedAttribute(newName);
    }

    // @formatter:off
    @Override public boolean resolved() {return false;}
    @Override public long exprId() {throw new UnresolvedException(this, "exprId");}
    @Override public DataType dataType() {throw new UnresolvedException(this, "dataType");}
    // @formatter:on

}
