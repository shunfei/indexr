package io.indexr.query.expr.attr;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.UnresolvedException;
import io.indexr.query.expr.Expression;
import io.indexr.query.types.DataType;

/**
 * A place holder used when printing expressions without debugging information such as the
 * expression id or the unresolved indicator.
 */
public class PrettyAttribute extends Attribute {
    public String name;
    public DataType dataType = DataType.IntegerType;

    public PrettyAttribute(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public PrettyAttribute(String name) {
        this.name = name;
    }

    @Override
    public Attribute withNewChildren(List<Expression> newChildren) {
        return new PrettyAttribute(name);
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(name, dataType);}

    // @formatter:off
    @Override public String name() {throw new UnresolvedException(this, "name");}
    @Override public long exprId() {throw new UnresolvedException(this, "exprId");}
    @Override public Attribute toAttribute() {throw new UnresolvedException(this, "toAttribute");}
    @Override public DataType dataType() {throw new UnresolvedException(this, "dataType");}
    @Override public PrettyAttribute newInstance() {throw new UnresolvedException(this, "newInstance");}
    @Override public Attribute withName(String newName) {throw new UnresolvedException(this, "withName");}
    // @formatter:on
}
