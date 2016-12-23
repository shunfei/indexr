package io.indexr.query.expr.attr;

import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.types.Metadata;

import java.util.List;

import io.indexr.query.TreeNode;
import io.indexr.query.expr.Expression;
import io.indexr.query.types.DataType;

public final class AttributeReference extends Attribute {
    public final String name;
    public final DataType dataType;
    public final Metadata metadata;
    public final long exprId;

    public AttributeReference(String name, DataType dataType, Metadata metadata, long exprId) {
        this.name = name;
        this.dataType = dataType;
        this.metadata = metadata;
        this.exprId = exprId;
    }

    public AttributeReference(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
        this.metadata = Metadata.empty;
        this.exprId = NamedExpression.newExprId();
    }

    @Override
    public String name() {return name;}

    @Override
    public long exprId() {return exprId;}

    @Override
    public DataType dataType() {return dataType;}

    @Override
    public int hashCode() {
        return Long.hashCode(exprId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AttributeReference)) {
            return false;
        }
        AttributeReference other = (AttributeReference) obj;
        return this.exprId == other.exprId;
    }

    /**
     * Returns true iff the expression id is the same for both attributes.
     */
    public boolean sameRef(AttributeReference other) {
        return this.exprId == other.exprId;
    }

    @Override
    public boolean fastEquals(TreeNode o) {
        if (this == o) {
            return true;
        }
        if (o instanceof AttributeReference) {
            AttributeReference other = (AttributeReference) o;
            return this.name.equals(other.name) && this.dataType.equals(other.dataType)
                    && this.metadata.equals(other.metadata) && this.exprId == other.exprId;
        }
        return false;
    }

    @Override
    public boolean semanticEquals(Expression other) {
        return (other instanceof AttributeReference) && sameRef((AttributeReference) other);
    }

    @Override
    public String toString() { return String.format("%s#%d", name, exprId);}

    // Since the expression id is not in the first constructor it is missing from the default
    // tree string.
    @Override
    public String simpleString() {
        return String.format("%s#%d: %s", name, exprId, dataType.simpleString());
    }

    @Override
    public Attribute withNewChildren(List<Expression> newChildren) {
        return new AttributeReference(name, dataType, metadata, exprId);
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(name, dataType, metadata, exprId);}

    @Override
    public AttributeReference newInstance() {
        return new AttributeReference(name, dataType, metadata, NamedExpression.newExprId());
    }

    @Override
    public Attribute withName(String newName) {
        if (StringUtils.equals(newName, name)) {
            return this;
        } else {
            return new AttributeReference(newName, dataType, metadata, exprId);
        }
    }
}
