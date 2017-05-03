package io.indexr.segment.rc;

import com.google.common.collect.Lists;

import org.junit.Test;

import io.indexr.segment.SQLType;

public class RCTest {
    @Test
    public void test() {
        RCOperator op = new Not(new Not(new Not(new Not(new Not(new Or(Lists.newArrayList(new Greater(new Attr("a", SQLType.DATE), 0, "2016-10-01"), new NotEqual(new Attr("b", SQLType.INT), 100, (String) null))))))));
        RCOperator newOp = op.optimize();
        System.out.println(newOp);
    }
}
