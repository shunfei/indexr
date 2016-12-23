package io.indexr.query;

import io.indexr.query.plan.physical.PhysicalPlan;
import io.indexr.query.sql.IndexRQL;

public class QueryContext {
    private Catalog catalog;
    private Analyzer analyzer;
    private Optimizer optimizer;
    private QueryPlanner<PhysicalPlan> planner;

    public QueryContext(Catalog catalog, QueryPlanner<PhysicalPlan> planner) {
        this.catalog = catalog;
        this.planner = planner;
        this.analyzer = new Analyzer(catalog);
        this.optimizer = new Optimizer();
    }

    public Catalog catalog() {
        return catalog;
    }

    public Analyzer analyzer() {
        return analyzer;
    }

    public Optimizer optimizer() {
        return optimizer;
    }

    public QueryPlanner<PhysicalPlan> planner() {
        return planner;
    }

    public QueryExecution executeSql(String sql) {
        IndexRQL rql = new IndexRQL(sql);
        return new QueryExecution(this, rql.parseToPlan());
    }
}
