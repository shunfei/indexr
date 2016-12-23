package io.indexr.query;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.function.Supplier;

import io.indexr.query.plan.physical.PhysicalPlan;

public class BasicPlanner extends IndexRStrategies {
    private final Supplier<Strategy<PhysicalPlan>> customStrategy;

    public BasicPlanner(Supplier<Strategy<PhysicalPlan>> customStrategy) {
        this.customStrategy = customStrategy;
    }

    public BasicPlanner(Strategy<PhysicalPlan> strategy) {
        this.customStrategy = () -> strategy;
    }

    public BasicPlanner() {
        this.customStrategy = null;
    }

    @Override
    protected List<Strategy<PhysicalPlan>> strategies() {
        List<Strategy<PhysicalPlan>> strategies = Lists.newArrayList(
                new Aggregation(),
                new BasicOperators()
        );
        if (customStrategy != null) {
            strategies.add(customStrategy.get());
        }
        return strategies;
    }
}
