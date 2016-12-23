package io.indexr.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class RuleExecutor<TreeType extends TreeNode<TreeType>> {
    private static final Logger log = LoggerFactory.getLogger(RuleExecutor.class);

    /**
     * An execution strategy for rules that indicates the maximum number of executions. If the
     * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
     */
    public class Strategy {
        public final int maxIterations;

        public Strategy(int maxIterations) {
            this.maxIterations = maxIterations;
        }
    }

    /** A batch of rules. */
    public class Batch {
        public final String name;
        public final Strategy strategy;
        public final Rule<TreeType>[] rules;

        public Batch(String name, Strategy strategy, Rule<TreeType>... rules) {
            this.name = name;
            this.strategy = strategy;
            this.rules = rules;
        }
    }

    /** Defines a sequence of rule batches, to be overridden by the implementation. */
    protected abstract List<Batch> batches();

    /**
     * Executes the batches of rules defined by the subclass. The batches are executed serially
     * using the defined execution strategy. Within each batch, rules are also executed serially.
     */
    public TreeType execute(TreeType thePlan) {
        TreeType curPlan = thePlan;
        for (Batch batch : batches()) {
            TreeType batchStartPlan = curPlan;
            int iteration = 1;
            TreeType lastPlan = curPlan;
            boolean isContinue = true;

            // Run until fix point (or the max number of iterations as specified in the strategy.
            while (isContinue) {
                for (Rule<TreeType> rule : batch.rules) {
                    TreeType result = rule.apply(curPlan);
                    if (!result.fastEquals(curPlan)) {
                        log.trace("Applying rule {}", rule.ruleName());
                    }
                    curPlan = result;
                }
                iteration += 1;
                if (iteration > batch.strategy.maxIterations) {
                    // Only log if this is a rule that is supposed to run more than once.
                    if (iteration != 2) {
                        log.info("Max iterations ({}) reached for batch {}", iteration - 1, batch.name);
                    }
                    isContinue = false;
                }

                if (curPlan.fastEquals(lastPlan)) {
                    log.trace(
                            "Fixed point reached for batch {} after {} iterations.", batch.name, iteration - 1);
                    isContinue = false;
                }
                lastPlan = curPlan;
            }

            if (!batchStartPlan.fastEquals(curPlan)) {
                // TODO log.trace(...)
            } else {
                log.trace("Batch {} has no effect.", batch.name);
            }
        }
        return curPlan;
    }

}
