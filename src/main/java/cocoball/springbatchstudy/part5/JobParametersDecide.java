package cocoball.springbatchstudy.part5;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.util.StringUtils;

public class JobParametersDecide implements JobExecutionDecider {

    public static final FlowExecutionStatus CONTINUE = new FlowExecutionStatus("CONTINUE");

    private final String key;

    public JobParametersDecide(String key) {
        this.key = key;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

        String value = jobExecution.getJobParameters().getString(key);

        if (StringUtils.isEmpty(value)) {
            return FlowExecutionStatus.COMPLETED; // key에 해당하는 값이 없으면 COMPLETED
        }

        return CONTINUE;
    }
}
