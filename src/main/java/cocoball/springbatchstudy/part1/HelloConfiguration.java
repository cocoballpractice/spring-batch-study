package cocoball.springbatchstudy.part1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class HelloConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public HelloConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    // Job은 실행 단위, 1개 이상의 Step을 가질 수 있음
    @Bean
    public Job helloJob() {
        return jobBuilderFactory.get("helloJob") // job의 이름, Batch를 실행하기 위한 키이기도 함
                .incrementer(new RunIdIncrementer()) // RunIdIncrementer : Job이 실행할 때마다 parameter ID를 자동으로 생성
                                                    // -> Job이 실행될 때 마다 새 JobInstance가 생성됨
                .start(this.helloStep()) // job 최초 실행 시 수행할 동작
                .build();
    }

    // Job의 실행 단위
    @Bean
    public Step helloStep() {
        return stepBuilderFactory.get("helloJob")
                .tasklet((contribution, chunkContext) -> {
                    log.info("hello spring batch");
                    return RepeatStatus.FINISHED;
                }).build();
    }

}
