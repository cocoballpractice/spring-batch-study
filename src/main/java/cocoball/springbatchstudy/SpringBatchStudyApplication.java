package cocoball.springbatchstudy;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
@EnableBatchProcessing // Batch Processing을 하겠음을 선언
public class SpringBatchStudyApplication {

    public static void main(String[] args) {

        // Async 적용 시 가끔씩 어플리케이션이 종료되지 않는 문제가 있어 추가
        // 배치 종료 시 안전하게 어플리케이션 종료
        System.exit(SpringApplication.exit(SpringApplication.run(SpringBatchStudyApplication.class, args)));

    }

    @Bean
    @Primary // 이미 빈이 설정되어 있어서 해당 빈을 우선으로 사용하게끔 설정
    TaskExecutor taskExecutor() {

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(10);
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setThreadNamePrefix("batch_thread-"); // 로그에 찍힘
        taskExecutor.initialize();

        return taskExecutor;
    }
}
