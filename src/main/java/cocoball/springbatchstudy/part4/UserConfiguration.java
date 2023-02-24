package cocoball.springbatchstudy.part4;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Configuration
@Slf4j
public class UserConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepostiory userRepostiory;
    private final EntityManagerFactory entityManagerFactory;


    public UserConfiguration(JobBuilderFactory jobBuilderFactory,
                             StepBuilderFactory stepBuilderFactory,
                             UserRepostiory userRepostiory,
                             EntityManagerFactory entityManagerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.userRepostiory = userRepostiory;
        this.entityManagerFactory = entityManagerFactory;
    }

    @Bean
    public Job userJob() throws Exception {
        return this.jobBuilderFactory.get("userJob")
                .incrementer(new RunIdIncrementer())
                .start(this.saveUserStep()) // 유저 저장 step
                .next(this.userLevelUpStep()) // 유저 레벨업 step
                .listener(new LevelUpJobExecutionListener(userRepostiory)) // JobExecutionListener
                .build();
    }

    @Bean
    public Step saveUserStep() {
        return this.stepBuilderFactory.get("saveUserStep")
                .tasklet(new SaveUserTasklet(userRepostiory)) // 유저 저장 tasklet
                .build();
    }

    @Bean
    public Step userLevelUpStep() throws Exception {
        return this.stepBuilderFactory.get("userLevelUpStep")
                .<User, User>chunk(100)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }


    private ItemReader<? extends User> itemReader() throws Exception {

        JpaPagingItemReader itemReader = new JpaPagingItemReaderBuilder<User>()
                .queryString("select u from User u")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(100) // 보통 chunk 사이즈와 동일하게
                .name("userItemReader")
                .build();

        itemReader.afterPropertiesSet();

        return itemReader;

    }


    private ItemProcessor<? super User, ? extends User> itemProcessor() {
        return user -> {

            // 상향 대상 여부 체크
            if (user.availableLevelUp()) {
                return user;
            }

            return null;

        };
    }


    private ItemWriter<? super User> itemWriter() {
        return users -> {

            users.forEach(x-> {
                x.levelUp();
                userRepostiory.save(x);
            });

        };
    }


}
