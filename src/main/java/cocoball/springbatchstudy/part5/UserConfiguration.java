package cocoball.springbatchstudy.part5;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class UserConfiguration {

    private final int CHUNK_SIZE = 100;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepostiory userRepostiory;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;


    public UserConfiguration(JobBuilderFactory jobBuilderFactory,
                             StepBuilderFactory stepBuilderFactory,
                             UserRepostiory userRepostiory,
                             EntityManagerFactory entityManagerFactory,
                             DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.userRepostiory = userRepostiory;
        this.entityManagerFactory = entityManagerFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job userJob() throws Exception {
        return this.jobBuilderFactory.get("userJob")
                .incrementer(new RunIdIncrementer())
                .start(this.saveUserStep()) // ?????? ?????? step
                .next(this.userLevelUpStep()) // ?????? ????????? step
                .next(this.orderStatisticsStep(null)) // ?????? ?????? ?????? ?????? step
                .listener(new LevelUpJobExecutionListener(userRepostiory)) // JobExecutionListener
                .next(new JobParametersDecide("date")) // date ??????????????? ????????? ??????
                    .on(JobParametersDecide.CONTINUE.getName()) // Decider??? CONTINUE??? ??????????????? (CONTINUE ????????? ????????? ????????? ??????)
                    .to(this.orderStatisticsStep(null)) // ?????? ????????? ??????
                    .build()
                .build();
    }

    @Bean
    public Step saveUserStep() {
        return this.stepBuilderFactory.get("saveUserStep")
                .tasklet(new SaveUserTasklet(userRepostiory)) // ?????? ?????? tasklet
                .build();
    }

    @Bean
    public Step userLevelUpStep() throws Exception {
        return this.stepBuilderFactory.get("userLevelUpStep")
                .<User, User>chunk(CHUNK_SIZE)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    @JobScope
    public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
        return this.stepBuilderFactory.get("orderStatisticsStep")
                .<OrderStatistics, OrderStatistics>chunk(CHUNK_SIZE)
                .reader(orderStatisticsItemReader(date))
                .writer(orderStatisticsItemWriter(date))
                .build();
    }


    private ItemReader<? extends User> itemReader() throws Exception {

        JpaPagingItemReader itemReader = new JpaPagingItemReaderBuilder<User>()
                .queryString("select u from User u")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(CHUNK_SIZE) // ?????? chunk ???????????? ????????????
                .name("userItemReader")
                .build();

        itemReader.afterPropertiesSet();

        return itemReader;

    }


    private ItemProcessor<? super User, ? extends User> itemProcessor() {
        return user -> {

            // ?????? ?????? ?????? ??????
            if (user.availableLevelUp()) {
                return user;
            }

            return null;

        };
    }


    private ItemWriter<? super User> itemWriter() {
        return users -> {

            users.forEach(x -> {
                x.levelUp();
                userRepostiory.save(x);
            });

        };
    }

    private ItemReader<? extends OrderStatistics> orderStatisticsItemReader(String date) throws Exception {

        YearMonth yearMonth = YearMonth.parse(date); // date??? ???, ??? parse

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("startDate", yearMonth.atDay(1)); // ?????? ?????? 1???
        parameters.put("endDate", yearMonth.atEndOfMonth()); // ?????? ?????? ??????

        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("created_date", Order.ASCENDING); // sort ?????? ??????

        JdbcPagingItemReader itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
                .dataSource(dataSource)
                .rowMapper((resultSet, i) -> OrderStatistics.builder()
                        .amount(resultSet.getString(1))
                        .date(LocalDate.parse(resultSet.getString(2), DateTimeFormatter.ISO_DATE))
                        .build())
                .pageSize(CHUNK_SIZE)
                .name("orderStatisticsItemReader")
                .selectClause("sum(amount), created_date") // select???
                .fromClause("orders") // from???
                .whereClause("created_date >= :startDate and created_date <= :endDate") // where???
                .groupClause("created_date") // group???
                .parameterValues(parameters) // ???????????? ??????
                .sortKeys(sortKey) // ??????
                .build();

        itemReader.afterPropertiesSet();

        return itemReader;
    }

    private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {

        YearMonth yearMonth = YearMonth.parse(date); // date??? ???, ??? parse

        String fileName = yearMonth.getYear() + "???_" + yearMonth.getMonthValue() + "???_??????_??????_??????.csv";

        BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"amount", "date"});

        DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
                .resource(new FileSystemResource("output/" + fileName))
                .lineAggregator(lineAggregator)
                .name("orderStatisticsItemWriter")
                .encoding("UTF-8")
                .headerCallback(writer -> writer.write("total_amount, date"))
                .build();

        itemWriter.afterPropertiesSet();

        return itemWriter;

    }


}
