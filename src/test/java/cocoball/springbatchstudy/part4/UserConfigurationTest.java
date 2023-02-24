package cocoball.springbatchstudy.part4;

import cocoball.springbatchstudy.part3.TestConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;

import static org.junit.Assert.*;

@SpringBatchTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {UserConfiguration.class, TestConfiguration.class})
public class UserConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private UserRepostiory userRepostiory;

    @Test
    public void test() throws Exception {

        // Given
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        int size = userRepostiory.findAllByUpdatedDate(LocalDate.now()).size();
        // 사실 현업에서 쓰기에 적절한 검증은 아님. 배치 수행 시간, 시작 / 종료 시간 등에 따라 검증 대상에서 제외되는 경우도 있으므로

        Assertions.assertThat(jobExecution.getStepExecutions().stream()
                .filter(x -> x.getStepName().equals("userLevelUpStep"))
                .mapToInt(StepExecution::getWriteCount)
                .sum())
                .isEqualTo(size)
                .isEqualTo(300);

        Assertions.assertThat(userRepostiory.count())
                .isEqualTo(400);

    }

}