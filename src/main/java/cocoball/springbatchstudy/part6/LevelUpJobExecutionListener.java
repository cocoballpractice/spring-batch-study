package cocoball.springbatchstudy.part6;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.time.LocalDate;
import java.util.Collection;

@Slf4j
public class LevelUpJobExecutionListener implements JobExecutionListener {

    private final UserRepostiory userRepostiory;

    public LevelUpJobExecutionListener(UserRepostiory userRepostiory) {
        this.userRepostiory = userRepostiory;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {}

    @Override
    public void afterJob(JobExecution jobExecution) {
        Collection<User> users = userRepostiory.findAllByUpdatedDate(LocalDate.now());

        // job 수행 시간
        long time = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();

        log.info("회원 등급 업데이트 배치 시작");
        log.info("총 데이터 처리 {}건, 처리 시간 {}millis", users.size(), time);
    }
}
