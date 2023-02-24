package cocoball.springbatchstudy.part5;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.Collection;

public interface UserRepostiory extends JpaRepository<User, Long> {

    Collection<User> findAllByUpdatedDate(LocalDate localDate);

}
