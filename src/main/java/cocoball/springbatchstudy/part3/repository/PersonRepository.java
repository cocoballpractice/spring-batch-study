package cocoball.springbatchstudy.part3.repository;

import cocoball.springbatchstudy.part3.Person;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PersonRepository extends JpaRepository<Person, Integer> {
}
