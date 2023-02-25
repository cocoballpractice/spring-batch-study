package cocoball.springbatchstudy.part6;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class UserLevelUpPartitioner implements Partitioner {

    private final UserRepostiory userRepostiory;

    public UserLevelUpPartitioner(UserRepostiory userRepostiory) {
        this.userRepostiory = userRepostiory;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        // gridSize = Slave Step의 size
        long minId = userRepostiory.findMinId(); // 가장 작은 id값, 1
        long maxId = userRepostiory.findMaxId(); // 가장 큰 id값, 40,000

        // 각 Slave Step에서 처리해야 할 Size
        long targetSize = (maxId - minId) / gridSize + 1; // 5,000

        /**
         * partition0 : 1, 5000
         * partition1 : 5001, 1000
         * ...
         * partition7 : 35001, 40000
         */
        Map<String, ExecutionContext> result = new HashMap<>();

        long number = 0; // step의 번호

        long start = minId;
        long end = start + targetSize - 1;

        while(start <= maxId) {
            ExecutionContext value = new ExecutionContext();

            result.put("partition" + number, value);

            if (end >= maxId) {
                end = maxId;
            }

            value.putLong("minId", start);
            value.putLong("maxId", end);

            start += targetSize;
            end += targetSize;
            number++;
        }

        return result;
    }
}
