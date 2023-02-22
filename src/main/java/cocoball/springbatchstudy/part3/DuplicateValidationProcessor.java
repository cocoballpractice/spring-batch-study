package cocoball.springbatchstudy.part3;

import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DuplicateValidationProcessor<T> implements ItemProcessor<T, T> {

    private final Map<String, Object> keyPool = new ConcurrentHashMap<>();
    private final Function<T, String> keyExtractor;
    private final boolean allowDuplicate;

    public DuplicateValidationProcessor(Function<T, String> keyExtractor, boolean allowDuplicate) {
        this.keyExtractor = keyExtractor;
        this.allowDuplicate = allowDuplicate;
    }


    @Override
    public T process(T item) throws Exception {

        if (allowDuplicate) {
            return item; // 필터링을 하지 않는 경우
        }

        String key = keyExtractor.apply(item); // 아이템에서 키를 추출

        if (keyPool.containsKey(key)) {
            return null; // 키가 키 풀에 존재하는 키이면 중복값이므로 null 리턴
        }

        keyPool.put(key, key); // 키 풀에 키를 저장 (차후 중복 체크를 위해)

        return item;
    }
}
