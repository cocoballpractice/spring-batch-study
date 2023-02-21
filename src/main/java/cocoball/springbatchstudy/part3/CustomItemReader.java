package cocoball.springbatchstudy.part3;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.ArrayList;
import java.util.List;

public class CustomItemReader<T> implements ItemReader<T> {

    // List를 Reader로 처리

    private final List<T> items;

    public CustomItemReader(List<T> items) {
        this.items = new ArrayList<>(items);
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!items.isEmpty()) {
            return items.remove(0); // 0번째 요소를 return하면서 동시에 items에서 제거
        }

        return null; // read()에서 null 리턴 시 ItemReader의 동작 끝 -> chunk 동작 끝
    }
}
