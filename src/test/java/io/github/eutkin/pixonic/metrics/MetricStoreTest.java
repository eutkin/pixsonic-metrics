package io.github.eutkin.pixonic.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Простой тест, проверяющий базовые возможности хранилища (запись и суммирование)
 * в однопоточном режиме.
 */
class MetricStoreTest {

    private Store store;

    @BeforeEach
    void setUp() throws IOException {
        System.setProperty(MetricStore.PIXONIC_METRIC_PARTITIONS_TIMESHIFT, "60");
        this.store = new MetricStore();
    }

    @AfterEach
    void tearDown() throws IOException {
        this.store.close();
    }

    /**
     * В качестве входных данных поступают timestamp, ключ и 1 как значение метрики для упрощения
     * расчетов. Timestamp генерируется в определенном интервале с шагом в 1 и хаотичны.
     * <p>
     * Проверяем сервис на консистетность данных. Сначала добавляем значения метрик,
     * затем считаем сумму по ключу. Сумма метрик в интервале по заданному ключу должна быть равна
     * длине интервала.
     *
     * @param inputs         входные данные. Timestamp, Ключ и 1 как значение
     * @param key            ключ, по которому считаем сумму.
     * @param startTimestamp начало интервала суммирования
     * @param endTimestamp   конец интервала суммирования
     * @param expectedSum    ожидаемая сумма
     */
    @DisplayName("Многопоточно добавляем, затем считаем")
    @ParameterizedTest
    @MethodSource("provider")
    void defaultCase(List<Input> inputs, Character key, Long startTimestamp, Long endTimestamp, Long expectedSum) {
        inputs.forEach(input -> store.add(input.timestamp, input.key, input.value));
        long sum = store.sum(startTimestamp, endTimestamp, key);
        assertEquals(expectedSum, sum);
    }



    private static Stream<Arguments> provider() {
        Set<Character> keySet = new CharGenerator().generate(1);
        Stream<Long> timestampStream = LongStream.range(0L, 130L).boxed();
        List<Input> inputStream = timestampStream
                .flatMap(timestamp -> keySet.stream().map(key -> new Input(timestamp, key, 1)))
                .sorted()
                .collect(Collectors.toList());
        Collections.shuffle(inputStream, ThreadLocalRandom.current());
        return Stream.of(
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 0L, 60L, 60L),
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 10L, 50L, 40L),
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 0L, 90L, 90L),
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 30L, 90L, 60L),
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 0L, 120L, 120L),
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 90L, 120L, 30L),
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 0L, 125L, 125L),
                Arguments.arguments(inputStream, keySet.stream().findAny().get(), 95L, 125L, 30L)
        );
    }

}