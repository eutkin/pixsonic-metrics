package io.github.eutkin.pixonic.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Тест для моделирования боевой нагрузки.
 * <p>
 * Проверяет, что сервис отрабатывает без ошибок в многопоточной среде
 * (два конкурентных потока добавляют метрики и два потока суммируют).
 */
class ConcurrentTest {

    private static final int MAX_TIMESTAMP = 1_500_000;
    private Store store;

    /**
     * Инициализируем хранилище и выставляем размер партиции в миллисекундах.
     *
     * @throws IOException если нет разрешения создавать папки и файлы во временном хранилище.
     */
    @BeforeEach
    void setUp() throws IOException {
        System.setProperty(MetricStore.PIXONIC_METRIC_PARTITIONS_TIMESHIFT, "50000");
        this.store = new MetricStore();
    }

    @AfterEach
    void tearDown() throws IOException {
        this.store.close();
    }

    @DisplayName("Многопоточно добавляем, одновременно многопоточно считаем")
    @ParameterizedTest
    @MethodSource("providerGatling")
    void parallelCase(List<Input> inputsA, List<Input> inputsB) throws Exception {
        CompletableFuture<Void> addFutureA = runAdd(inputsA);
        CompletableFuture<Void> addFutureB = runAdd(inputsB);

        ThreadLocalRandom random = ThreadLocalRandom.current();


        CompletableFuture<Double> sumFutureA = runSum(inputsA, random, 'a');
        CompletableFuture<Double> sumFutureB = runSum(inputsB, random, 'b');

        CompletableFuture.allOf(addFutureA, sumFutureA, addFutureB, sumFutureB).get();
    }

    private CompletableFuture<Double> runSum(List<Input> inputs, ThreadLocalRandom random, char a) {
        return CompletableFuture.supplyAsync(() ->
                IntStream.range(0, inputs.size() / 1000)
                        .mapToLong(ignore -> {
                            sleep();
                            long startTimestampInclusive = random.nextLong(0, MAX_TIMESTAMP / 4);
                            long endTimestampExclusive = random.nextLong(
                                    random.nextLong(MAX_TIMESTAMP / 4, MAX_TIMESTAMP / 2),
                                    random.nextLong(MAX_TIMESTAMP / 2, MAX_TIMESTAMP)
                            );
                            return store.sum(startTimestampInclusive, endTimestampExclusive, a);
                        }).average().orElse(0.0));
    }

    private CompletableFuture<Void> runAdd(List<Input> inputsA) {
        return CompletableFuture.runAsync(() -> inputsA.stream()
                .forEachOrdered(input -> {
                    sleep();
                    store.add(input.timestamp, input.key, input.value);
                }));
    }

    private static Stream<Arguments> providerGatling() {
        List<Input> inputsA = getInputs('a');
        List<Input> inputsB = getInputs('b');

        return Stream.of(
                Arguments.arguments(inputsA, inputsB)
        );
    }

    private static List<Input> getInputs(Character key) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<Long> timestamps = new ArrayList<>();
        /* генерируем временную последовательность (timestamp)
           чтобы смоделировать ситуацию с отставанием метрик по времени,
           в рандом добавим отрицательную границу.
        */
        for (long i = 3; i < MAX_TIMESTAMP; i += random.nextInt(-20, 50)) {
            timestamps.add(i);
        }
        List<Input> inputs = timestamps
                .stream()
                .map(timestamp -> new Input(timestamp, key, 1))
                .collect(Collectors.toList());
        for (int i = 0; i < inputs.size() / 100; i++) {
            int index1 = random.nextInt(200, inputs.size() - 200);
            int index2 = index1 + random.nextInt(-100, 100);
            Input input = inputs.get(index1);
            Input buffer = inputs.get(index2);
            inputs.set(index2, input);
            inputs.set(index1, buffer);
        }
        return inputs;
    }

    private void sleep() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try {
            TimeUnit.MICROSECONDS.sleep(random.nextLong(10, 500));
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
