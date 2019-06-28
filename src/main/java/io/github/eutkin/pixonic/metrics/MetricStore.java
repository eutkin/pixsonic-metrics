package io.github.eutkin.pixonic.metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static java.lang.Integer.parseInt;
import static java.nio.file.StandardOpenOption.*;

/**
 * Хранилище метрик
 */
public class MetricStore implements Store {

    private final static Logger log = Logger.getLogger(MetricStore.class.getName());

    /**
     * Имя свойства для настройки размера партиций. Определяет, какой промежуток в миллисекундах для одного
     * ключа будет храниться в одном файле.
     */
    public static final String PIXONIC_METRIC_PARTITIONS_TIMESHIFT = "pixonic.metric.partition.timeshift";

    /**
     * Значение свойства {@link MetricStore#PIXONIC_METRIC_PARTITIONS_TIMESHIFT}.
     */
    private final int timeshift;

    /**
     * Длина буффера одной записи в байтах. Так как мы знаем схему, то можно заранее вычислить длину буффера.
     */
    private static final int LENGTH = Long.BYTES + Integer.BYTES;

    /**
     * Кэш каналов для записи в файлы.
     */
    private final Map<Partition, ByteChannel> channels;

    /**
     * Кэш, который хранит ранее записанные партиции. Нужен, чтобы очищать {@link this#channels},
     * чтобы не держать в памяти кучу незакрытых I/O каналов. Если надо записать метрики с устаревшим timestamp,
     * то будет временно открыт новый I/O канал. После записи он будет закрыт, так как вряд ли нам придет еще запись
     * с устравшим timestamp.
     */
    private final Map<Character, Partition> oldPartitions;

    /**
     * Директория хранилища метрик.
     */
    private final Path root;

    /**
     * Создает хранилище.
     *
     * @param dataStoreDir директория для хранения файлов с метриками.
     * @throws IOException ошибка при работает с I/О.
     */
    public MetricStore(Path dataStoreDir) throws IOException {
        this.channels = new ConcurrentHashMap<>();
        this.oldPartitions = new ConcurrentHashMap<>();
        this.timeshift = parseInt(System.getProperty(PIXONIC_METRIC_PARTITIONS_TIMESHIFT, "60000"));
        this.root = dataStoreDir;
        if (!Files.exists(dataStoreDir)) {
            Files.createDirectory(dataStoreDir);
        }
    }

    /**
     * Создает хранилище в во временной директории
     *
     * @throws IOException ошибка при работает с I/О.
     */
    public MetricStore() throws IOException {
        this(Files.createTempDirectory("metrics"));
    }


    /**
     * Добавляет метрику в хранилище.
     *
     * @param timestamp время
     * @param key       имя метрики
     * @param value     значение метрики
     */
    public void add(long timestamp, char key, int value) {
        Partition partition = new Partition(key, timestamp);
        Partition cachedPartition = oldPartitions.get(key);
        if (cachedPartition != null && partition.timestamp - cachedPartition.timestamp >= 2* timeshift) {
            ByteChannel channel = channels.remove(cachedPartition);
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                log.severe(e.getMessage());
            }
        }
        oldPartitions.put(key, partition);
        ByteChannel channel = channels.computeIfAbsent(partition, this::getByteChannel);
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH)
                .putLong(timestamp).putInt(value).flip();
        try {
            channel.write(buffer);
        } catch (IOException e) {
            log.severe(e.getMessage());
        }
    }

    private ByteChannel getByteChannel(Partition partition) {
        try {
            Path path = root.resolve(partition.toString());
            log.info("Open I/O channel for file: " + path);
            return Files.newByteChannel(path, CREATE, WRITE, APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    /**
     * Суммирует значения метрики за временный интервал.
     *
     * @param startTimestampInclusive начало интервала
     * @param endTimestampExclusive   конец интервала
     * @param key                     имя метрики
     * @return сумму значений метрики за интервал
     * @throws UncheckedIOException ошибки в работе I/O
     */
    public long sum(long startTimestampInclusive, long endTimestampExclusive, char key) {
        try {
            List<Path> paths = new ArrayList<>();
            long startPartition = (startTimestampInclusive / timeshift) * timeshift;
            long endPartition = (endTimestampExclusive / timeshift) * timeshift;
            for (long i = startPartition; i <= endPartition; i += timeshift) {
                Path storeFile = root.resolve(key + "-" + i);
                if (Files.exists(storeFile)) {
                    paths.add(storeFile);
                }
            }
            long sum = 0;
            for (Path path : paths) {
                SeekableByteChannel channel = Files.newByteChannel(path, READ);
                try (channel) {
                    sum += sum(channel, startTimestampInclusive, endTimestampExclusive);
                }
            }
            return sum;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex.getMessage(), ex);
        }
    }

    /**
     * Читает последовательно файл. Берет только те записи, которые попадают внутрь заданного интервала.
     *
     * <p> Оптимально расходует память, так как для чтения каждой строки буффер переиспользуется.
     *
     * @param channel                 I/O канал
     * @param startTimestampInclusive начало интервала
     * @param endTimestampExclusive   конец интервала
     * @return сумму для одного файла
     * @throws IOException ошибка при работе с I/O
     */
    private long sum(SeekableByteChannel channel, long startTimestampInclusive, long endTimestampExclusive) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH);
        long sum = 0;
        for (int pos = 0; pos < channel.size(); pos += LENGTH) {
            channel = channel.position(pos);
            int readed = channel.read(buffer);
            if (readed == -1) {
                break;
            }
            buffer.flip();
            long timestamp = buffer.getLong();
            int value = buffer.getInt();
            if (timestamp >= startTimestampInclusive && timestamp < endTimestampExclusive) {
                sum += value;
            }
            buffer.clear();
        }
        return sum;
    }

    /**
     * Закрывает I/O каналы из кэша.
     */
    @Override
    public void close() {
        for (ByteChannel value : channels.values()) {
            try (value) {
                log.info("Success close byte channel");
            } catch (IOException e) {
                log.severe(e.getMessage());
            }
        }
    }

    /**
     * Одна партиция данных.
     */
    private class Partition implements Comparable<Partition> {
        private final char key;
        private final long timestamp;

        Partition(char key, long timestamp) {
            this.key = key;
            this.timestamp = (timestamp / timeshift) * timeshift;
        }

        @Override
        public String toString() {
            return key + "-" + timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Partition partition = (Partition) o;
            return key == partition.key &&
                    partition.timestamp / timeshift == timestamp / timeshift;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, timestamp / timeshift);
        }

        @Override
        public int compareTo(Partition o) {
            return Long.compare(timestamp, o.timestamp);
        }
    }

}
