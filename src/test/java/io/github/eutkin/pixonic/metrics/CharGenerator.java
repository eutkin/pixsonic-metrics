package io.github.eutkin.pixonic.metrics;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CharGenerator {

    private final String abc;

    public CharGenerator() {
        this.abc = "abcdefghijklmnopqrstuvwxyz";
    }

    public CharGenerator(String alphabet) {
        this.abc = alphabet;
    }

    public Character generate() {
        return generate(1).iterator().next();
    }

    public Set<Character> generate(int power) {
        if (power < 0 || power > abc.length()) {
            throw new IllegalArgumentException("power must be above 1");
        }
        List<Character> chars = abc.chars().mapToObj(c -> (char) c).collect(Collectors.toList());
        return Stream.generate(() -> randomChar(chars)).limit(power).collect(Collectors.toSet());
    }

    private Character randomChar(List<Character> queue) {
        int index = ThreadLocalRandom.current().nextInt(0, queue.size());
        return queue.remove(index);
    }
}
