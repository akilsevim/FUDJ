import java.io.Serializable;

public interface FlexibleJoin<T, C> {
    Summary<T> createSummarizer1();

    default Summary<T> createSummarizer2() {
        return this.createSummarizer1();
    }

    C divide(Summary<T> var1, Summary<T> var2);

    int[] assign1(T var1, C var2);

    default int[] assign2(T k2, C c) {
        return this.assign1(k2, c);
    }

    default boolean match(int b1, int b2) {
        return b1 == b2;
    }

    default boolean verify(int b1, T k1, int b2, T k2, C c) {
        if (this.verify(k1, k2)) {
            int[] buckets1DA = this.assign1(k1, c);
            int[] buckets2DA = this.assign2(k2, c);
            int i = 0;
            int j = 0;

            while(i < buckets1DA.length && j < buckets2DA.length) {
                if (buckets1DA[i] == buckets2DA[j]) {
                    return buckets1DA[i] == b1 && buckets2DA[j] == b2;
                }

                if (buckets1DA[i] > buckets2DA[j]) {
                    ++j;
                } else {
                    ++i;
                }
            }
        }

        return false;
    }

    boolean verify(T var1, T var2);
}

public interface Configuration extends Serializable {
}

public interface Summary<T> extends Serializable {
    void add(T var1);

    void add(Summary<T> var1);
}