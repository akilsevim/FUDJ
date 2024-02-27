public class IntervalJoin implements FlexibleJoin<long[], IntervalJoinConfig> {
    private long k = 0;

    public IntervalJoin(long k) {
        this.k = k;
    }

    @Override
    public Summary<long[]> createSummarizer1() {
        return new IntervalSummary();
    }

    @Override
    public IntervalJoinConfig divide(Summary<long[]> s1, Summary<long[]> s2) {

        IntervalSummary iS1 = (IntervalSummary) s1;
        IntervalSummary iS2 = (IntervalSummary) s2;

        iS1.add(iS2);

        double d1 = (double) (iS1.oEnd - iS1.oStart) / k;
        double d2 = (double) (iS1.oEnd - iS1.oStart) / k;

        return new IntervalJoinConfig(d1, d2, iS1, iS2, k);
    }

    @Override
    public int[] assign1(long[] k1, IntervalJoinConfig intervalJoinConfig) {

        short i = (short) ((k1[0] - intervalJoinConfig.iS1.oStart) / intervalJoinConfig.d1);
        short j = (short) (Math.ceil((k1[1] - intervalJoinConfig.iS1.oStart) / intervalJoinConfig.d1) - 1);

        int bucketId = (i << 16) | (j & 0xFFFF);
        return new int[] {bucketId};
    }

    @Override
    public boolean match(int b1, int b2) {
        short b1Start = (short) (b1 >> 16);
        short b1End = (short) b1;

        short b2Start = (short) (b2 >> 16);
        short b2End = (short) b2;

        return (b1Start <= b2End && b1End >= b2Start);
    }

    @Override
    public boolean verify(int b1, long[] k1, int b2, long[] k2, IntervalJoinConfig c) {
        return verify(k1, k2);
    }

    @Override
    public boolean verify(long[] k1, long[] k2) {
        return k1[0] < k2[1] && k1[1] > k2[0];
    }
}

class IntervalJoinConfig implements Configuration {
    public IntervalSummary iS1;
    public IntervalSummary iS2;
    long k;
    double d1;
    double d2;

    IntervalJoinConfig(double d1, double d2, IntervalSummary iS1, IntervalSummary iS2, long k) {
        this.iS1 = iS1;
        this.iS2 = iS2;
        this.d1 = d1;
        this.d2 = d2;
        this.k = k;
    }
}

class IntervalSummary implements Summary<long[]> {
    public long oStart = Long.MAX_VALUE;
    public long oEnd = Long.MIN_VALUE;

    @Override
    public void add(long[] k) {
        if (k[0] < this.oStart)
            this.oStart = k[0];
        if (k[1] > this.oEnd)
            this.oEnd = k[1];
    }

    @Override
    public void add(Summary<long[]> s) {
        IntervalSummary iS = (IntervalSummary) s;
        if (iS.oStart < this.oStart)
            this.oStart = iS.oStart;
        if (iS.oEnd > this.oEnd)
            this.oEnd = iS.oEnd;
    }
}
