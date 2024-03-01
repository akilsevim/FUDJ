import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SetSimilarityJoin implements FlexibleJoin<String, SetSimilarityConfig> {
    Double SimilarityThreshold = 0.0;

    public SetSimilarityJoin(Double SimilarityThreshold) {
        this.SimilarityThreshold = SimilarityThreshold;
    }

    @Override
    public Summary<String> createSummarizer1() {
        return (Summary<String>) new WordCount();
    }

    @Override
    public SetSimilarityConfig divide(Summary<String> s1, Summary<String> s2) {
        WordCount s1wc = (WordCount) s1;
        WordCount s2wc = (WordCount) s2;
        for (String token : s1wc.WordCountMap.keySet()) {
            s2wc.WordCountMap.merge(token, s1wc.WordCountMap.get(token), Integer::sum);
        }

        LinkedHashMap<String, Integer> SortedWordCountMap =
                s2wc.WordCountMap.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        return new SetSimilarityConfig(SortedWordCountMap.keySet().toArray(String[]::new));

    }

    @Override
    public int[] assign1(String k1, SetSimilarityConfig setSimilarityConfig) {
        int startIx = 0;
        int l = k1.length();
        k1 = k1.toLowerCase();

        ArrayList<Integer> ranks = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        int length = 0;
        while (startIx < l) {
            while (startIx < l && isSeparator(k1.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < l && !isSeparator(k1.charAt(startIx))) {

                stringBuilder.append(k1.charAt(startIx));
                startIx++;
            }
            int tokenEnd = startIx;

            String token = stringBuilder.toString();
            if(!token.isEmpty() && setSimilarityConfig.S.containsKey(token)) {
                ranks.add(setSimilarityConfig.S.get(token));
                length++;
                stringBuilder = new StringBuilder();
            }

        }

        int PrefixLength = length == 0?0:(int) (length - Math.ceil(SimilarityThreshold * length) + 1);

        int[] ranksToReturn = new int[PrefixLength];

        Collections.sort(ranks);
        for (int i = 0; i < PrefixLength; i++) {
            ranksToReturn[i] = ranks.get(i);
        }
        return ranksToReturn;
    }

    @Override
    public boolean verify(String k1, String k2) {
        return calculateJaccardSimilarityHashMap(k1, k2) >= SimilarityThreshold;
    }

    public static double calculateJaccardSimilarityHashMap(String left, String right) {

        double intersectionSize = 0;

        int leftLength = left.length();
        int rightLength = right.length();

        int leftTokenC = 0;
        int rightTokenC = 0;

        HashMap<String, Integer> map = new HashMap<>();

        String probe;
        String build;

        if(leftLength<rightLength) {
            build = left.toLowerCase();
            probe = right.toLowerCase();
        } else {
            build = right.toLowerCase();
            probe = left.toLowerCase();
        }

        int startIx = 0;
        int l = build.length();

        StringBuilder stringBuilder = new StringBuilder();
        while (startIx < l) {
            while (startIx < l && isSeparator(build.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < l && !isSeparator(build.charAt(startIx))) {
                stringBuilder.append(build.charAt(startIx));
                startIx++;
            }
            int tokenEnd = startIx;

            String token = stringBuilder.toString();
            if(!token.isEmpty()) {
                map.merge(token, 1, Integer::sum);
                leftTokenC++;
                stringBuilder = new StringBuilder();
            }
        }

        startIx = 0;
        l = probe.length();

        stringBuilder = new StringBuilder();
        while (startIx < l) {
            while (startIx < l && isSeparator(probe.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < l && !isSeparator(probe.charAt(startIx))) {
                stringBuilder.append(probe.charAt(startIx));
                startIx++;
            }
            int tokenEnd = startIx;

            String token = stringBuilder.toString();
            if(!token.isEmpty()) {
                if (map.containsKey(token)) {
                    map.merge(token, -1, Integer::sum);
                    if (map.get(token) == 0) map.remove(token);
                    intersectionSize++;
                }
                rightTokenC++;
                stringBuilder = new StringBuilder();
            }
        }

        double sim = (intersectionSize / ((leftTokenC + rightTokenC) - intersectionSize));
        sim = Math.round(sim * 100000000d) / 100000000d;
        return sim;
    }

    private static boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER
                || Character.getType(c) == Character.OTHER_NUMBER);
    }

}

public class WordCount implements Summary<String> {

    public Map<String, Integer> WordCountMap = new HashMap<>();

    @Override
    public void add(String k) {
        String[] tokens = tokenizer(k);
        for (String token : tokens) {
            WordCountMap.merge(token, 1, Integer::sum);
        }
    }

    @Override
    public void add(Summary<String> s) {
        WordCount wc = (WordCount) s;
        for (String token : wc.WordCountMap.keySet()) {
            WordCountMap.merge(token, wc.WordCountMap.get(token), Integer::sum);
        }
    }

    public String[] tokenizer(String text) {
        ArrayList<String> tokens = new ArrayList<>();
        String lowerCaseText = text.toLowerCase();
        int startIx = 0;

        while (startIx < lowerCaseText.length()) {
            while (startIx < lowerCaseText.length() && isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < lowerCaseText.length() && !isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenEnd = startIx;

            String token = lowerCaseText.substring(tokenStart, tokenEnd);

            if (!token.isEmpty())
                tokens.add(token);

        }
        String[] arr = new String[tokens.size()];
        arr = tokens.toArray(arr);
        return arr;
    }

    private static boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER
                || Character.getType(c) == Character.OTHER_NUMBER);
    }
}

public class SetSimilarityConfig implements Configuration {
    HashMap<String, Integer> S = new HashMap<>();

    SetSimilarityConfig(String[] OrderedTokens) {
        for (int i = 0; i < OrderedTokens.length; i++) {
            this.S.put(OrderedTokens[i], i);
        }
    }
}