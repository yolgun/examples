package com.bytro.firefly.stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yunus on 28.11.16.
 */
public class AwardContainer implements Iterable<AwardChecker>{
    private List<AwardChecker> checkerList = new ArrayList<>();
    private static final int SCORE_COUNT = 10;

    public AwardContainer() {
        for (int i = 0; i < SCORE_COUNT; i++) {
            for (int j = 0; j < 3; j++) {
                checkerList.add(new AwardChecker(j + i * 3, "score-" + i, (j + 1) * 30));
            }
        }
    }


    @Override
    public Iterator<AwardChecker> iterator() {
        return checkerList.iterator();
    }
}
