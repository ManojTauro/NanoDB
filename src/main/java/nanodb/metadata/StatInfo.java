package nanodb.metadata;

import lombok.Getter;

@Getter
public class StatInfo {
    private final int numBlocks;
    private final int numRecords;

    public StatInfo(int numBlocks, int numRecords) {
        this.numBlocks = numBlocks;
        this.numRecords = numRecords;
    }

    public int distinctValues(String fieldName) {
        return 1 + (numRecords / 3 ); //dummy
    }
}
