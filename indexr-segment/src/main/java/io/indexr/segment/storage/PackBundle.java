package io.indexr.segment.storage;

import io.indexr.segment.PackExtIndex;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;

public class PackBundle {
    public final DataPack dataPack;
    public final DataPackNode dpn;
    public final PackRSIndex rsIndex;
    public final PackExtIndex extIndex;

    public PackBundle(DataPack dataPack,
                      DataPackNode dpn,
                      PackRSIndex rsIndex,
                      PackExtIndex extIndex) {
        this.dataPack = dataPack;
        this.dpn = dpn;
        this.rsIndex = rsIndex;
        this.extIndex = extIndex;
    }
}
