package io.indexr.segment.storage.itg;

import java.io.IOException;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.storage.StorageSegment;

public interface Integrate {
    SegmentMeta write(StorageSegment segment, ByteBufferWriter.PredictSizeOpener writerOpener) throws IOException;

    SegmentMeta read(ByteBufferReader reader) throws IOException;

    public static Integrate INSTANCE = IntegrateV1.INSTANCE;

    //public static Integrate INSTANCE = new Integrate() {
    //    @Override
    //    public SegmentMeta write(StorageSegment segment, ByteBufferWriter.PredictSizeOpener writerOpener) throws IOException {
    //        if (segment.version() >= Version.VERSION_7_ID) {
    //            return IntegrateV2.INSTANCE.write(segment, writerOpener);
    //        } else {
    //            return IntegrateV1.INSTANCE.write(segment, writerOpener);
    //        }
    //    }
    //
    //    @Override
    //    public SegmentMeta read(ByteBufferReader reader) throws IOException {
    //        ByteBuffer infoBuffer = null;
    //        Version version = Version.check(reader);
    //        if (version == null) {
    //            return null;
    //        }
    //
    //        long sectionOffset = Version.INDEXR_SEG_FILE_FLAG_SIZE;
    //        SegmentMeta sectionInfo;
    //        if (version.id >= Version.VERSION_7_ID) {
    //            sectionInfo = ObjectSaver.load(reader, sectionOffset, IntegrateV2.SegmentMetaV2::read);
    //        } else {
    //            sectionInfo = ObjectSaver.load(reader, sectionOffset, IntegrateV1.SegmentMetaV1::read);
    //        }
    //        assert sectionInfo.version == version.id;
    //        return sectionInfo;
    //    }
    //};
}
