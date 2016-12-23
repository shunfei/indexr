package io.indexr.segment.rt;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TagSetting {
    @JsonProperty("tag.field")
    public final String tagField;
    @JsonProperty("accept.tags")
    public final List<String> acceptTags;
    @JsonProperty("accept.none")
    public final boolean acceptNone;

    public TagSetting(@JsonProperty("tag.field") String tagField,
                      @JsonProperty("accept.tags") List<String> acceptTags,
                      @JsonProperty("accept.none") boolean acceptNone) {
        this.tagField = tagField;
        this.acceptTags = acceptTags;
        this.acceptNone = acceptNone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TagSetting that = (TagSetting) o;

        if (acceptNone != that.acceptNone) return false;
        if (tagField != null ? !tagField.equals(that.tagField) : that.tagField != null)
            return false;
        return acceptTags != null ? acceptTags.equals(that.acceptTags) : that.acceptTags == null;
    }
}
