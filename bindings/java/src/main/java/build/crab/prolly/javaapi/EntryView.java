package build.crab.prolly.javaapi;

/** Callback-scoped key/value views backed by one native packed page. */
public record EntryView(ScopedBytes key, ScopedBytes value) {
    static EntryView fromNative(build.crab.prolly.api.EntryView value) {
        return new EntryView(new ScopedBytes(value.getKey()), new ScopedBytes(value.getValue()));
    }
}
