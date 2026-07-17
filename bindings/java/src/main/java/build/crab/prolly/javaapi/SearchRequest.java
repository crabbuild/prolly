package build.crab.prolly.javaapi;

public record SearchRequest(float[] vector, int topK) {
    public SearchRequest {
        vector = vector.clone();
        if (vector.length == 0 || topK <= 0) throw new IllegalArgumentException("invalid search request");
    }
    public static SearchRequest exact(float[] vector, int topK) { return new SearchRequest(vector, topK); }
}
