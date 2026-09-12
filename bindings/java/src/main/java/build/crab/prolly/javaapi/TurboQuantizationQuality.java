package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaTurboQuantizationQuality;

public record TurboQuantizationQuality(double meanSquaredError, double maximumSquaredError) {
    static TurboQuantizationQuality fromNative(JavaTurboQuantizationQuality value) {
        return new TurboQuantizationQuality(
                value.getMeanSquaredError(), value.getMaximumSquaredError());
    }
}
