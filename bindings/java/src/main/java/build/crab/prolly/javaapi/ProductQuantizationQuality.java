package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaProductQuantizationQuality;

public record ProductQuantizationQuality(double meanSquaredError, double maximumSquaredError) {
    static ProductQuantizationQuality fromNative(JavaProductQuantizationQuality value) {
        return new ProductQuantizationQuality(
                value.getMeanSquaredError(), value.getMaximumSquaredError());
    }
}
