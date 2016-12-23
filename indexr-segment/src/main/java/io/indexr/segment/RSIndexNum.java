package io.indexr.segment;

public interface RSIndexNum extends RSIndex {
    byte isValue(int packId, long minVal, long maxVal, long packMin, long packMax);

    public static byte minMaxCheck(long minVal, long maxVal,
                                   long packMin, long packMax,
                                   boolean isFloat) {
        if (isFloat) {
            double dminVal = Double.longBitsToDouble(minVal);
            double dmaxVal = Double.longBitsToDouble(maxVal);
            double dpackMin = Double.longBitsToDouble(packMin);
            double dpackMax = Double.longBitsToDouble(packMax);
            assert dminVal <= dmaxVal;
            assert dpackMin <= dpackMax;
            if (dmaxVal < dpackMin || dminVal > dpackMax) {
                return RSValue.None;
            }
            if (dmaxVal >= dpackMax && dminVal <= dpackMin) {
                return RSValue.All;
            }
            if (dmaxVal >= dpackMax || dminVal <= dpackMin) {
                return RSValue.Some;
            }
        } else {
            if (maxVal < packMin || minVal > packMax) {
                return RSValue.None;
            }
            if (maxVal >= packMax && minVal <= packMin) {
                return RSValue.All;
            }
            if (maxVal >= packMax || minVal <= packMin) {
                return RSValue.Some;
            }
        }
        return RSValue.Some;
    }
}
