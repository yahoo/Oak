package oak;

// this enum class describes the operation the thread is progressing with. The enum is also used
// for helping in rebalance time
public enum Operation {
    NO_OP,
    PUT,
    PUT_IF_ABSENT,
    REMOVE,
    COMPUTE,
    PUT_IF_ABS_COMPUTE_IF_PRES
}
