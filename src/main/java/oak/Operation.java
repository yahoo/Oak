package oak;

// this enum class describes the operation the thread is progressing with and is also used for
// helping in rebalance time
public enum Operation {
    NO_OP,
    PUT,
    PUT_IF_ABSENT,
    REMOVE,
    COMPUTE
}
