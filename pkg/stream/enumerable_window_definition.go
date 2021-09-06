package stream

//
// The window specification for windows that can be enumerated for a single event based on its time.
//
// Grace period defines how long to wait on out-of-order events. That is, windows will continue to accept new records until {@code stream_time >= window_end + grace_period}.
// Records that arrive after the grace period passed are considered <em>late</em> and will not be processed but are dropped.
//
// Window state is stored until it is no longer needed, measured from the beginning of the window. To assist the store in not discarding window state too soon, this definition
// also includes a maxSize(), indicating the longest span windows can have between their start and end. This does not need to include the grace period.
//
type EnumerableWindowDefinition interface {
	/**
	 * List all possible windows that contain the provided timestamp,
	 * indexed by non-negative window start timestamps.
	 *
	 * @param timestamp the timestamp window should get created for
	 * @return a map of windowStartTimestamp -> Window entries
	 */
	WindowsFor(timestamp uint64) (map[uint64]Window, error)
	/**
	 * Return an upper bound on the size of windows in milliseconds.
	 * Used to determine the lower bound on store retention time.
	 *
	 * @return the maximum size of the specified windows
	 */
	MaxSize() uint64
	/**
	 * Return the window grace period (the time to admit
	 * out-of-order events after the end of the window.)
	 *
	 * Delay is defined as (stream_time - record_timestamp).
	 */
	GracePeriodMs() uint64
}
