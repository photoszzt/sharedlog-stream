// Code generated by "stringer -type=GuaranteeMth"; DO NOT EDIT.

package exactly_once_intr

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[NO_GUARANTEE-0]
	_ = x[AT_LEAST_ONCE-1]
	_ = x[TWO_PHASE_COMMIT-2]
	_ = x[REMOTE_2PC-3]
	_ = x[EPOCH_MARK-4]
	_ = x[ALIGN_CHKPT-5]
}

const _GuaranteeMth_name = "NO_GUARANTEEAT_LEAST_ONCETWO_PHASE_COMMITREMOTE_2PCEPOCH_MARKALIGN_CHKPT"

var _GuaranteeMth_index = [...]uint8{0, 12, 25, 41, 51, 61, 72}

func (i GuaranteeMth) String() string {
	if i >= GuaranteeMth(len(_GuaranteeMth_index)-1) {
		return "GuaranteeMth(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _GuaranteeMth_name[_GuaranteeMth_index[i]:_GuaranteeMth_index[i+1]]
}
