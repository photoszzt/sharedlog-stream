// Code generated by "stringer -type=EType"; DO NOT EDIT.

package ntypes

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[PERSON-0]
	_ = x[AUCTION-1]
	_ = x[BID-2]
}

const _EType_name = "PERSONAUCTIONBID"

var _EType_index = [...]uint8{0, 6, 13, 16}

func (i EType) String() string {
	if i >= EType(len(_EType_index)-1) {
		return "EType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _EType_name[_EType_index[i]:_EType_index[i+1]]
}
