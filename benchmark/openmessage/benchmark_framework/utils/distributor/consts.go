package distributor

type KeyDistributorType uint8

const (
	NO_KEY          KeyDistributorType = 0
	KEY_ROUND_ROBIN KeyDistributorType = 1
	RANDOM_NANO     KeyDistributorType = 2
)
