package main

type Q5WorkloadConfig struct {
	SrcOutTopic               string `json:"srcOutTopic"`
	BidKeyedByAuctionOutTopic string `json:"bidKeyedByAucOutTopic"`
	AucBidsOutTopic           string `json:"aucBidsOutTopic"`
	MaxBidsOutTopic           string `json:"maxBidsOutTopic"`
	NumSrcPartition           uint8  `json:"numSrcPartition"`
	NumSrcInstance            uint8  `json:"numSrcInstance"`
	NumInstance               uint8  `json:"numInstance"`
}

type BasicWorkloadConfig struct {
	SrcOutTopic     string `json:"srcOutTopic"`
	SinkOutTopic    string `json:"sinkOutTopic"`
	NumSrcPartition uint8  `json:"numSrcPartition"`
	NumSrcInstance  uint8  `json:"numSrcInstance"`
}

type Q7WorkloadConfig struct {
	SrcOutTopic             string `json:"srcOutTopic"`
	BidKeyedByPriceOutTopic string `json:"bidKeyedByPriceOutTopic"`
	Q7TransOutTopic         string `json:"q7TransOutTopic"`
	NumSrcPartition         uint8  `json:"numSrcPartition"`
	NumInstance             uint8  `json:"numInstance"`
	NumSrcInstance          uint8  `json:"numSrcInstance"`
}

type Q3WorkloadConfig struct {
	SrcOutTopic            string `json:"srcOutTopic"`
	AucsBySellerIDOutTopic string `json:"aucsBySellerIDOutTopic"`
	PersonsByIDOutTopic    string `json:"persByIDOutTopic"`
	JoinTableOutTopic      string `json:"joinTableOutTopic"`
	NumSrcPartition        uint8  `json:"numSrcPartition"`
	NumInstance            uint8  `json:"numInstance"`
	NumSrcInstance         uint8  `json:"numSrcInstance"`
}

type Q8WorkloadConfig struct {
	SrcOutTopic            string `json:"srcOutTopic"`
	AucsBySellerIDOutTopic string `json:"aucsBySellerIDOutTopic"`
	PersonsByIDOutTopic    string `json:"persByIDOutTopic"`
	JoinStreamOutTopic     string `json:"joinStreamOutTopic"`
	NumSrcPartition        uint8  `json:"numSrcPartition"`
	NumInstance            uint8  `json:"numInstance"`
	NumSrcInstance         uint8  `json:"numSrcInstance"`
}

type WindowedAvgConfig struct {
	SrcOutTopic     string `json:"srcOutTopic"`
	GroupByOutTopic string `json:"groupByOutTopic"`
	AvgOutTopic     string `json:"avgOutTopic"`
	NumSrcPartition uint8  `json:"numSrcPartition"`
	NumInstance     uint8  `json:"numInstance"`
	NumSrcInstance  uint8  `json:"numSrcInstance"`
}
