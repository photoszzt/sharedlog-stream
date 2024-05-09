package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"log"
	"os"
	"text/template"
)

type Variant struct {
	// Package is the package name.
	PackageName string

	// Name is the variant name: should be unique among variants.
	TypeName string

	// Path is the file path into which the generator will emit the code for this
	// variant.
	Path string
}

func generate(v *Variant, code string) {
	// Parse templateCode anew for each variant because Parse requires Funcs to be
	// registered, and it helps type-check the funcs.
	tmpl, err := template.New("gen").Parse(code)
	if err != nil {
		log.Fatal("template Parse:", err)
	}

	var out bytes.Buffer
	err = tmpl.Execute(&out, v)
	if err != nil {
		log.Fatal("template Execute:", err)
	}

	os.WriteFile(v.Path, out.Bytes(), 0644)

	formatted, err := format.Source(out.Bytes())
	if err != nil {
		println(string(out.Bytes()))
		log.Fatal("format:", err)
	}

	if err := os.WriteFile(v.Path, formatted, 0644); err != nil {
		log.Fatal("WriteFile:", err)
	}
}

func gen_ntypes(fname string, typeName string) {
	nexmark_path := "../benchmark/nexmark/pkg/nexmark/ntypes/"
	start_end := &Variant{
		PackageName: "ntypes",
		TypeName:    typeName,
		Path:        fmt.Sprintf("%s/%s_gen_serde.go", nexmark_path, fname),
	}
	generate(start_end, serde)
	start_end.Path = fmt.Sprintf("%s/%s_gen_serdeG.go", nexmark_path, fname)
	generate(start_end, serdeG)
}

func main() {
	gen_ntypes("start_end_time", "StartEndTime")
	gen_ntypes("sum_and_count", "SumAndCount")
	gen_ntypes("price_time_list", "PriceTimeList")
	gen_ntypes("price_time", "PriceTime")
	gen_ntypes("person_time", "PersonTime")
	gen_ntypes("name_city_state_id", "NameCityStateId")
	gen_ntypes("bid_price", "BidPrice")
	gen_ntypes("bid_and_max", "BidAndMax")
	gen_ntypes("auction_id_seller", "AuctionIdSeller")
	gen_ntypes("auction_id_count", "AuctionIdCount")
	gen_ntypes("auction_id_cnt_max", "AuctionIdCntMax")
	gen_ntypes("auction_id_category", "AuctionIdCategory")

	nexmark_path := "../benchmark/nexmark/pkg/nexmark/ntypes/"
	fname := "auction_bid"
	auction_bid := &Variant{
		PackageName: "ntypes",
		TypeName:    "AuctionBid",
		Path:        fmt.Sprintf("%s/%s_gen_serde.go", nexmark_path, fname),
	}
	generate(auction_bid, serde_ptr)
	auction_bid.Path = fmt.Sprintf("%s/%s_gen_serdeG.go", nexmark_path, fname)
	generate(auction_bid, serdeG_ptr)
}

//go:embed serde.tmpl
var serde string

//go:embed serde_ptr.tmpl
var serde_ptr string

//go:embed serdeG.tmpl
var serdeG string

//go:embed serdeG_ptr.tmpl
var serdeG_ptr string
