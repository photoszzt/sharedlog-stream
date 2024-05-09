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

	ExtraImports string

	CommtypesPrefix string
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

	err = os.WriteFile(v.Path, out.Bytes(), 0644)
	if err != nil {
		log.Fatal("os.WriteFile:", err)
	}

	formatted, err := format.Source(out.Bytes())
	if err != nil {
		println(string(out.Bytes()))
		log.Fatal("format:", err)
	}

	if err := os.WriteFile(v.Path, formatted, 0644); err != nil {
		log.Fatal("WriteFile:", err)
	}
}

func gen_serde(fname, typeName, dirpath, packageName string, inCommtypes bool) {
	start_end := &Variant{
		PackageName: packageName,
		TypeName:    typeName,
		Path:        fmt.Sprintf("%s/%s_gen_serde.go", dirpath, fname),
	}
	if inCommtypes {
		start_end.ExtraImports = ""
		start_end.CommtypesPrefix = ""
	} else {
		start_end.ExtraImports = "\"sharedlog-stream/pkg/commtypes\""
		start_end.CommtypesPrefix = "commtypes."
	}
	generate(start_end, serde)
	start_end.Path = fmt.Sprintf("%s/%s_gen_serdeG.go", dirpath, fname)
	generate(start_end, serdeG)
}

func gen_serde_ptr(fname, typeName, dirpath, packageName string, inCommtypes bool) {
	start_end := &Variant{
		PackageName: packageName,
		TypeName:    typeName,
		Path:        fmt.Sprintf("%s/%s_gen_serde.go", dirpath, fname),
	}
	if inCommtypes {
		start_end.ExtraImports = ""
		start_end.CommtypesPrefix = ""
	} else {
		start_end.ExtraImports = "\"sharedlog-stream/pkg/commtypes\""
		start_end.CommtypesPrefix = "commtypes."
	}
	generate(start_end, serde_ptr)
	start_end.Path = fmt.Sprintf("%s/%s_gen_serdeG.go", dirpath, fname)
	generate(start_end, serdeG_ptr)
}

func main() {
	ntypes_path := "../benchmark/nexmark/pkg/nexmark/ntypes/"
	gen_serde("start_end_time", "StartEndTime", ntypes_path, "ntypes", false)
	gen_serde("sum_and_count", "SumAndCount", ntypes_path, "ntypes", false)
	gen_serde("price_time_list", "PriceTimeList", ntypes_path, "ntypes", false)
	gen_serde("price_time", "PriceTime", ntypes_path, "ntypes", false)
	gen_serde("person_time", "PersonTime", ntypes_path, "ntypes", false)
	gen_serde("name_city_state_id", "NameCityStateId", ntypes_path, "ntypes", false)
	gen_serde("bid_price", "BidPrice", ntypes_path, "ntypes", false)
	gen_serde("bid_and_max", "BidAndMax", ntypes_path, "ntypes", false)
	gen_serde("auction_id_seller", "AuctionIdSeller", ntypes_path, "ntypes", false)
	gen_serde("auction_id_count", "AuctionIdCount", ntypes_path, "ntypes", false)
	gen_serde("auction_id_cnt_max", "AuctionIdCntMax", ntypes_path, "ntypes", false)
	gen_serde("auction_id_category", "AuctionIdCategory", ntypes_path, "ntypes", false)
	gen_serde_ptr("event", "Event", ntypes_path, "ntypes", false)
	gen_serde_ptr("auction_bid", "AuctionBid", ntypes_path, "ntypes", false)

	commtypes_path := "../pkg/commtypes/"
	gen_serde("checkpoint", "Checkpoint", commtypes_path, "commtypes", true)
	gen_serde("epoch_meta", "EpochMarker", commtypes_path, "commtypes", true)
	gen_serde("payload_arr", "PayloadArr", commtypes_path, "commtypes", true)
	gen_serde("table_snapshots", "TableSnapshots", commtypes_path, "commtypes", true)
	gen_serde("offset_marker", "OffsetMarker", commtypes_path, "commtypes", true)
	gen_serde("message_serialized", "MessageSerialized", commtypes_path, "commtypes", true)
}

//go:embed serde.tmpl
var serde string

//go:embed serde_ptr.tmpl
var serde_ptr string

//go:embed serdeG.tmpl
var serdeG string

//go:embed serdeG_ptr.tmpl
var serdeG_ptr string
