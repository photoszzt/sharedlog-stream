package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/utils"
)

var (
	FLAGS_spec       string
	FLAGS_app_name   string
	FLAGS_outputFile string
)

func outputEvents(events []*ntypes.Event, outputFile string) error {
	earr := events_arr{
		Earr: events,
	}
	earr_bytes, err := json.Marshal(earr)
	if err != nil {
		return err
	}
	absOutFileName, err := filepath.Abs(outputFile)
	if err != nil {
		return err
	}
	err = os.WriteFile(absOutFileName, earr_bytes, 0660)
	return err
}

func main() {
	flag.StringVar(&FLAGS_app_name, "app_name", "q5", "")
	flag.StringVar(&FLAGS_spec, "spec", "", "spec to generate events")
	flag.StringVar(&FLAGS_outputFile, "ofile", "", "output file name")
	flag.Parse()

	if FLAGS_app_name == "" {
		fmt.Fprintf(os.Stderr, "must specify app name")
		return
	}

	if FLAGS_outputFile == "" {
		fmt.Fprintf(os.Stderr, "should specify output file name")
		return
	}

	if FLAGS_spec == "" {
		fmt.Fprintf(os.Stderr, "spec file must be specified")
		return
	}

	specs, err := utils.ReadFileContent(FLAGS_spec)
	if err != nil {
		panic(err)
	}
	if FLAGS_app_name == "q5" {
		err = q5_gen_data(specs, FLAGS_outputFile)
		if err != nil {
			panic(err)
		}
	} else if FLAGS_app_name == "q7" {
		err = q7_gen_data(specs, FLAGS_outputFile)
		if err != nil {
			panic(err)
		}
	} else if FLAGS_app_name == "q8" {
		err = q8_gen_data(specs, FLAGS_outputFile)
		if err != nil {
			panic(err)
		}
	}
}
