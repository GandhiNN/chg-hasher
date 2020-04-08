package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	chghasher "github.com/gandhinn/chg-hasher/hasher"
)

// Flag initialisation
var fileNameFlag, chgTypeFlag, compression string
var revenue bool

func init() {
	// Set logging syntax
	log.SetPrefix("LOG: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds)
	log.Println("init started")

	// Set CLI input flags
	flag.StringVar(&fileNameFlag, "filename", "", "[REQUIRED] Name of the CHG file to be hashed")
	flag.StringVar(&chgTypeFlag, "type", "", "[REQUIRED] CHG type of the file to be hashed. Available options : {hourly | monthly | subs | upcc}")
	flag.StringVar(&compression, "compression", "plain", "Compression type of input file. Either 'gzip' or 'plain'")
	flag.BoolVar(&revenue, "revenue", false, "Invoke hashing of revenue data")
}

func main() {

	// Quick and dirty way to log script runtime
	start := time.Now()

	// Parse the CLI input flags
	flag.Parse()

	// File Name is mandatory
	if fileNameFlag == "" {
		log.Println("FATAL : You must provide the filename to be hashed.")
		fmt.Println("=====")
		fmt.Println("Usage")
		fmt.Println("=====")
		fmt.Println("./msisdn_md5_hasher -filename=<csv_file_name> -type=<chg_type> -revenue=<true|false>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// CHG Type is mandatory
	if chgTypeFlag == "" {
		log.Println("FATAL: You must provide the type of the CHG file specified.")
		fmt.Println("=====")
		fmt.Println("Usage")
		fmt.Println("=====")
		fmt.Println("./msisdn_md5_hasher -filename=<csv_file_name> -type=<chg_type> revenue=<true|false>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// File name is fed from the flag argument
	csvInFileName := fileNameFlag
	compressionType := compression
	csvOutFileName := csvInFileName + ".hashed.out"

	// Logging
	log.Printf("Processing CHG file input :%v\n", csvInFileName)
	log.Printf("CHG compression : %v\n", compressionType)
	log.Printf("CHG Type is : %v\n", chgTypeFlag)
	log.Printf("Hashed CHG is written to : %v\n", csvOutFileName)

	// Set the index number of MSISDN column for chg type
	var msisdnIndex int

	// Switch case of chgTypeFlag
	switch chgTypeFlag {
	case "hourly":
		msisdnIndex = 2
		log.Printf("MSISDN Index in %v is : %v\n", csvInFileName, msisdnIndex)
		if compressionType == "gzip" {
			chghasher.HashChgHourlyGzip(csvInFileName, csvOutFileName, msisdnIndex)
		} else {
			chghasher.HashChgHourly(csvInFileName, csvOutFileName, msisdnIndex)
		}
	case "monthly":
		msisdnIndex = 1 // zero indexed
		log.Printf("MSISDN Index in %v is : %v\n", csvInFileName, msisdnIndex)
		if compressionType == "gzip" {
			chghasher.HashChgMonthlyGzip(csvInFileName, csvOutFileName, msisdnIndex)
		} else {
			chghasher.HashChgMonthly(csvInFileName, csvOutFileName, msisdnIndex)
		}
	case "subs":
		imsiIndex := 1  // zero indexed
		msisdnIndex = 0 // zero indexed
		log.Printf("MSISDN Index in %v is : %v and IMSI Index is : %v\n", csvInFileName, msisdnIndex, imsiIndex)
		if compressionType == "gzip" {
			chghasher.HashChgSubsInfoGzip(csvInFileName, csvOutFileName, msisdnIndex, imsiIndex)
		} else {
			chghasher.HashChgSubsInfo(csvInFileName, csvOutFileName, msisdnIndex, imsiIndex)
		}
	case "upcc":
		var msisdnIndex int
		if revenue == true {
			msisdnIndex = 1
		} else {
			msisdnIndex = 2
		}
		log.Printf("MSISDN Index in %v is : %v\n", csvInFileName, msisdnIndex)
		if compressionType == "gzip" {
			chghasher.HashUpccHourlyGzip(csvInFileName, csvOutFileName, msisdnIndex)
		} else {
			chghasher.HashUpccHourly(csvInFileName, csvOutFileName, msisdnIndex)
		}
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}

	// End Timer
	elapsed := time.Since(start)
	log.Printf("Script took %s", elapsed)
}
