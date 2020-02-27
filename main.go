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
var fileNameFlag, chgTypeFlag string

func init() {
	// Set logging syntax
	log.SetPrefix("LOG: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds)
	log.Println("init started")

	// Set CLI input flags
	flag.StringVar(&fileNameFlag, "filename", "", "[REQUIRED] Name of the CHG file to be hashed")
	flag.StringVar(&chgTypeFlag, "type", "", "[REQUIRED] CHG type of the file to be hashed. Possible options : {hourly | monthly | subs}")
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
		fmt.Println("./msisdn_md5_hasher -filename=<csv_file_name> -type=<chg_type>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// CHG Type is mandatory
	if chgTypeFlag == "" {
		log.Println("FATAL: You must provide the type of the CHG file specified.")
		fmt.Println("=====")
		fmt.Println("Usage")
		fmt.Println("=====")
		fmt.Println("./msisdn_md5_hasher -filename=<csv_file_name> -type=<chg_type>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// File name is fed from the flag argument
	csvInFileName := fileNameFlag
	csvOutFileName := csvInFileName + ".hashed.out"

	// Logging
	log.Printf("Processing CHG file input :%v\n", csvInFileName)
	log.Printf("CHG Type is : %v\n", chgTypeFlag)
	log.Printf("Hashed CHG is written to : %v\n", csvOutFileName)

	// Set the index number of MSISDN column for chg type
	var msisdnIndex int

	// Switch case of chgTypeFlag
	if chgTypeFlag == "hourly" {
		msisdnIndex = 2 // zero indexed
		log.Printf("MSISDN Index in %v is : %v\n", csvInFileName, msisdnIndex)
		chghasher.HashChgHourly(csvInFileName, csvOutFileName, msisdnIndex)
	} else if chgTypeFlag == "monthly" {
		msisdnIndex = 1 // zero indexed
		log.Printf("MSISDN Index in %v is : %v\n", csvInFileName, msisdnIndex)
		chghasher.HashChgMonthly(csvInFileName, csvOutFileName, msisdnIndex)
	} else if chgTypeFlag == "subs" {
		imsiIndex := 1  // zero indexed
		msisdnIndex = 0 // zero indexed
		log.Printf("MSISDN Index in %v is : %v and IMSI Index is : %v\n", csvInFileName, msisdnIndex, imsiIndex)
		chghasher.HashChgSubsInfo(csvInFileName, csvOutFileName, msisdnIndex, imsiIndex)
	} else { // chg type can only contains the above values
		flag.PrintDefaults()
		os.Exit(1)
	}

	// End Timer
	elapsed := time.Since(start)
	log.Printf("Script took %s", elapsed)
}
