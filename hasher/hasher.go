package chghasher

import (
	"compress/gzip"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"io"
	"log"
	"os"
)

// HashChgHourly is a function that accepts a hourly CHG file (plain text),
// and replace the content of MSISDN column with its hashed value.
func HashChgHourly(csvIn, csvOut string, msisdnColumnIndex int) {
	// Setup CSV reader
	cIn, err := os.Open(csvIn)
	if err != nil {
		log.Fatal(err)
	}
	defer cIn.Close()

	r := csv.NewReader(cIn)
	// Set the reader object to accept varying number of CSV fields
	r.FieldsPerRecord = -1
	// Set for delimiter type of '|'
	// Use single quote to preserve as rune object and use Lazy Quotes
	r.Comma = '|'
	r.LazyQuotes = true

	// Setup CSV writer
	cOut, err := os.Create(csvOut)
	if err != nil {
		log.Fatal("Unable to open output file")
	}
	w := csv.NewWriter(cOut)
	// Use the same delimiter for the writer object
	w.Comma = '|'
	defer cOut.Close()

	// Handle CSV header
	rec, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}

	// Check error for writer object creation
	if err = w.Write(rec); err != nil {
		log.Fatal(err)
	}

	// Loop CSV file per line
	for {
		rec, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		// Get MSISDN value based on the column number provided
		// add a newline char at the end of string to cater with
		// "echo $msisdn | md5sum" logic.
		msisdn := []byte(rec[msisdnColumnIndex] + "\n")

		// Hash the MSISDN. If using md5 then it will produce a [16]byte type.
		// We reconvert the value to readable format (Hex-String-Encoded)
		msisdnHashed := md5.Sum([]byte(msisdn))
		msisdnHashedString := hex.EncodeToString(msisdnHashed[:])

		// Replace the original MSISDN column with hashed MSISDN
		rec[msisdnColumnIndex] = msisdnHashedString

		// Check error for writer object creation
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
		w.Flush()
	}
}

// HashChgHourlyGzip is a function that accepts a hourly CHG file (in gzip),
// and replace the content of MSISDN column with its hashed value.
func HashChgHourlyGzip(gzipIn, csvOut string, msisdnColumnIndex int) {
	// Setup GZIP reader object
	gzIn, err := os.Open(gzipIn)
	if err != nil {
		log.Fatal(err)
	}
	defer gzIn.Close()
	gr, err := gzip.NewReader(gzIn)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	// Setup CSV reader object
	r := csv.NewReader(gr)
	// Set the reader object to accept varying number of CSV fields
	r.FieldsPerRecord = -1
	// Set for delimiter type of '|'
	// Use single quote to preserve as rune object and use Lazy Quotes
	r.Comma = '|'
	r.LazyQuotes = true

	// Setup CSV writer
	cOut, err := os.Create(csvOut)
	if err != nil {
		log.Fatal("Unable to open output file")
	}
	w := csv.NewWriter(cOut)
	// Use the same delimiter for the writer object
	w.Comma = '|'
	defer cOut.Close()

	// Handle CSV header
	rec, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}

	// Check error for writer object creation
	if err = w.Write(rec); err != nil {
		log.Fatal(err)
	}

	// Loop CSV file per line
	for {
		rec, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		// Get MSISDN value based on the column number provided
		// add a newline char at the end of string to cater with
		// "echo $msisdn | md5sum" logic.
		msisdn := []byte(rec[msisdnColumnIndex] + "\n")

		// Hash the MSISDN. If using md5 then it will produce a [16]byte type.
		// We reconvert the value to readable format (Hex-String-Encoded)
		msisdnHashed := md5.Sum([]byte(msisdn))
		msisdnHashedString := hex.EncodeToString(msisdnHashed[:])

		// Replace the original MSISDN column with hashed MSISDN
		rec[msisdnColumnIndex] = msisdnHashedString

		// Check error for writer object creation
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
		w.Flush()
	}
}

// HashChgMonthly is a function that accepts a monthly CHG file (plain text),
// and replace the content of MSISDN column with its hashed value.
func HashChgMonthly(csvIn, csvOut string, msisdnColumnIndex int) {
	// Setup CSV reader
	cIn, err := os.Open(csvIn)
	if err != nil {
		log.Fatal(err)
	}
	defer cIn.Close()

	r := csv.NewReader(cIn)
	// Set reader for varying number of fields
	r.FieldsPerRecord = -1
	// Set for delimiter type of '|'
	// Use single quote so it will still be preserved as a rune object and use Lazy Quotes
	r.Comma = '|'
	r.LazyQuotes = true

	// Setup CSV writer
	cOut, err := os.Create(csvOut)
	if err != nil {
		log.Fatal("Unable to open output file")
	}
	w := csv.NewWriter(cOut)
	// Use the same delimiter for the writer object
	w.Comma = '|'
	defer cOut.Close()

	// Handle CSV header
	rec, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}

	// Check error during writer object creation
	if err = w.Write(rec); err != nil {
		log.Fatal(err)
	}

	// Loop CSV per line
	for {
		rec, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		// Get MSISDN value.
		// Append with "\n" to cater the
		// `echo $msisdn | md5sum` logic
		msisdn := []byte(rec[msisdnColumnIndex] + "\n")

		// Hash the MSISDN using MD5 algorithm.
		// The value will be of [16]byte type.
		// Encode to hex string format.
		msisdnHashed := md5.Sum([]byte(msisdn))
		msisdnHashedString := hex.EncodeToString(msisdnHashed[:])

		// Replace the original MSISDN column with the
		// value of hashed MSISDN
		rec[msisdnColumnIndex] = msisdnHashedString

		// Check error during writer object creation
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
		w.Flush()
	}
}

// HashChgMonthlyGzip is a function that accepts a monthly CHG file (gzip format),
// and replace the content of MSISDN column with its hashed value.
func HashChgMonthlyGzip(gzipIn, csvOut string, msisdnColumnIndex int) {
	// Setup GZIP reader object
	gzIn, err := os.Open(gzipIn)
	if err != nil {
		log.Fatal(err)
	}
	defer gzIn.Close()
	gr, err := gzip.NewReader(gzIn)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	// Setup CSV reader
	r := csv.NewReader(gr)
	// Set reader for varying number of fields
	r.FieldsPerRecord = -1
	// Set for delimiter type of '|'
	// Use single quote so it will still be preserved as a rune object and use Lazy Quotes
	r.Comma = '|'
	r.LazyQuotes = true

	// Setup CSV writer
	cOut, err := os.Create(csvOut)
	if err != nil {
		log.Fatal("Unable to open output file")
	}
	w := csv.NewWriter(cOut)
	// Use the same delimiter for the writer object
	w.Comma = '|'
	defer cOut.Close()

	// Handle CSV header
	rec, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}

	// Check error during writer object creation
	if err = w.Write(rec); err != nil {
		log.Fatal(err)
	}

	// Loop CSV per line
	for {
		rec, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		// Get MSISDN value.
		// Append with "\n" to cater the
		// `echo $msisdn | md5sum` logic
		msisdn := []byte(rec[msisdnColumnIndex] + "\n")

		// Hash the MSISDN using MD5 algorithm.
		// The value will be of [16]byte type.
		// Encode to hex string format.
		msisdnHashed := md5.Sum([]byte(msisdn))
		msisdnHashedString := hex.EncodeToString(msisdnHashed[:])

		// Replace the original MSISDN column with the
		// value of hashed MSISDN
		rec[msisdnColumnIndex] = msisdnHashedString

		// Check error during writer object creation
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
		w.Flush()
	}
}

// HashChgSubsInfo is a function that accepts subscriber info (plain text)
// and hash the value contained inside the MSISDN and IMSI columns.
func HashChgSubsInfo(csvIn, csvOut string, msisdnColumnIndex, imsiColumnIndex int) {
	// Setup CSV reader
	cIn, err := os.Open(csvIn)
	if err != nil {
		log.Fatal(err)
	}
	defer cIn.Close()

	r := csv.NewReader(cIn)
	// Set reader for varying number of fields
	r.FieldsPerRecord = -1
	// Set for delimiter type of '|'
	// use single quote to preserve as rune object and use Lazy Quotes
	r.Comma = '|'
	r.LazyQuotes = true

	// Setup CSV writer
	cOut, err := os.Create(csvOut)
	if err != nil {
		log.Fatal("Unable to open output file")
	}
	w := csv.NewWriter(cOut)
	// Use the same delimiter for the writer object
	w.Comma = '|'
	defer cOut.Close()

	// Handle CSV header
	rec, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}

	// Check error for writer object creation
	if err = w.Write(rec); err != nil {
		log.Fatal(err)
	}

	// Loop CSV per line
	for {
		rec, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		// Get MSISDN and IMSI value.
		// Append "\n" at the end of both
		// to cater for `echo $msisdn | md5sum"
		msisdn := []byte(rec[msisdnColumnIndex] + "\n")
		imsi := []byte(rec[imsiColumnIndex] + "\n")

		// MD5 hash the MSISDN, IMSI, and convert
		// the resulting [16]byte type to readable
		// hexadecimal representation.
		msisdnHashed := md5.Sum([]byte(msisdn))
		msisdnHashedString := hex.EncodeToString(msisdnHashed[:])

		imsiHashed := md5.Sum([]byte(imsi))
		imsiHashedString := hex.EncodeToString(imsiHashed[:])

		// Replace the original value of MSISDN, IMSI column
		// with the hashed values
		rec[msisdnColumnIndex] = msisdnHashedString
		rec[imsiColumnIndex] = imsiHashedString

		// Check error for writer object creation
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
		w.Flush()
	}
}

// HashChgSubsInfoGzip is a function that accepts subscriber info (gzip format)
// and hash the value contained inside the MSISDN and IMSI columns.
func HashChgSubsInfoGzip(gzipIn, csvOut string, msisdnColumnIndex, imsiColumnIndex int) {
	// Setup GZIP reader object
	gzIn, err := os.Open(gzipIn)
	if err != nil {
		log.Fatal(err)
	}
	defer gzIn.Close()
	gr, err := gzip.NewReader(gzIn)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	// Setup CSV reader
	r := csv.NewReader(gr)
	// Set reader for varying number of fields
	r.FieldsPerRecord = -1
	// Set for delimiter type of '|'
	// use single quote to preserve as rune object and use Lazy Quotes
	r.Comma = '|'
	r.LazyQuotes = true

	// Setup CSV writer
	cOut, err := os.Create(csvOut)
	if err != nil {
		log.Fatal("Unable to open output file")
	}
	w := csv.NewWriter(cOut)
	// Use the same delimiter for the writer object
	w.Comma = '|'
	defer cOut.Close()

	// Handle CSV header
	rec, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}

	// Check error for writer object creation
	if err = w.Write(rec); err != nil {
		log.Fatal(err)
	}

	// Loop CSV per line
	for {
		rec, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		// Get MSISDN and IMSI value.
		// Append "\n" at the end of both
		// to cater for `echo $msisdn | md5sum"
		msisdn := []byte(rec[msisdnColumnIndex] + "\n")
		imsi := []byte(rec[imsiColumnIndex] + "\n")

		// MD5 hash the MSISDN, IMSI, and convert
		// the resulting [16]byte type to readable
		// hexadecimal representation.
		msisdnHashed := md5.Sum([]byte(msisdn))
		msisdnHashedString := hex.EncodeToString(msisdnHashed[:])

		imsiHashed := md5.Sum([]byte(imsi))
		imsiHashedString := hex.EncodeToString(imsiHashed[:])

		// Replace the original value of MSISDN, IMSI column
		// with the hashed values
		rec[msisdnColumnIndex] = msisdnHashedString
		rec[imsiColumnIndex] = imsiHashedString

		// Check error for writer object creation
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
		w.Flush()
	}
}
