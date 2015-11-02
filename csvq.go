package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
	"path/filepath"
)

type ProgramOptions struct {
	delimiter rune
	keepExtension bool
	hasHeader bool
}

func main() {

	var query = flag.String("q", "SELECT sqlite_version()", "The query to run over the files.  Omit to retrieve the SQLite library version.")
	var dbName = flag.String("db", ":memory:", "The name of the database to create.  Omit to create an in-memory database")
	var dm = flag.String("dl", ",", "The field delimiter.  The default is a comma")
	var keepExtension = flag.Bool("ke", false, "Whether or not the file extension should saved as part of the table name")
	var hasHeader = flag.Bool("he", true, "Whether or not the input files have a header row.")

	flag.Parse();

	options := ProgramOptions{}
	options.delimiter, _ = utf8.DecodeRuneInString(*dm)
	options.keepExtension = *keepExtension
	options.hasHeader = *hasHeader

	db, err := sqlx.Open("sqlite3", *dbName)
	checkErr(err)

	defer db.Close()

	var tableRegex = regexp.MustCompile(`{{(?P<tableName>\s*[\w\.]+\s*)}}`)
	//var tableRegex = regexp.MustCompile(`{{(?P<tableName>.*)}}`)

	matches := tableRegex.FindAllStringSubmatch(*query, -1)

	// This will store the set of files
	// We store it as a set so that self joins don't result
	// in the same tables being created twic
	fileSet := make(map[string]struct{})

	var modifiedQuery = *query
	for _, match := range matches {

		var filePath = strings.TrimSpace(match[1])

		// Check that this file exists
		if _, err := os.Stat(filePath); err != nil {
			fmt.Printf("%s \n", err)
			os.Exit(-1)
		}

		modifiedQuery = strings.Replace(modifiedQuery, match[0], "\"" + filePath + "\"", -1)
		fileSet[filePath] = struct{}{}

		//fmt.Printf("%s\n", modifiedQuery)
	}

	// Add any extra files specified in the command line
	for i := 0; i < flag.NArg(); i++ {
		var filePath = strings.TrimSpace(flag.Arg(i))
		fmt.Printf("%s\n", filePath)

		// Check that this file exists
		if _, err := os.Stat(filePath); err != nil {
			fmt.Printf("%s \n", err)
			os.Exit(-1)
		}

		fileSet[filePath] = struct{}{}

	}


	for filePath := range fileSet {
		tableName := createTableFromFile(db, filePath, options)
		
		// Obvious bugs ahoy...
		modifiedQuery = strings.Replace(modifiedQuery, filePath, tableName, -1)
	}

	rows, err := db.Queryx(modifiedQuery)
	checkErr(err)
	defer rows.Close()

	results := map[string]interface{}{}
	for rows.Next() {
		err = rows.MapScan(results)
		checkErr(err)

		for k := range results {
			fmt.Printf("%s\t", results[k])
		}

		fmt.Println()
	}
}

func createTableFromFile(db *sqlx.DB, fileName string, options ProgramOptions) string {
	f, err := os.Open(fileName)
	checkErr(err)

	defer f.Close()

	csvReader := csv.NewReader(f)
	csvReader.Comma = options.delimiter

	lines, err := csvReader.ReadAll()
	checkErr(err)

	var createStatement = `CREATE TABLE IF NOT EXISTS "%s" (`
	var tableName string
	var insertStatement = ``
	
	const BATCH_SIZE int = 200

	for i, line := range lines {

		if i == 0 {
			for j := 0; j < len(line); j++ {
				if j > 0 {
					createStatement += ", "
				}

				var column string
				if options.hasHeader {
					column = fmt.Sprintf(`"%s" TEXT NULL`, line[j])
				} else {
					column = fmt.Sprintf(`"Column%d" TEXT NULL`, j + 1)

				}

				createStatement += column

			}

			createStatement += ");"

			
			if options.keepExtension {
				tableName = fileName[0:len(fileName)]
			} else {
				var extension = filepath.Ext(fileName)
				tableName = fileName[0:len(fileName) - len(extension)]
			}

			createStatement = fmt.Sprintf(createStatement, tableName)

			execSql(db, createStatement)

			if options.hasHeader {
				continue
			}
		}

		// Perform a bulk insert
		var row int
		if options.hasHeader {
			row = i - 1
		} else {
			row = i
		}

		if row == 0 || (row % BATCH_SIZE) == 0 {
			// Insert the current batch
			if len(insertStatement) > 0 {

				execSql(db, insertStatement)
				insertStatement = ""

			}

			// Start a new batch
			insertStatement = `INSERT INTO "%s" VALUES ("%s")`
			insertStatement = fmt.Sprintf(insertStatement, tableName, strings.Join(line, `","`))
		} else {
			insertStatement += `, ("%s")` + "\n"
			insertStatement = fmt.Sprintf(insertStatement, strings.Join(line, `","`))	
		}
	}

	// Insert the final batch
	if len(insertStatement) > 0 {
		// fmt.Println(insertStatement)
		execSql(db, insertStatement)
	}

	return tableName

}

func execSql(db *sqlx.DB, sql string) {
	stmt, err := db.Prepare(sql)
	checkErr(err)

	_, err = stmt.Exec()

	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
