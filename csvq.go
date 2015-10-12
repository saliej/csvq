package main

import (
    "fmt"
    "os"
    "strings"
    "flag"
    //"database/sql"
    "encoding/csv"
    _ "github.com/mattn/go-sqlite3"
    "github.com/jmoiron/sqlx"
)

func main() {

    var query = flag.String("q", "SELECT sqlite_version()", "The query to run over the files.  Omit to retrieve the SQLite library version.")
    var dbName = flag.String("db", ":memory:", "The name of the database to create.  Omit to create an in-memory database")

	flag.Parse()

	db, err := sqlx.Open("sqlite3", *dbName)
    checkErr(err)

    defer db.Close()

    for i := 0; i < flag.NArg(); i++ {
        createTableFromFile(db, flag.Arg(i))
    }

    // query
    rows, err := db.Queryx(*query)
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

func createTableFromFile (db *sqlx.DB, fileName string) {
    f, err := os.Open(fileName)
    checkErr(err)

    defer f.Close()

    csvReader := csv.NewReader(f)

    lines, err := csvReader.ReadAll()
    checkErr(err)

    var createStatement = `CREATE TABLE "%s" (`
    for i, line := range lines {

        if i == 0 {
            for j := 0; j < len(line); j++ {
                if j > 0 {
                    createStatement += ", "
                }

                column := fmt.Sprintf(`"%s" TEXT NULL`, line[j])
                createStatement += column
            }
            
            createStatement += ");"

            createStatement = fmt.Sprintf(createStatement, fileName)

            // fmt.Println(createStatement)

            stmt, err := db.Prepare(createStatement)
            checkErr(err)

            _, err = stmt.Exec()    

            checkErr(err)

            continue
        }

        var insertStatement = `INSERT INTO "%s" VALUES ("%s")`
        insertStatement = fmt.Sprintf(insertStatement, fileName, strings.Join(line, `","`))
        //  fmt.Println(insertStatement);
        stmt, err := db.Prepare(insertStatement)
        checkErr(err)

        _, err = stmt.Exec()    

        checkErr(err)
        
        //break
    }



}

func checkErr(err error) {
    if err != nil {
        panic(err)
    }
}
