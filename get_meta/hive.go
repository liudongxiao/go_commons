package get_meta

import (
	"dmp_web/go/commons/reader"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/akolb1/gometastore/hmsclient"
)

type table struct {
	name               string
	db                 *DB
	columns            []column
	partitions         []column
	NotPatitionColumns []column
}

type SimpleTable struct {
	name    string
	columns string
}

type DB struct {
	name   string
	tables []*table
	host   *hmsclient.MetastoreClient
}

func NewDB() {

}

type SimpleDB struct {
	name   string
	tables []string
}

type column struct {
	name      string
	partition bool
	note      string
}

type saveLocation struct {
	time time.Time
	path string
}

func init() {
	NewConfig()
}

var host string
var port int

var path string
var debug bool

func NewConfig() {
	flag.StringVar(&host, "host", host, "hive metaStore metaHost")
	flag.IntVar(&port, "Port", port, "hive metaStore port")
	//flag.StringVar(&metaHost.UserName, "userName", "root", "userName")
	//flag.StringVar(&metaHost.Password, "password", "root", "password")
	flag.StringVar(&path, "path ", "/tmp/get_database_meta_result", "file path")
	flag.BoolVar(&debug, "debug", false, "debug mode, print message to std io ")
	flag.Parse()

}

func usage() {
	fmt.Println("usage: ")
	fmt.Println(`./get_hive_meta -host 192.168.10.60 -port     -path "./hive_db_meta `)
}

func write() error {
	cli, err := hmsclient.Open(host, port)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	dbs, err := cli.GetAllDatabases()
	if err != nil {
		return err
	}
	//if debug {
	//	fmt.Println("all dbs are  : ", dbs)
	//	for _, DB := range dbs {
	//		tables, err := cli.GetAllTables(DB)
	//		if err != nil {
	//			panic(err)
	//		}
	//		fmt.Printf("DB %s: ", DB)
	//		fmt.Println(tables)
	//	}
	//}

	files, err := reader.MultiFileWriter(path)
	if err != nil {
		panic(err)
	}

	var mw io.WriteCloser
	if debug {
		mw = reader.MultiWriteCloser(os.Stdin, files)

	}
	mw = reader.MultiWriteCloser(files)

	mw.Write([]byte(fmt.Sprintln("all dbs are  : ", strings.Join(dbs, ","))))
	for _, db := range dbs {
		tables, err := cli.GetAllTables(db)
		if err != nil {
			return err
		}

		_, err = mw.Write([]byte(fmt.Sprintln((strings.Join(tables, ",")))))
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := write(); err != nil {
		panic(err)
	}

}
