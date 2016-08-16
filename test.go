package main

import (
	"fmt"
	"kdbgo"
)

func main(){
/*
	setup q process on port 1234: bash$ q -p 1 2 3 4
	create simple table:
	n:1000000;t:([]time:asc n?.z.n;sym:n?`ibm`msft`kx;price:n?1000.;size:n?10000)
*/

	con, err := kdb.DialKDB("localhost", 1234, "")
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}

	// list
	list, err := con.Call("til", &kdb.K{-kdb.KI, kdb.NONE, int32(10)})
	if err != nil {
		fmt.Println("Query failed:", err)
		return
	}
	fmt.Println("Result:", list)
	fmt.Print("\n")

	// table
	tbl, err := con.Call("select open:first price, high:max price, low:min price, close:last price from t")
	if err != nil {
        	fmt.Println("Query failed:", err)
		return
	}
	printTbl(tbl.Data.(kdb.Table))
	fmt.Print("\n")

	// keyed table
	ktbl, err := con.Call("select vwap:size wavg price, lprice:last price by time.hh,sym from t")
	if err != nil {
		fmt.Println("Query failed:", err)
		return
	}
	printKeyTbl(ktbl.Data.(kdb.Dict))
	fmt.Print("\n")

	// dictionary
	dict, err := con.Call("exec size wavg price by sym from t")
	if err != nil {
		fmt.Println("Query failed:", err)
		return
	}
	printDict(dict.Data.(kdb.Dict))
}

func printTbl(tbl kdb.Table){
	ncols := len(tbl.Data)
	nrows := int(tbl.Data[0].Len())
	// print Columns
	for i := 0; i < ncols; i++ {
		fmt.Printf("%v\t",tbl.Columns[i])
	}
	fmt.Print("\n");
	for i := 0; i < ncols; i++ {
		fmt.Printf("--------")
	}
	fmt.Print("\n");
	// print Data
	for i := 0; i < nrows; i++ {
		for j:= 0; j < ncols; j++ {
			fmt.Printf("%v\t", tbl.Data[j].Index(i))
		}
		fmt.Print("\n")
	}
	return
}

func printKeyTbl(ktbl kdb.Dict){
	nkcols := len(ktbl.Key.Data.(kdb.Table).Data)
	nucols := len(ktbl.Value.Data.(kdb.Table).Data)
	nrows := int(ktbl.Key.Len())
	// print Columns
	for i := 0; i < nkcols; i++ {
		fmt.Printf("%v\t", ktbl.Key.Data.(kdb.Table).Columns[i])
	}
	fmt.Print("| ")
	for i := 0; i < nucols; i++ {
		fmt.Printf("%v\t", ktbl.Value.Data.(kdb.Table).Columns[i])
	}
	fmt.Print("\n")
	for i := 0; i < (nkcols + nucols); i++ {
		fmt.Printf("--------")
	}
	fmt.Print("\n")
	// print Data
	for i := 0; i < nrows; i++ {
		// print keys
		for j := 0; j < len(ktbl.Key.Data.(kdb.Table).Data); j++ {
			fmt.Printf("%v\t", ktbl.Key.Data.(kdb.Table).Data[j].Index(i))
		}
		fmt.Print("| ")
		// print values
		for j := 0; j < len(ktbl.Value.Data.(kdb.Table).Data); j++ {
			fmt.Printf("%v\t", ktbl.Value.Data.(kdb.Table).Data[j].Index(i))
		}
		fmt.Print("\n")
	}
	return
}

func printDict(dict kdb.Dict){
	for i := 0; i < int(dict.Key.Len()); i++ {
		fmt.Printf("%s\t| %v\n", dict.Key.Index(i), dict.Value.Index(i))
	}
	return
}
