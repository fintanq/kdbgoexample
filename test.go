package main

import (
	"fmt"
	"github.com/sv/kdbgo"
)

func main(){
/*
	setup q process on port 1234: bash$ q -p 1234
	create simple table:
	n:1000000;t:([]sym:n?`ibm`msft`kx;price:n?1000.;size:n?10000)
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
	PrintTbl(tbl.Data.(kdb.Table))
	fmt.Print("\n")

	// keyed table
	ktbl, err := con.Call("select vwap:size wavg price, lprice:last price by sym from t")
	if err != nil {
		fmt.Println("Query failed:", err)
		return
	}
	PrintKeyTbl(ktbl.Data.(kdb.Dict))
	fmt.Print("\n")

	// dictionary
	dict, err := con.Call("exec size wavg price by sym from t")
	if err != nil {
		fmt.Println("Query failed:", err)
		return
	}
	PrintDict(dict.Data.(kdb.Dict))

	// async call
	err = con.AsyncCall("a:1 2 3")
        if err != nil {
		fmt.Println("Async call failed", err)
        }

	// sync list
	res, err := con.Call("{x+y}", &kdb.K{-kdb.KJ, kdb.NONE, int64(1)}, &kdb.K{-kdb.KJ, kdb.NONE, int64(2)})
	if err != nil {
		fmt.Println("sync list call failed", err)
	}
	fmt.Println(res)

	// single insert
	sym := &kdb.K{-kdb.KS, kdb.NONE, "kx"}
	price := &kdb.K{-kdb.KF, kdb.NONE, float64(100.1)}
	size := &kdb.K{-kdb.KJ, kdb.NONE, int64(1000)}
	row := &kdb.K{kdb.K0, kdb.NONE, []*kdb.K{sym, price, size}}
	// insert row sync
	insertRes, err := con.Call("insert", &kdb.K{-kdb.KS, kdb.NONE, "t"}, row)
	if err != nil {
		fmt.Println("Query failed:", err)
	        return
	}
	fmt.Println(insertRes)


	// bulk insert
	syms := &kdb.K{kdb.KS, kdb.NONE, []string{"kx","msft"}}
	prices := &kdb.K{kdb.KF, kdb.NONE, []float64{1.1,100.1}}
	sizes := &kdb.K{kdb.KJ, kdb.NONE, []int64{1000,2000}}
	tab := &kdb.K{kdb.XT, kdb.NONE, kdb.Table{[]string{"sym","price","size"},[]*kdb.K{syms, prices, sizes}}}
	// insert tab sync
	bulkInsertRes, err := con.Call("insert", &kdb.K{-kdb.KS, kdb.NONE, "t"}, tab)
	if err != nil {
	        fmt.Println("Query failed:", err)
                return
        }
	fmt.Println(bulkInsertRes)
	// close connection
	con.Close()
}

func PrintTbl(tbl kdb.Table){
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

func PrintKeyTbl(ktbl kdb.Dict){
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

func PrintDict(dict kdb.Dict){
	for i := 0; i < int(dict.Key.Len()); i++ {
		fmt.Printf("%s\t| %v\n", dict.Key.Index(i), dict.Value.Index(i))
	}
	return
}
