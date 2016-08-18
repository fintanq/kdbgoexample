package main

import (
	"fmt"
	"github.com/sv/kdbgo"
)

func main(){
/*
	setup kdb+tick process on port 5010 as per http://code.kx.com/wsvn/code/kx/kdb+tick
*/

	con, err := kdb.DialKDB("localhost", 5010, "")
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "trade"}, &kdb.K{-kdb.KS, kdb.NONE, "GOOG"})
	if err != nil {
                fmt.Println("Subscribe:", err)
                return
	}
	for {
		// ignore type print output
		res, _, err := con.ReadMessage()
		if err != nil {
			fmt.Println("Error processing message: ", err)
			return
		}
		fmt.Println(res)
	}
}
