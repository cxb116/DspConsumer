package main

import (
	"fmt"
	"time"
)

func main() {
	format := time.Now().Add(10 * time.Minute).Truncate(10 * time.Minute).Format("200601021504")
	fmt.Println(format)
}
