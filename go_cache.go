package main

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"time"
)

func main() {
	queryCache := cache.New(10*time.Second, 30*time.Second)
	requestCount := 0

	mockFetch := func() {
		fmt.Println("begin fetch", requestCount)
		time.Sleep(3 * 1000000000)
		fmt.Println("end fetch", requestCount)
		queryCache.Set("_fetch", nil, cache.DefaultExpiration)
	}

	mockDoQuery := func() string {
		fmt.Println("do query", requestCount)
		time.Sleep(1 * 1000000000)
		return "do query"
	}

	mockQuery := func(url string) {
		if cret, find := queryCache.Get(url); find && cret != nil {
			//fmt.Println("find cache", cret)
		} else {
			ret := mockDoQuery()
			queryCache.Set(url, ret, cache.DefaultExpiration)
		}

		if cret, find := queryCache.Get("_fetch"); !find || cret == nil {
			queryCache.Set("_fetch", 1, cache.DefaultExpiration)
			go mockFetch()
		}

	}

	for i := 0; i < 1; {
		mockQuery(string(i % 4))
		requestCount++
		time.Sleep(1 * 1)
	}

}
