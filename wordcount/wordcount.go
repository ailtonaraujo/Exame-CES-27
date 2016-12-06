package main

import (
	"ces27-lab1-part2/mapreduce"
	"strings"
	"unicode"
	"sort"

)

// mapFunc is called for each array of bytes read from the splitted files. For wordcount
// it should convert it into an array and parses it into an array of KeyValue that have
// all the words in the input.
func mapFunc(input []byte) (result []mapreduce.KeyValue) {
	var (
		text          string
		delimiterFunc func(c rune) bool
		words         []string
	)

	text = string(input)

	delimiterFunc = func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}

	words = strings.FieldsFunc(text, delimiterFunc)

	for index, word := range words {
		words[index] = strings.ToLower(word)
	}

	sort.Strings(words)

	result = make([]mapreduce.KeyValue, 0)

	for _, word := range words {
		kv := mapreduce.KeyValue{strings.ToLower(word), ""}
		result = append(result, kv)
	}

	return result
}

// reduceFunc is called for each merged array of KeyValue resulted from all map jobs.
// It should return a similar array that summarizes all similar keys in the input.
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	return input
}

// shuffleFunc will shuffle map job results into different job tasks. It should assert that
// the related keys will be sent to the same job, thus it will hash the key (a word) and assert
// that the same hash always goes to the same reduce job.
// http://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func shuffleFunc(task *mapreduce.Task, key string) (reduceJob int) {
	var asciiValue int
	var first rune
	for _,c := range key {
		first = c
		break
	}
	if first < 64{
		return 0
	}

	asciiValue = int(first - 'a')

	return asciiValue / ((128 - 'a') / task.NumReduceJobs) + 1
}
