package main

import (
	"ces27-lab1-part2/mapreduce"
	"os"
	"strconv"
	"testing"
)

func createTestFile(t *testing.T, fileName string, content string) int {
	var (
		err      error
		fileSize int
		file     *os.File
	)

	if file, err = os.Create(fileName); err != nil {
		t.Error("Couldn't create file '", fileName, "'. Error: ", err)
	}

	if fileSize, err = file.WriteString(content); err != nil {
		t.Error("Couldn't write to file. Error: ", err)
	}

	file.Close()

	return fileSize
}

func deleteTestFile(t *testing.T, fileName string) {
	var err error
	if err = os.Remove(fileName); err != nil {
		t.Error("Couldn't delete file '", fileName, "' . Error: ", err)
	}
}

func TestSplitData(t *testing.T) {
	var tests = []struct {
		description string
		content     string
		chunkSize   int
	}{
		{"text file empty", "", 32},
		{"text files bigger than chunk size", "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In eu elit vel libero auctor tempor nullam.", 32},
		{"text file smaller than chunk size", "Lorem ipsum.", 32},
		{"text file has exact chunk size", "Lorem ipsum dolor sit amet, con.", 32},
	}

	var (
		err              error
		fileName         = "test_data"
		fileSize         int
		expectedNumFiles int
		tmpFileName      string
		tmpFile          *os.File
		tmpFileInfo      os.FileInfo
	)

	_ = os.Mkdir(MAP_PATH, os.ModeDir)

	for _, test := range tests {
		t.Logf("Description: %v", test.description)

		fileSize = createTestFile(t, fileName, test.content)

		if _, err = splitData(fileName, test.chunkSize); err != nil {
			t.Error("Couldn't split data file. Error: ", err)
		}

		expectedNumFiles = fileSize / test.chunkSize
		if fileSize%test.chunkSize > 0 {
			expectedNumFiles++
		}

		for i := 0; i < expectedNumFiles; i++ {
			tmpFileName = mapFileName(i)

			if tmpFile, err = os.Open(tmpFileName); err != nil {
				deleteTestFile(t, fileName)
				t.Fatal("Couldn't open '", tmpFileName, "'. Error: ", err)
			}

			if tmpFileInfo, err = tmpFile.Stat(); err != nil {
				t.Error("Couldn't read stats for '", tmpFileName, "'. Error: ", err)
			}

			if tmpFileInfo.Size() > int64(test.chunkSize) {
				t.Error("File '", tmpFileName, "' is larger than chunk size.")
			}

			tmpFile.Close()

			deleteTestFile(t, tmpFileName)
		}

		deleteTestFile(t, fileName)
	}
}

func TestMapFunc(t *testing.T) {
	var tests = []struct {
		description string
		input       []byte
		output      map[string]int
	}{
		{"empty", []byte(""), make(map[string]int, 0)},
		{"one word", []byte("foo"), map[string]int{"foo": 1}},
		{"two words", []byte("foo foo"), map[string]int{"foo": 2}},
		{"repeated word", []byte("foo refoo foo"), map[string]int{"foo": 2, "refoo": 1}},
		{"invalid character", []byte("foo-bar"), map[string]int{"foo": 1, "bar": 1}},
		{"newline character", []byte("foo\nbar"), map[string]int{"foo": 1, "bar": 1}},
		{"multiple whitespaces", []byte("foo  bar"), map[string]int{"foo": 1, "bar": 1}},
		{"special characters", []byte("foo, foo. foo? foo! \"foo\" 'foo' foo's"), map[string]int{"foo": 7, "s": 1}},
		{"uppercase characters", []byte("Foo foo"), map[string]int{"foo": 2}},
	}

	var (
		mapResult []mapreduce.KeyValue
		combined  map[string]int
	)

	for _, test := range tests {
		t.Logf("Description: %v", test.description)

		mapResult = mapFunc(test.input)

		combined = make(map[string]int, 0)

		for _, kv := range mapResult {
			if _, ok := combined[kv.Key]; !ok {
				value, err := strconv.Atoi(kv.Value)
				if err != nil {
					combined[kv.Key] = 1
				} else {
					combined[kv.Key] = value
				}

			} else {
				value, err := strconv.Atoi(kv.Value)
				if err != nil {
					combined[kv.Key] += 1
				} else {
					combined[kv.Key] += value
				}
			}
		}

		for k, v := range combined {
			if test.output[k] != v {
				t.Error("Expected:", k, ":", test.output[k], " ->  Received:", k, ":", v)
			}
		}

		for k, v := range test.output {
			if _, ok := combined[k]; !ok {
				t.Error("Expected:", k, ":", test.output[k], " ->  Not Found!")
			} else if combined[k] != v {
				t.Error("Expected:", k, ":", test.output[k], " ->  Received:", k, ":", combined[k])
			}
		}
	}
}

func TestReduceFunc(t *testing.T) {
	var tests = []struct {
		description string
		input       []mapreduce.KeyValue
		output      map[string]string
	}{
		{
			"no entry",
			make([]mapreduce.KeyValue, 0),
			make(map[string]string, 0),
		},
		{
			"one entry",
			[]mapreduce.KeyValue{{"foo", "1"}},
			map[string]string{"foo": "1"},
		},
		{
			"two entries with same keys",
			[]mapreduce.KeyValue{{"foo", "1"}, {"foo", "2"}},
			map[string]string{"foo": "3"},
		},
		{
			"two entries with different keys",
			[]mapreduce.KeyValue{{"foo", "1"}, {"bar", "2"}},
			map[string]string{"foo": "1", "bar": "2"},
		},
		{
			"non-numeric counter",
			[]mapreduce.KeyValue{{"foo", "+"}, {"foo", "+"}},
			map[string]string{"foo": "2"},
		},
	}

	var (
		reduceResult []mapreduce.KeyValue
		foundKey     bool
	)

	for _, test := range tests {
		t.Logf("Description: %v", test.description)

		reduceResult = reduceFunc(test.input)

		for _, kv := range reduceResult {
			if test.output[kv.Key] != kv.Value {
				t.Error("Expected:", kv.Key, ":", test.output[kv.Key], " ->  Received:", kv.Key, ":", kv.Value)
			}
		}

		for k, v := range test.output {
			foundKey = false
			for _, kv := range reduceResult {

				if k == kv.Key {
					foundKey = true
					break
				}
			}

			if !foundKey {
				t.Error("Expected:", k, ":", v, " ->  Received: Not Found!")
			}
		}
	}
}
