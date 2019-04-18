package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"io"
	"os"
	"regexp"
	"strings"
)

//LogSeeker logSeeker struct
type LogSeeker struct {
	FilePath string
	file     *os.File
	fields   int
	reader   *bufio.Reader
}

//New return new LogSeeker instance
func New(filePath string) (logSeeker *LogSeeker, err error) {
	logSeeker = &LogSeeker{
		FilePath: filePath,
	}

	logSeeker.file, err = os.Open(filePath)
	return
}

// BeginReader set the reader
func (logSeeker *LogSeeker) BeginReader() {
	logSeeker.reader = bufio.NewReader(logSeeker.file)
}

//BeginReaderSize set the reader with buffer size
func (logSeeker *LogSeeker) BeginReaderSize(size int) {
	logSeeker.reader = bufio.NewReaderSize(logSeeker.file, size)
}

//Tell return the file's current position
func (logSeeker *LogSeeker) Tell() (offset int64, err error) {
	if logSeeker.file == nil {
		return
	}
	offset, err = logSeeker.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return
	}
	return
}

//Seek seek file position
func (logSeeker *LogSeeker) Seek(offset int64, whence int) (ret int64, err error) {

	if logSeeker.file == nil {
		return
	}
	ret, err = logSeeker.file.Seek(offset, whence)
	if err != nil {
		return
	}
	return
}

func (logSeeker *LogSeeker) getFields(fieldSep rune, content string) (fields []string, err error) {

	r := csv.NewReader(strings.NewReader(content))
	r.Comma = fieldSep
	fields, err = r.Read()

	return fields, nil

}

func (logSeeker *LogSeeker) getFieldsByRegex(fieldSep rune, content string) (fields []string, err error) {

	r := regexp.MustCompile(`[^\s"']+|"([^"]*)"|'([^']*)`)

	// fmt.Printf("ddd %v\n", r.FindAllStringSubmatch(content, -1)[1])
	fields = r.FindAllString(content, -1)

	// fields = strings.Split(content, " ")
	return fields, nil

}

//SeekLinePosition find the  date field >= beginDate line position
func (logSeeker *LogSeeker) SeekLinePosition(pos int64) (offset int64, err error) {
	if pos == 0 {
		return 0, nil
	}

	offset, err = logSeeker.file.Seek(pos, os.SEEK_SET)

	if err != nil {
		return offset, err
	}

	lineSep := byte('\n')
	buf := make([]byte, 1)

	_, err = logSeeker.file.Read(buf)

	if err != nil && err != io.EOF {
		return 0, err
	}

	if buf[0] == lineSep {
		offset, err = logSeeker.Tell()
		return
	}

	offset, err = logSeeker.Tell()

	var stepSize int64 = 1024

	seekPos := stepSize
	found := false
	i := 0
	for {

		found = false

		if offset == 0 {
			break
		}

		if offset <= stepSize {
			seekPos = offset
		} else {
			seekPos = stepSize
		}

		// fmt.Printf("before Seek pos: %d\n", offset)

		offset, err = logSeeker.file.Seek(seekPos*-1, os.SEEK_CUR) // get left chars
		// fmt.Printf("before ReadAt pos: %d\n", offset)
		if err != nil {
			break
		}

		buf = make([]byte, seekPos)

		realSize, err := logSeeker.file.Read(buf)

		if err != nil {
			break
		}

		i = realSize - 1
		for ; i >= 0; i-- {
			if buf[i] == lineSep {
				found = true
				break
			}
		}

		if found {

			// fmt.Printf("Tell pos 1: %d,%d\n", offset, i)

			offset, err = logSeeker.file.Seek(int64(i-realSize+1), os.SEEK_CUR) //fallback
			// fmt.Printf("last pos: %d\n", offset)

			break
		}

	}

	return
}

//BSearchBegin search the begin pos
func (logSeeker *LogSeeker) BSearchBegin(begin int64, end int64, startValue string, fieldSep rune, fieldIndex int) (offset int64, err error) {

	if begin > end {
		//not found
		return -1, nil
	}

	offset, err = logSeeker.SeekLinePosition(begin)

	field, err := logSeeker.readLineField(offset, fieldSep, fieldIndex)

	if startValue < field {
		//found
		return 0, nil
	}

	offset, err = logSeeker.SeekLinePosition(end - 2)

	field, err = logSeeker.readLineField(offset, fieldSep, fieldIndex)

	// fmt.Printf("scan end  %d-%d ,%s %d\n", end, offset, field, fieldIndex)

	if startValue > field {
		//not found
		return -1, nil
	}

	mid := (begin + end) / 2

	var lastOffset int64 = -1

	for end > begin {

		offset, err = logSeeker.SeekLinePosition(mid)
		// fmt.Printf("scan %d \n", offset)
		if lastOffset > 0 && lastOffset == offset {
			// repeat find the same row
			break
		}

		field, err = logSeeker.readLineField(offset, fieldSep, fieldIndex)
		// fmt.Printf("scan begin %d, %d mid:%d\n", begin, end, mid)

		if field >= startValue && offset == begin {
			return
		}

		if offset == begin {
			offset = lastOffset
			return
		}

		if field >= startValue {
			lastOffset = offset
			end = mid
		} else {
			begin = mid + 1
		}

		mid = (begin + end) / 2

	}
	return lastOffset, nil
}

//BSearchEnd search the end pos
func (logSeeker *LogSeeker) BSearchEnd(begin int64, end int64, endValue string, fieldSep rune, fieldIndex int) (offset int64, err error) {

	if begin > end {
		//not found
		return -1, nil
	}

	offset, err = logSeeker.SeekLinePosition(end - 2)

	field, err := logSeeker.readLineField(offset, fieldSep, fieldIndex)

	// fmt.Printf("scan end  %d-%d ,%s %d\n", end, offset, field, fieldIndex)

	if endValue > field {
		//found
		return end, nil
	}

	mid := (begin + end) / 2

	var lastOffset int64 = -1

	for end > begin {

		offset, err = logSeeker.SeekLinePosition(mid)
		// fmt.Printf("scan %d \n", offset)
		if lastOffset > 0 && lastOffset == offset {
			// repeat find the same row
			break
		}

		field, err = logSeeker.readLineField(offset, fieldSep, fieldIndex)
		// fmt.Printf("scan begin %d, %d mid:%d\n", begin, end, mid)

		if field < endValue && offset == begin {
			return
		}

		if offset == begin {
			offset = lastOffset
			return
		}

		if field >= endValue {
			lastOffset = offset
			end = mid
		} else {
			begin = mid + 1
		}

		mid = (begin + end) / 2

	}
	return lastOffset, nil
}

func (logSeeker *LogSeeker) readLineField(offset int64, fieldSep rune, fieldIndex int) (field string, err error) {
	originPos, err := logSeeker.Tell()
	defer func() {
		logSeeker.reader = nil
		logSeeker.Seek(originPos, os.SEEK_SET)
	}()

	logSeeker.Seek(offset, os.SEEK_SET)
	logSeeker.BeginReader()
	content, err := logSeeker.reader.ReadString(byte('\n'))
	// fmt.Printf("readline: %s", content)
	fields, err := logSeeker.getFields(fieldSep, content)

	if len(fields) >= fieldIndex && fieldIndex > 0 {
		return fields[fieldIndex-1], nil
	}
	if fieldIndex <= 0 && len(fields) >= fieldIndex*-1+1 {
		return fields[len(fields)-fieldIndex*-1-1], nil
	}
	return "", nil
}

//printRangeLines print lines
func printRangeLines(logSeeker *LogSeeker, filedSeperator rune, fieldIndex int, startOffset int64, endOffset int64, done chan bool) {

	f := bufio.NewWriterSize(os.Stdout, 1024*20)

	defer func() {
		f.Flush()
		done <- true
	}()

	for {

		bytes, err := logSeeker.reader.ReadBytes(byte('\n'))
		if err != nil {
			break
		}

		startOffset += int64(len(bytes))

		if startOffset > endOffset {
			break
		}
		f.Write(bytes)
	}
}

func main() {

	startValue := flag.String("s", "", "start value")
	endValue := flag.String("e", "", "end value")
	fieldSep := flag.String("f", " ", "field separator")
	fieldIndex := flag.Int("n", 1, "field index")
	flag.Parse()

	if *startValue == "" || *endValue == "" {
		flag.Usage()
		os.Exit(1)
	}

	args := flag.Args()

	if len(args) != 1 {
		flag.Usage()
		os.Exit(1)
	}

	filedSeperator := []rune(*fieldSep)[0]
	logSeeker, err := New(args[0])
	if err != nil {
		panic(err)
	}
	// fmt.Printf(" options: %s - %d\n", *fieldSep, *fieldIndex)

	end, _ := logSeeker.file.Seek(0, os.SEEK_END)

	offset, _ := logSeeker.BSearchBegin(0, end, *startValue, filedSeperator, *fieldIndex)

	endOffset, _ := logSeeker.BSearchEnd(offset, end, *endValue, filedSeperator, *fieldIndex)

	logSeeker.Seek(offset, os.SEEK_SET)

	logSeeker.BeginReaderSize(20 * 1024)

	done := make(chan bool)
	go printRangeLines(logSeeker, filedSeperator, *fieldIndex, offset, endOffset, done)

	<-done
	os.Exit(0)
}
