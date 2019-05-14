package reader

import (
	"bufio"
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/log"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
)

const (
	EType = iota
	EContent
)

// 是否十六进制字符
func IsHexNum(str string) bool {
	for _, ch := range str {
		if !(ch >= '0' && ch <= '9') &&
			!(ch >= 'a' && ch <= 'f') &&
			!(ch >= 'A' && ch <= 'F') {
			return false
		}
	}
	return true
}

// 是否idfa
func IsIdfa(str string) bool {
	for _, ch := range str {
		if !(ch >= '0' && ch <= '9') &&
			!(ch >= 'a' && ch <= 'f') &&
			!(ch >= 'A' && ch <= 'F') &&
			!(ch == '-' || ch == '_') {
			return false
		}
	}
	return true
}

//检查文件大小, 限制100m 以下
func CheckFileSize(fileName string, maxsize int64) (bool, error) {
	fi, err := os.Stat(fileName)
	if err != nil {
		log.Debug("get file stat fail")
		return false, err
	}

	if fi.Size() > maxsize {
		log.Debug("file oversize")
		return false, err
	}
	return true, nil
}

func Inslice(key int, slice ...int) bool {
	if len(slice) == 0 {
		return false
	}
	for _, i := range slice {
		if key == i {
			return true
		}

	}
	return false
}

func IsNum(str string) bool {
	for _, c := range str {
		if !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func eTypeCheck(etype int) bool {
	if etype == env.PC || etype == env.Mob {
		return true
	}
	return false
}

// 检查文件格式，返回总行数，错误行数，有效行数
//空行不计入总数
func FileLines(etype int, r io.Reader) (allLine, falseLine, trueLine int64, err error) {
	if !eTypeCheck(etype) {
		return 0, 0, 0, errors.New("unvalid type")
	}
	input := bufio.NewScanner(r)
	for input.Scan() {
		line := strings.Fields(input.Text())
		if len(line) > 0 {
			allLine++
			if etype == env.Mob {
				if len(line) == (EContent + 1) {
					switch line[EType] {
					case env.Android_id:
						if len(line[EContent]) != env.Android_id_len {
							falseLine++
						}

					case env.Idfa:
						if len(line[EContent]) != env.Idfa_len {
							falseLine++
						}

					case env.Imei:
						if len(line[EContent]) != env.Imei_len {
							falseLine++
						}

					case env.Mandroid_id:
						if len(line[EContent]) != env.Mandroid_id_len {
							falseLine++

						}
					case env.Sandroid_id:
						if len(line[EContent]) != env.Sandroid_id_len {
							falseLine++
						}

					case env.Midfa:
						if len(line[EContent]) != env.Midfa_len {
							falseLine++
						}
					case env.Sidfa:
						if len(line[EContent]) != env.Sidfa_len {
							falseLine++
						}
					case env.Mimei:
						if len(line[EContent]) != env.Mimei_len {
							falseLine++
						}
					case env.Simei:
						if len(line[EContent]) != env.Simei_len {
							falseLine++
						}
					default:
						falseLine++
					}
				}
			}
		}
	}
	if err = input.Err(); err != nil {
		return
	}
	trueLine = allLine - falseLine
	return
}

//去除重复行，返回行数统计
func UnixLine(r io.ReadCloser, w io.WriteCloser, equipmentType int, mobSubType string) (err error) {
	mobSubType = fixType(mobSubType)
	if r == nil || w == nil {
		return fmt.Errorf("no inFile or outFile, inFile is %v , outFile is %v", r, w)
	}
	typeLen, ok := env.TypeMap[mobSubType]
	if !ok {
		return errors.New("not valid etype")
	}

	seperator := "\t"
	counts := make(map[string]struct{})

	defer r.Close()

	input := bufio.NewScanner(r)

	for input.Scan() {
		if _, ok := counts[input.Text()]; !ok {
			counts[input.Text()] = struct{}{}
		}
	}

	if err := input.Err(); err != nil {
		return errors.New(err.Error())
	}

	defer w.Close()

	bufWritter := bufio.NewWriter(w)
	for line, _ := range counts {
		if equipmentType == env.Mob {
			words := strings.Fields(line)
			if len(words) == 2 {
				switch words[EType] {
				case env.Android_id:
					if len(words[EContent]) != env.Android_id_len || !IsHexNum(words[EContent]) {
						continue
					}

				case env.Idfa:
					if len(words[EContent]) != env.Idfa_len || !IsIdfa(words[EContent]) {
						continue
					}

				case env.Imei:
					if len(words[EContent]) != env.Imei_len || !IsHexNum(words[EContent]) {
						continue
					}

				case env.Mandroid_id:
					if len(words[EContent]) != env.Mandroid_id_len || !IsHexNum(words[EContent]) {
						continue

					}
				case env.Sandroid_id:
					if len(words[EContent]) != env.Sandroid_id_len || !IsHexNum(words[EContent]) {
						continue
					}

				case env.Midfa:
					if len(words[EContent]) != env.Midfa_len || !IsHexNum(words[EContent]) {
						continue
					}
				case env.Sidfa:
					if len(words[EContent]) != env.Sidfa_len || !IsHexNum(words[EContent]) {
						continue
					}
				case env.Mimei:
					if len(words[EContent]) != env.Mimei_len || !IsHexNum(words[EContent]) {
						continue
					}
				case env.Simei:
					if len(words[EContent]) != env.Simei_len || !IsHexNum(words[EContent]) {
						continue
					}
				default:
				}
			} else if len(words) == 1 {
				if mobSubType == env.Did {
					if len(words[EType]) == env.Android_id_len {
						words[EType] = env.Android_id + seperator + words[EType]
					} else if len(words[EType]) == env.Idfa_len {
						words[EType] = env.Idfa + seperator + words[EType]
					} else {
						continue
					}
				} else if len(words[EType]) != typeLen {
					continue
				} else {
					words[EType] = mobSubType + seperator + words[EType]
				}
			}
			line = strings.Join(words, "\t")

		}
		_, err := bufWritter.WriteString(line + "\n")
		if err != nil {
			return errors.New(err.Error())
		}
	}
	if err = bufWritter.Flush(); err != nil {
		return errors.New(err.Error())
	}
	return
}

func fixType(mobSubType string) string {
	if mobSubType == "android_id" || mobSubType == "mandroid-id" || mobSubType == "sandroid-id" {
		return strings.Replace(mobSubType, "_", "-", 1)
	}
	return mobSubType

}
