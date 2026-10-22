package filex

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func DirExist(dir string) bool {
	s, err := os.Stat(dir)
	if err != nil {
		return false
	}
	return s.IsDir()
}

// CreateDir 创建文件夹
func CreateDir(dirName string) bool {
	err := os.MkdirAll(dirName, 0755)
	return err == nil
}

// 删除文件（支持带通配符）
func DeleteFiles(regString string) error {
	files, err := filepath.Glob(regString)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return err
		}
	}
	return nil
}

// 获取文件夹内各文件的文件名
func GetFolderSubFileName(path string) (fileNames []string, err error) {
	dirList, err := os.ReadDir(path)
	if err != nil {
		return
	}
	for _, v := range dirList {
		fileNames = append(fileNames, v.Name())
	}
	return
}

func GetCurrentDirectory() string {
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	return strings.Replace(dir, "\\", "/", -1)
}

func GetRootDir() string {
	file, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		file = fmt.Sprintf(".%s", string(os.PathSeparator))
	} else {
		file = fmt.Sprintf("%s%s", file, string(os.PathSeparator))
	}
	return file
}

func GetExecFilePath() string {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		file = fmt.Sprintf(".%s", string(os.PathSeparator))
	} else {
		file, _ = filepath.Abs(file)
	}
	return file
}
