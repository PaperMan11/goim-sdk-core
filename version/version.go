// Package version 提供 SDK 版本号的单一事实来源（Single Source of Truth）。
//
// 权威定义是同目录下的 VERSION 文件：
//   - Go 代码经 go:embed 在编译期注入（本文件）
//   - CI/发布脚本可直接读取该文件做 git tag、产物命名等（无需解析 Go 代码）
//
// 编译期仍可用 ldflags 覆盖下游包的 Version 变量实现发布时打标。
package version

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var versionFile string

// Version SDK 版本号（已去除文件尾部换行符）
var Version = strings.TrimSpace(versionFile)
