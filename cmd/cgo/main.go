// Package main PC 桌面平台 cgo 导出入口。
//
// 构建：
//
//	Windows: CGO_ENABLED=1 GOOS=windows GOARCH=amd64 go build -buildmode=c-shared -o goim-sdk.dll ./cmd/cgo
//	macOS:   CGO_ENABLED=1 GOOS=darwin  GOARCH=amd64 go build -buildmode=c-shared -o goim-sdk.dylib ./cmd/cgo
//
// 全部 C ABI 导出函数在 export.go（//go:build cgo）；
// 本文件仅提供 func main()（-buildmode=c-shared 要求 main 包）。
package main

func main() {}
