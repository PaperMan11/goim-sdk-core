# goim-sdk-core 构建入口
#
# 依赖（按目标）：
#   - wasm           : Go 1.26+（自动携带 wasm_exec.js）
#   - cgo            : C 编译器（Windows 需 gcc / mingw；macOS 需 Xcode CLT）
#   - ios / android  : gomobile 工具链（go install golang.org/x/mobile/cmd/gomobile@latest && gomobile init）
#
# 用法：make <target>（需要 POSIX shell：bash / Git Bash / WSL）
#
#   make wasm        # Web：dist/goim.wasm + dist/wasm_exec.js
#   make cgo         # PC：dist/goim-sdk.dll|.dylib|.so（按宿主平台）
#   make ios         # iOS：dist/goim-sdk.framework（需 macOS + Xcode）
#   make android     # Android：dist/goim-sdk.aar（需 Android SDK/NDK）
#   make mobile      # 上面两个
#   make all         # wasm + cgo（默认）
#
# 所有产物统一输出到 dist/

.DEFAULT_GOAL := all

# Windows 下 GNU make 默认用 cmd 执行 recipe，而本文件使用 POSIX 命令（mkdir -p / cp / rm）。
# 这里以 bash 是否存在来判定 Windows 环境（make 从 Git Bash 启动时甚至看不到 OS 变量）。
POSIX_BASH := $(firstword $(wildcard C:/msys64/usr/bin/bash.exe C:/PROGRA~1/Git/bin/bash.exe C:/PROGRA~2/Git/bin/bash.exe))
PWSH       := $(firstword $(wildcard C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe C:/PROGRA~1/PowerShell/7/pwsh.exe))
ifneq ($(POSIX_BASH),)
SHELL := $(POSIX_BASH)
.SHELLFLAGS := -c
endif

# 从 Git Bash 启动 MSYS2 的 make 时，USERPROFILE/GOPATH/GOCACHE 等 Windows 环境变量会丢失，
# Go 将报 "module cache not found: neither GOMODCACHE nor GOPATH is set"。
# 以 recipe shell 中实际可见的值为准，缺失时通过 PowerShell 系统 API 取回用户目录兜底。
ifneq ($(PWSH),)
SHELL_USERPROFILE := $(shell echo $$USERPROFILE)
SHELL_GOPATH      := $(shell echo $$GOPATH)
ifeq ($(strip $(SHELL_USERPROFILE)$(SHELL_GOPATH)),)
WIN_HOME  := $(shell "$(PWSH)" -NoProfile -Command "[Environment]::GetFolderPath('UserProfile')" 2>/dev/null | tr -d '\r')
WIN_CACHE := $(shell "$(PWSH)" -NoProfile -Command "[Environment]::GetFolderPath('LocalApplicationData')" 2>/dev/null | tr -d '\r')
ifneq ($(WIN_HOME),)
export USERPROFILE := $(WIN_HOME)
export GOPATH      := $(WIN_HOME)/go
export GOMODCACHE  := $(WIN_HOME)/go/pkg/mod
export GOCACHE     := $(WIN_CACHE)/go-build
endif
endif
endif

# 若本机装了 MSYS2 的 gcc，自动加入 PATH：Git Bash 会话的 PATH 可能没刷新到
# C:\msys64\ucrt64\bin，cgo 会以 "cgo.exe: exit status 2" 失败。
MSYS_GCC_BIN := $(firstword $(wildcard C:/msys64/ucrt64/bin C:/msys64/mingw64/bin))
ifneq ($(MSYS_GCC_BIN),)
MSYS_GCC_POSIX := /c/$(patsubst C:/msys64/%,msys64/%,$(MSYS_GCC_BIN))
export PATH := $(MSYS_GCC_POSIX):$(PATH)
endif

MODULE  := github.com/PaperMan11/goim-sdk-core
VERSION := $(shell cat version/VERSION)
LDFLAGS := -X $(MODULE)/pkg/client.Version=$(VERSION)

DIST      := dist
WASM_DIR  := $(DIST)
# Windows 下 go env 返回反斜杠路径，统一转成 / 供 wildcard / cp 使用
GOROOT    := $(subst \,/,$(shell go env GOROOT))
# Go 1.26 起 wasm_exec.js 从 misc/wasm 迁到 lib/wasm，兼容旧版本
WASM_EXEC := $(if $(wildcard $(GOROOT)/lib/wasm/wasm_exec.js),$(GOROOT)/lib/wasm/wasm_exec.js,$(GOROOT)/misc/wasm/wasm_exec.js)

HOST_OS := $(shell go env GOOS)
ifeq ($(HOST_OS),windows)
CGO_OUT := $(DIST)/goim-sdk.dll
else ifeq ($(HOST_OS),darwin)
CGO_OUT := $(DIST)/goim-sdk.dylib
else
CGO_OUT := $(DIST)/goim-sdk.so
endif

.PHONY: all wasm cgo ios android mobile check-gomobile clean

all: wasm cgo ## Web + PC 动态库

wasm: ## Web：goim.wasm + wasm_exec.js
	@mkdir -p $(WASM_DIR)
	GOOS=js GOARCH=wasm go build -trimpath -ldflags "$(LDFLAGS)" -o $(WASM_DIR)/goim.wasm ./cmd/wasm
	@cp -f "$(WASM_EXEC)" $(WASM_DIR)/wasm_exec.js
	@echo "built: $(WASM_DIR)/goim.wasm (+ wasm_exec.js)"

cgo: ## PC：C ABI 动态库（按宿主平台命名）
	@mkdir -p $(DIST)
	CGO_ENABLED=1 go build -trimpath -ldflags "$(LDFLAGS)" -buildmode=c-shared -o $(CGO_OUT) ./cmd/cgo
	@echo "built: $(CGO_OUT) (+ 同名 .h)"

ios: check-gomobile ## iOS：.framework（需 macOS + Xcode）
	@mkdir -p $(DIST)
	gomobile bind -target=ios -o $(DIST)/goim-sdk.framework ./pkg/call
	@echo "built: $(DIST)/goim-sdk.framework"

android: check-gomobile ## Android：.aar（需 Android SDK/NDK）
	@mkdir -p $(DIST)
	gomobile bind -target=android -o $(DIST)/goim-sdk.aar ./pkg/call
	@echo "built: $(DIST)/goim-sdk.aar"

mobile: ios android ## gomobile 双平台

check-gomobile:
	@command -v gomobile >/dev/null 2>&1 || { \
		echo "gomobile not found. install with:"; \
		echo "  go install golang.org/x/mobile/cmd/gomobile@latest"; \
		echo "  gomobile init"; \
		exit 1; \
	}

clean: ## 清理构建产物
	rm -rf $(DIST) cmd/wasm/dist
