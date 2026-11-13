# goim-sdk-core 构建入口
#
# 依赖（按目标）：
#   - wasm           : Go 1.26+（自动携带 wasm_exec.js）
#   - cgo            : C 编译器（Windows 需 gcc / mingw；macOS 需 Xcode CLT）
#   - ios / android  : gomobile 工具链（go install golang.org/x/mobile/cmd/gomobile@latest && gomobile init）
#
# 用法：make <target>（需要 POSIX shell：bash / Git Bash / WSL）
#
#   make wasm        # Web：cmd/wasm/dist/goim.wasm + wasm_exec.js
#   make cgo         # PC：dist/goim-sdk.dll|.dylib|.so（按宿主平台）
#   make ios         # iOS：dist/goim-sdk.framework（需 macOS + Xcode）
#   make android     # Android：dist/goim-sdk.aar（需 Android SDK/NDK）
#   make mobile      # 上面两个
#   make all         # wasm + cgo（默认）
#   make clean

.DEFAULT_GOAL := all

MODULE  := github.com/PaperMan11/goim-sdk-core
VERSION := $(shell cat version/VERSION)
LDFLAGS := -X $(MODULE)/pkg/client.Version=$(VERSION)

DIST      := dist
WASM_DIR  := cmd/wasm/dist
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
	@cp "$(WASM_EXEC)" $(WASM_DIR)/wasm_exec.js
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
	rm -rf $(DIST) $(WASM_DIR)/goim.wasm $(WASM_DIR)/wasm_exec.js
