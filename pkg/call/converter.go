package call

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// 参数 / 返回值 ↔ JSON 编解码约定（跨语言边界只传 string，gomobile 硬约束）：
//
//   - argsJSON 入参：JSON 数组，按方法签名（ctx 之后）的位置一一对应。
//     例：UserMgr.GetUsersInfo(ctx, userIDs []string)  →  '[["u1","u2"]]'
//     例：MsgMgr.SendMsg(ctx, msg *MsgData)             →  '[{"sendID":"u1",...}]'
//     例：MsgMgr.NewestSeq(ctx)                          →  '[]' 或 ''
//   - 返回值：0 个非 error 返回值 → ""；1 个 → 该值的 JSON；多个 → JSON 数组按序排列。
//     例：MsgMgr.NewestSeq 返回 (maxSeqs, minSeqs, err) → '[{...},{...}]'
//
// 反射调用内核靠位置对齐参数（Go 反射拿不到参数名），调用方务必按此约定传参。

var (
	errorType = reflect.TypeOf((*error)(nil)).Elem()
	ctxType   = reflect.TypeOf((*context.Context)(nil)).Elem()
)

// decodeArgs 把 argsJSON（JSON 数组）解码为反射调用参数列表
//
// mt 是被调方法的 Type（MethodByName 取到的 Func 类型，不再含接收者）。
// 约定第一个参数是 context.Context，由调用方注入（ctx），不从 argsJSON 消费。
func decodeArgs(argsJSON string, mt reflect.Type, ctx context.Context) ([]reflect.Value, error) {
	// 收集需要从 JSON 消费的参数（跳过 context.Context）
	var paramTypes []reflect.Type
	for i := 0; i < mt.NumIn(); i++ {
		pt := mt.In(i)
		if pt == ctxType {
			continue
		}
		paramTypes = append(paramTypes, pt)
	}

	if argsJSON == "" {
		argsJSON = "[]"
	}
	var raw []json.RawMessage
	if err := json.Unmarshal([]byte(argsJSON), &raw); err != nil {
		return nil, fmt.Errorf("args must be a JSON array: %w", err)
	}
	if len(raw) != len(paramTypes) {
		return nil, fmt.Errorf("args count mismatch: method needs %d, got %d", len(paramTypes), len(raw))
	}

	// argsJSON = '[{"groupID":"g1","groupName":"测试"},["u1"],["u1"],"u1",true]'
	// i=0: ctx → args=[ctx]
	// i=1: pt=*model.GroupInfo, Kind=Ptr
	//      pv = reflect.New(model.GroupInfo) → *GroupInfo（正是签名要的类型）
	//      Unmarshal(第一个元素, pv.Interface()) → 填充结构体
	//      append(pv)          ← 直接 append 指针
	// i=2: []string → New+Unmarshal+Elem → 值
	// i=3: []string → 同上
	// i=4: string   → New+Unmarshal+Elem → "u1"
	// i=5: bool     → New+Unmarshal+Elem → true
	args := make([]reflect.Value, 0, mt.NumIn())
	argIdx := 0
	for i := 0; i < mt.NumIn(); i++ {
		pt := mt.In(i)
		if pt == ctxType {
			args = append(args, reflect.ValueOf(ctx))
			continue
		}
		// 指针参数：New(elem) 出实例再 Unmarshal 进去；值参数同理用 New + Elem
		pv := reflect.New(pt)
		if pt.Kind() == reflect.Ptr {
			pv = reflect.New(pt.Elem())
		}
		if err := json.Unmarshal(raw[argIdx], pv.Interface()); err != nil {
			return nil, fmt.Errorf("arg[%d] decode to %s: %w", argIdx, pt, err)
		}
		if pt.Kind() == reflect.Ptr {
			args = append(args, pv)
		} else {
			args = append(args, pv.Elem())
		}
		argIdx++
	}
	return args, nil
}

// encodeResult 把反射返回值编码为 JSON string
//
// error 返回值在调用方已处理，这里只编码非 error 返回值。
func encodeResult(outs []reflect.Value) (string, error) {
	var results []reflect.Value
	for _, out := range outs {
		if out.Type() == errorType {
			continue
		}
		results = append(results, out)
	}
	switch len(results) {
	case 0:
		return "", nil
	case 1:
		bs, err := json.Marshal(results[0].Interface())
		if err != nil {
			return "", fmt.Errorf("marshal result: %w", err)
		}
		return string(bs), nil
	default:
		arr := make([]interface{}, 0, len(results))
		for _, r := range results {
			arr = append(arr, r.Interface())
		}
		bs, err := json.Marshal(arr)
		if err != nil {
			return "", fmt.Errorf("marshal results: %w", err)
		}
		return string(bs), nil
	}
}
