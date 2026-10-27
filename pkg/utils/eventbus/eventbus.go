// Package eventbus 通用的 channel-based pub/sub 组件。
//
// 设计目标：
//  1. 零业务依赖 — key / payload 都是 interface{}，谁都能拿来用
//  2. 非阻塞派发 — channel 满了直接丢掉，避免 dispatch 被慢订阅者拖死
//  3. goroutine-safe — 全用 sync.RWMutex 保护
//  4. gomobile 友好 — 不泛型、不 channel 类型暴露给外部
//
// 用法：
//
//	bus := eventbus.New()
//	ch, unsubscribe := bus.Subscribe("user-login")
//	go for ev := range ch { fmt.Println(ev) }()
//	bus.Publish("user-login", userInfo)
//	unsubscribe()
//
// 注意：EventBus 只负责「可靠投递到 channel」，不负责持久化、不负责补偿。
// 订阅者自己要保证消费速度 ≥ 生产速度，否则消息会被丢弃。
package eventbus

import "sync"

const defaultBufferSize = 64

// AllKey 通配订阅 key：Subscribe(AllKey) 可收到所有 Publish 的事件
// （Publish 时除派发给精确 key 订阅者外，会额外派发给 AllKey 订阅者）
const AllKey = "__all__"

// UnsubscribeFunc 取消订阅函数
type UnsubscribeFunc func()

// Logger 可选的日志接口，EventBus 不直接依赖任何日志库
type Logger interface {
	Warnf(format string, args ...interface{})
}

// EventBus 通用事件总线，key 和 payload 都是 interface{}
//
// 用 interface{} 而不是泛型，因为 gomobile 不支持泛型跨语言编译。
// 调用方自己保证类型安全（通常做法：用具体类型做 key + 在 Subscribe 后用类型断言消费）。
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[interface{}][]chan interface{}
	logger      Logger
	bufferSize  int
}

// New 创建 EventBus，bufferSize 默认 64
func New(opts ...Option) *EventBus {
	b := &EventBus{
		subscribers: make(map[interface{}][]chan interface{}),
		bufferSize:  defaultBufferSize,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Option EventBus 可选配置
type Option func(*EventBus)

// WithLogger 设置日志（用于 channel 满时的 warn）
func WithLogger(l Logger) Option {
	return func(b *EventBus) { b.logger = l }
}

// WithBufferSize 覆盖默认 channel buffer size
func WithBufferSize(n int) Option {
	return func(b *EventBus) {
		if n > 0 {
			b.bufferSize = n
		}
	}
}

// Subscribe 订阅指定 key 的事件
//
// 返回值：
//   - <-chan interface{}：事件接收通道（buffer 由 EventBus 配置）
//   - UnsubscribeFunc：取消订阅，幂等，可多次调用
//
// 订阅者必须自己消费 channel，否则 buffer 满了后续 Publish 会丢弃并打 warn。
func (b *EventBus) Subscribe(key interface{}) (<-chan interface{}, UnsubscribeFunc) {
	ch := make(chan interface{}, b.bufferSize)

	b.mu.Lock()
	b.subscribers[key] = append(b.subscribers[key], ch)
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if subs, ok := b.subscribers[key]; ok {
			for i, c := range subs {
				if c == ch {
					b.subscribers[key] = append(subs[:i], subs[i+1:]...)
					close(c)
					break
				}
			}
			if len(b.subscribers[key]) == 0 {
				delete(b.subscribers, key)
			}
		}
	}

	return ch, unsubscribe
}

// Publish 发布事件到所有订阅者（非阻塞）
//
// 如果某个订阅者 channel 满了，消息会被丢弃并打 warn。
// Publish 不会阻塞调用方。
func (b *EventBus) Publish(key interface{}, payload interface{}) {
	b.mu.RLock()
	// 显式拷贝（不能 append 到 map 持有的 slice，并发 Publish 会写同一底层数组）
	allSubs := b.subscribers[AllKey]
	targets := make([]chan interface{}, 0, len(b.subscribers[key])+len(allSubs))
	targets = append(targets, b.subscribers[key]...)
	// 合并通配订阅者（key 自身是 AllKey 时不能重复派发）
	if key != AllKey {
		targets = append(targets, allSubs...)
	}
	b.mu.RUnlock()

	for _, ch := range targets {
		select {
		case ch <- payload:
		default:
			if b.logger != nil {
				b.logger.Warnf("eventbus channel full, dropping, key=%v", key)
			}
		}
	}
}

// HasSubscribers 某个 key 是否有订阅者（可用来跳过空 Publish 的开销）
func (b *EventBus) HasSubscribers(key interface{}) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers[key]) > 0
}

// Close 关闭所有订阅者 channel 并清空 map
//
// 调用后 EventBus 不可再使用，对已订阅者会收到 channel close 信号。
func (b *EventBus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for key, subs := range b.subscribers {
		for _, ch := range subs {
			close(ch)
		}
		delete(b.subscribers, key)
	}
}
