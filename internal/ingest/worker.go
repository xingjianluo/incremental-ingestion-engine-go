package ingest

import (
	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/storage"
)

// ProcessResult 是单条事件的处理结果。
type ProcessResult struct {
	Event  protocol.ChangeEvent
	OK     bool
	Reason protocol.RejectReason
	Detail string
	Action string // 落地动作：inserted / updated / deleted / duplicate-skip
}

// Processor 处理单条事件（校验 + 落地/拒绝），实现需保证幂等与并发安全。
// plan 是引擎在并发前按 seq 顺序算出的批次预解析（批内冲突/重投判定），
// 使主键冲突结果确定、不依赖 worker 调度顺序。
type Processor interface {
	Process(ev protocol.ChangeEvent, plan storage.BatchPlan) ProcessResult
}

// RunWorkers 并发处理一批事件：worker 只做无顺序依赖的落地处理（目标端/日志内部
// 各自加锁且按 Seq 幂等），位点提交不由 worker 负责，而由引擎按 Seq 顺序串行执行，
// 从而在并发消费下保证已提交位点严格单调。
// 返回的结果按 Seq 升序排列，便于引擎顺序提交。
func RunWorkers(p Processor, events []protocol.ChangeEvent, pks []string,
	plan storage.BatchPlan, concurrency int) []ProcessResult {
	n := len(events)
	results := make([]ProcessResult, n)
	if n == 0 {
		return results
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > n {
		concurrency = n
	}

	jobs := make(chan int)
	done := make(chan struct{}, concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for idx := range jobs {
				// 结果写回各自的槽位，无需额外加锁。
				results[idx] = p.Process(events[idx], plan)
			}
		}()
	}
	for i := range events {
		jobs <- i
	}
	close(jobs)
	for w := 0; w < concurrency; w++ {
		<-done
	}

	// events 由调用方（引擎）保证按 Seq 升序读取；此处再做一次稳定排序以防万一。
	for i := 1; i < n; i++ {
		for j := i; j > 0 && results[j-1].Event.Seq > results[j].Event.Seq; j-- {
			results[j-1], results[j] = results[j], results[j-1]
		}
	}
	return results
}
