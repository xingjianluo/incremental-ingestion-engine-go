package ingest

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/storage"
)

// RunStats 汇总一轮摄入的处置情况。
type RunStats struct {
	Processed     int                           `json:"processed"`
	Applied       int                           `json:"applied"`
	Rejected      int                           `json:"rejected"`
	DuplicateSkip int                           `json:"duplicate_skip"`
	ByReason      map[protocol.RejectReason]int `json:"by_reason"`
	CommittedFrom int64                         `json:"committed_from"`
	CommittedTo   int64                         `json:"committed_to"`
}

// Engine 串起来源、Schema 注册表、处理器、目标端与位点存储，
// 执行增量摄入。并发只发生在"处理"阶段；位点提交严格按 Seq 串行执行。
type Engine struct {
	source      storage.EventSource
	target      storage.TargetStore
	rejects     storage.RejectLog
	changelog   storage.ChangeLogger
	schemaHist  *storage.SchemaHistory
	registry    *protocol.SchemaRegistry
	offsets     *OffsetStore
	processor   Processor
	batchSize   int
	concurrency int
	logger      *log.Logger
	recoveredAt int64
}

// Config 是引擎配置。
type Config struct {
	BatchSize   int
	Concurrency int
	Logger      *log.Logger
}

// NewEngine 组装引擎；打开时位点文件已经恢复，RecoveredOffset 可读。
func NewEngine(
	src storage.EventSource,
	tgt storage.TargetStore,
	rejects storage.RejectLog,
	cl storage.ChangeLogger,
	hist *storage.SchemaHistory,
	reg *protocol.SchemaRegistry,
	offsets *OffsetStore,
	cfg Config,
) *Engine {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &Engine{
		source:      src,
		target:      tgt,
		rejects:     rejects,
		changelog:   cl,
		schemaHist:  hist,
		registry:    reg,
		offsets:     offsets,
		processor:   NewRecordProcessor(reg, tgt, rejects, cl),
		batchSize:   cfg.BatchSize,
		concurrency: cfg.Concurrency,
		logger:      cfg.Logger,
		recoveredAt: offsets.Committed(),
	}
}

// Processor 返回引擎使用的处理器。
func (e *Engine) Processor() Processor { return e.processor }

// RecoveredOffset 返回启动时恢复出的已提交位点。
func (e *Engine) RecoveredOffset() int64 { return e.recoveredAt }

// RegisterSchema 注册一个结构版本：
//   - 兼容的新增字段：登记后立即对目标端历史行按稳定缺省语义回填，并写结构变更历史；
//   - 不兼容的变化：不登记，写一条 incompatible 的结构历史并原样返回错误，
//     调用方必须显式处理（失败可见、可追溯，绝不静默错列）。
func (e *Engine) RegisterSchema(s *protocol.Schema) error {
	old, _ := e.registry.Latest(s.SourceName)
	from := 0
	if old != nil {
		from = old.Version
	}
	desc, err := protocol.CheckCompatibility(old, s)
	if err != nil {
		_ = e.schemaHist.Append(storage.SchemaChangeEvent{
			Source:      s.SourceName,
			FromVersion: from,
			ToVersion:   s.Version,
			Compatible:  false,
			Description: err.Error(),
			OccurredAt:  time.Now(),
		})
		return err
	}
	if err := e.registry.Register(s); err != nil {
		return err
	}
	// 新增字段回填历史行（首个版本无历史行）。
	backfilled := 0
	if old != nil {
		var newFields []protocol.Field
		oldNames := map[string]bool{}
		for _, f := range old.Fields {
			oldNames[f.Name] = true
		}
		for _, f := range s.Fields {
			if !oldNames[f.Name] {
				newFields = append(newFields, f)
			}
		}
		if len(newFields) > 0 {
			n, berr := e.target.BackfillDefaults(newFields)
			if berr != nil {
				return fmt.Errorf("backfill defaults for schema v%d failed: %w", s.Version, berr)
			}
			backfilled = n
		}
	}
	if err := e.schemaHist.Append(storage.SchemaChangeEvent{
		Source:         s.SourceName,
		FromVersion:    from,
		ToVersion:      s.Version,
		Compatible:     true,
		Description:    desc,
		BackfilledRows: backfilled,
		Schema:         s,
		OccurredAt:     time.Now(),
	}); err != nil {
		return err
	}
	e.logger.Printf("[schema] source=%s v%d -> v%d compatible backfilled=%d desc=%q",
		s.SourceName, from, s.Version, backfilled, desc)
	return nil
}

// RunOnce 执行一轮摄入：从已提交位点之后读取、并发处理、按 Seq 顺序提交位点，
// 直到来源暂无更多事件。坏记录同样推进位点（已被显式拒绝留痕，不能重放卡死管道），
// 因此 watermark 始终连续。返回本轮统计。
func (e *Engine) RunOnce() (RunStats, error) {
	stats := RunStats{
		ByReason:      map[protocol.RejectReason]int{},
		CommittedFrom: e.offsets.Committed(),
	}
	for {
		from := e.offsets.Committed() + 1
		events, err := e.source.Read(from, e.batchSize)
		if err != nil {
			return stats, fmt.Errorf("read source from %d: %w", from, err)
		}
		if len(events) == 0 {
			break
		}

		// 并发前先在锁内按 seq 顺序预解析整批：确定性判定重投与批内主键冲突，
		// 使 insert 冲突结果不依赖 worker 调度顺序。
		pks := make([]string, len(events))
		for i, ev := range events {
			if sch, gerr := e.registry.Get(ev.SourceName, ev.SchemaVer); gerr == nil {
				pks[i] = sch.PrimaryKeyValue(ev.Payload)
			}
		}
		plan := e.target.PrepareBatch(events, pks)

		// 并发处理：存储层加锁 + 按 Seq 幂等，允许重复投递。
		results := RunWorkers(e.processor, events, pks, plan, e.concurrency)

		// 串行提交位点：只接受 committed+1。
		for _, r := range results {
			if err := e.offsets.Commit(r.Event.Seq); err != nil {
				if errors.Is(err, ErrOffsetRollback) || errors.Is(err, ErrOffsetGap) {
					// 协议违例：显式失败并立即中止，绝不"从中间继续"。
					e.logger.Printf("[offset] FATAL %v", err)
					return stats, err
				}
				return stats, err
			}
			stats.Processed++
			if r.OK {
				stats.Applied++
				if r.Action == "duplicate-skip" {
					stats.DuplicateSkip++
				}
				e.logger.Printf("[commit] seq=%d op=%s action=%s committed_now=%d",
					r.Event.Seq, r.Event.Op, r.Action, r.Event.Seq)
			} else {
				stats.Rejected++
				stats.ByReason[r.Reason]++
				e.logger.Printf("[commit] seq=%d REJECTED reason=%s detail=%q committed_now=%d",
					r.Event.Seq, r.Reason, r.Detail, r.Event.Seq)
			}
		}

		if len(events) < e.batchSize {
			break
		}
	}
	stats.CommittedTo = e.offsets.Committed()
	e.logger.Printf("[run] processed=%d applied=%d rejected=%d dup_skip=%d watermark %d -> %d commit_order=%v",
		stats.Processed, stats.Applied, stats.Rejected, stats.DuplicateSkip,
		stats.CommittedFrom, stats.CommittedTo, e.offsets.CommitOrder())
	return stats, nil
}
