package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jpillora/backoff"
	"golang.org/x/net/context"
)

// Backoff for fetching. It starts by waiting the minimum duration after a
// failed fetch, doubling it each time (with a bitter of jitter) up to max
// duration between requests.
var defaultBackoff = backoff.Backoff{
	Min:    1 * time.Second,
	Max:    5 * time.Minute,
	Jitter: true,
	Factor: 2,
}

type Worker struct {
	query   *Query
	payload []byte
	client  *http.Client
	result  *QueryResult
	log     *log.Logger
	backoff backoff.Backoff
	ctx     context.Context
}

// 04. 设置监控指标
func (w *Worker) SetMetrics(recs records) {
	list, err := w.result.SetMetrics(recs)
	if err != nil {
		w.log.Printf("Error setting metrics: %s", err)
		return
	}

	// 该方法用来实现真正注册监控指标项逻辑
	w.result.RegisterMetrics(list)
}

// 03. 通过HTTP方式查询数据库数据，由SqlAgent服务实现
func (w *Worker) Fetch(url string) (records, error) {
	var (
		t    time.Time
		err  error
		req  *http.Request
		resp *http.Response
	)

	for {
		t = time.Now()

		// 1) 构造POST请求，以JSON方式发送SQL等相关参数
		req, err = http.NewRequest("POST", url, bytes.NewBuffer(w.payload))

		if err != nil {
			panic(err)
		}
		// 2) 绑定请求上下文（Worker）
		req = req.WithContext(w.ctx)

		// Set the content-type of the request body and accept LD-JSON.
		req.Header.Set("content-type", "application/json")
		req.Header.Set("accept", "application/json")

		// 3) 执行请求
		resp, err = w.client.Do(req)

		// No formal error, but a non-successful status code. Construct an error.
		if err == nil && resp.StatusCode != 200 {
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			err = fmt.Errorf("%s: %s", resp.Status, string(b))
		}

		// No error, break to read the data.
		// 4) 循环重试，如果获取成功则退出循环
		if err == nil {
			break
		}

		// 5) 请求失败计数监控
		if w.query.ValueOnError != "" {
			w.SetMetrics([]record{
				map[string]interface{}{
					"error": w.query.ValueOnError,
				},
			})
		}

		// Backoff on an error.
		w.log.Print(err)
		d := w.backoff.Duration()
		w.log.Printf("Backing off for %s", d)
		// 6) 这里控制退出的逻辑是怎样的（总不能失败了会一直重试吧）？
		select {
		case <-time.After(d):
			continue
		case <-w.ctx.Done():
			return nil, errors.New("Execution was canceled")
		}
	}

	w.backoff.Reset()

	w.log.Printf("Fetch took %s", time.Now().Sub(t))

	var recs []record

	// 7) 函数退出前关闭请求消息体
	defer resp.Body.Close()

	// 8) 解码响应消息体
	if err = json.NewDecoder(resp.Body).Decode(&recs); err != nil {
		return nil, err
	}

	// 将结果写入监控指标
	w.SetMetrics(recs)

	return recs, nil
}

// 02. 启动Worker（协程方法）
func (w *Worker) Start(url string) {
	// 1) 声明tick函数，该函数内部通过HTTP访问数据库，获取查询结果
	tick := func() {
		_, err := w.Fetch(url)
		if err != nil {
			w.log.Printf("Error fetching records: %s", err)
			return
		}
	}

	// 2) 启动时调用一次tick函数
	tick()
	// 3) 启动一个计时器（受配置中的间隔时间控制）
	ticker := time.NewTicker(w.query.Interval)

	// 4) 死循环进行请求轮循（通过计时器控制间隔）
	for {
		select {
		case <-w.ctx.Done():
			// 5) 当上下文发出 Done 信号时，停止
			wg, _ := w.ctx.Value("wg").(*sync.WaitGroup)
			wg.Done()
			w.log.Printf("Stopping worker")
			return

		case <-ticker.C:
			// 6) 当计时器到时间时，调用tick函数
			tick()
		}
	}
}

// 01. 初始化Worker对象
// NewWorker creates a new worker for a query.
func NewWorker(ctx context.Context, q *Query) *Worker {
	// Encode the payload once for all subsequent requests.
	// 1) 将SQL查询请求参数编码为JSON，做为请求消息体使用（由SqlAgent提供服务）
	payload, err := json.Marshal(map[string]interface{}{
		"driver":     q.Driver,
		"connection": q.Connection,
		"sql":        q.SQL,
		"params":     q.Params,
	})

	if err != nil {
		panic(err)
	}

	// 2) 通过配置查询参数构建Worker对象
	return &Worker{
		query:   q,
		result:  NewQueryResult(q),
		payload: payload,
		backoff: defaultBackoff,
		log:     log.New(os.Stderr, fmt.Sprintf("[%s] ", q.Name), log.LstdFlags),
		client: &http.Client{
			Timeout: q.Timeout,
		},
		ctx: ctx,
	}
}
