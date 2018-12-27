package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/context"
	"gopkg.in/tylerb/graceful.v1"
)

func main() {
	log.Println("prometheus-sql starting up...")

	// 01. 声明配置变量
	var (
		host                         string
		port                         int
		service                      string
		queriesFile                  string
		queryDir                     string
		confFile                     string
		tolerateInvalidQueryDirFiles bool
	)

	// 02. flag 库用于输出 Usage 信息，类似于命令提示信息
	flag.StringVar(&host, "host", DefaultHost, "Host of the service.")
	flag.IntVar(&port, "port", DefaultPort, "Port of the service.")
	flag.StringVar(&service, "service", DefaultService, "Query of SQL agent service.")
	flag.StringVar(&queriesFile, "queries", DefaultQueriesFile, "Path to file containing queries.")
	flag.StringVar(&queryDir, "queryDir", DefaultQueriesDir, "Path to directory containing queries.")
	flag.StringVar(&confFile, "config", DefaultConfFile, "Configuration file to define common data sources etc.")
	flag.BoolVar(&tolerateInvalidQueryDirFiles, "lax", DefaultTolerateInvalidQueryDirFiles, "Tolerate invalid files in queryDir")

	flag.Parse()

	// 03. 部分参数检查，flag.Usage() 用于打印提示信息
	// 		01) 必须指定 sql-agent 服务地址（URL）
	if service == "" {
		flag.Usage()
		log.Fatal("Error: URL to SQL Agent service required.")
	}

	// 		02) queriesFile和queryDir必须指定其中一个（且只能是其中一个）
	if queriesFile == DefaultQueriesFile && queryDir != "" {
		queriesFile = ""
	}
	if queriesFile != "" && queryDir != "" {
		flag.Usage()
		log.Fatal("Error: You can specify either -queries or -queryDir")
	}

	var (
		err     error
		queries QueryList
		config  *Config
	)
	config = newConfig()

	// 04. 解析配置文件，该配置文件用于定义数据源和一些公共配置
	if confFile != "" {
		// config对象包含： Defaults 和 DataSources 两部分
		config, err = loadConfig(confFile)
		if err != nil {
			log.Fatal(err)
		}
	}

	// 05. 加载查询配置文件，如果是目录，则加载目录下所有配置文件，返回一个QueryList对象
	if queryDir != "" {
		queries, err = loadQueriesInDir(queryDir, config, tolerateInvalidQueryDirFiles)
	} else {
		queries, err = loadQueryConfig(queriesFile, config)
	}
	if err != nil {
		log.Fatal(err)
	}

	if len(queries) == 0 {
		log.Fatal("No queries loaded!")
	}

	// Wait group of queries.
	// 06. 定义一个同步等待组，添加一个计数（查询数量）
	wg := new(sync.WaitGroup)
	wg.Add(len(queries))

	// Shared context. Close the cxt.Done channel to stop the workers.
	ctx, cancel := context.WithCancel(context.Background())

	var w *Worker

	// 08. （这段代码可以放在下面的循环语句之后）声明一个多路复用HTTP服务
	mux := http.NewServeMux()

	// 07. 迭代查询列表，每个查询启动一个协程（并发），每一个worker包含各自完整的执行参数（相互不影响）
	for _, q := range queries {
		// Create a new worker and start it in its own goroutine.
		// type key string
		// const wgKey key = "wg"
		w = NewWorker(context.WithValue(ctx, "wg", wg), q)
		go w.Start(service)
	}

	// Register the handler.
	// 09. 注册监控API处理器，对外提供 /metrics API
	mux.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("* Listening on %s...", addr)

	// Handles OS kill and interrupt.
	graceful.Run(addr, 5*time.Second, mux)

	log.Print("Canceling workers")
	cancel()
	log.Print("Waiting for workers to finish")
	wg.Wait()
	log.Println("All workers have finished, exiting!")
}
