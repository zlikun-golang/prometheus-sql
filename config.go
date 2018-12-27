package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

// Default config values
var (
	DefaultHost                         = ""
	DefaultTimeout                      = time.Minute
	DefaultInterval                     = time.Minute * 5
	DefaultService                      = ""
	DefaultQueriesFile                  = "queries.yml"
	DefaultQueriesDir                   = ""
	DefaultPort                         = 8080
	DefaultConfFile                     = ""
	DefaultTolerateInvalidQueryDirFiles = false
)

// Config is the base data structure.
type Config struct {
	Defaults    DefaultsData          `yaml:"defaults"`
	DataSources map[string]DataSource `yaml:"data-sources"`
}

// DefaultsData defines the possible default values to define.
type DefaultsData struct {
	DataSourceRef     string        `yaml:"data-source"`
	QueryInterval     time.Duration `yaml:"query-interval"`
	QueryTimeout      time.Duration `yaml:"query-timeout"`
	QueryValueOnError string        `yaml:"query-value-on-error"`
}

// DataSource is configuration a data source which must be supported by sql-agent.
type DataSource struct {
	Driver     string                 `yaml:"driver"`
	Properties map[string]interface{} `yaml:"properties"`
}

// Query defines a SQL statement and parameters as well as configuration for the monitoring behavior
type Query struct {
	Name          string
	DataSourceRef string `yaml:"data-source"`
	Driver        string
	Connection    map[string]interface{}
	SQL           string
	Params        map[string]interface{}
	Interval      time.Duration
	Timeout       time.Duration
	DataField     string            `yaml:"data-field"`
	SubMetrics    map[string]string `yaml:"sub-metrics"`
	ValueOnError  string            `yaml:"value-on-error"`
}

// QueryList is a array or Queries
type QueryList []*Query

func createDefaultsData() DefaultsData {
	return DefaultsData{
		DataSourceRef:     "",
		QueryInterval:     DefaultInterval,
		QueryTimeout:      DefaultTimeout,
		QueryValueOnError: "",
	}
}

func newConfig() *Config {
	return &Config{Defaults: createDefaultsData()}
}

func appendDefaults(c *Config) {
	if c.Defaults.QueryInterval == 0 {
		c.Defaults.QueryInterval = DefaultInterval
	}
	if c.Defaults.QueryTimeout == 0 {
		c.Defaults.QueryTimeout = DefaultTimeout
	}
}

func validateConfig(c *Config) error {
	for name, ds := range c.DataSources {
		if ds.Driver == "" {
			return fmt.Errorf("Driver is not defined for data source [%s]", name)
		}
		if len(ds.Properties) == 0 {
			return fmt.Errorf("Properties are not defined for data source [%s]", name)
		}
	}

	return nil
}

// 校验查询配置
func validateQuery(q *Query) error {
	// 1) 查询名称，非空
	if q.Name == "" {
		return errors.New("Query is not named")
	}
	// 2) 驱动（来自数据源），非空
	if q.Driver == "" {
		return fmt.Errorf("No data source or driver is specified for query [%s]", q.Name)
	}
	// 3) 查询SQL，非空
	if q.SQL == "" {
		return fmt.Errorf("SQL statement required for query [%s]", q.Name)
	}
	// 4) 超时时间，非空，通过跟踪源码可知，这里的超时时间是指HTTP请求超时时间
	if q.Timeout == 0 {
		return fmt.Errorf("Timeout must be greater than zero for query [%s]", q.Name)
	}
	// 5) 间隔时间，非空
	if q.Interval == 0 {
		return fmt.Errorf("Interval must be greater than zero for query [%s]", q.Name)
	}

	return nil
}

// 加载配置文件，解析YAML格式配置，返回全局配置和数据源列表
func loadConfig(file string) (*Config, error) {
	log.Printf("Load config from file [%s]", file)
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("Error reading config file: %s", err)
	}

	// Expand environment variables.
	b = []byte(os.ExpandEnv(string(b)))

	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("Error decoding config file: %s", err)
	}

	appendDefaults(&c)
	if err := validateConfig(&c); err != nil {
		return nil, err
	}

	return &c, err
}

// 加载查询配置文件
func loadQueryConfig(queriesFile string, config *Config) (QueryList, error) {
	log.Printf("Load queries from file [%s]", queriesFile)
	// Read queries for request body.
	file, err := os.Open(queriesFile)
	if err != nil {
		return nil, fmt.Errorf("Error opening queries file: %s", err)
	}

	defer file.Close()
	return decodeQueries(file, config)
}

// 解码查询文件（拆分查询配置文件中所有的查询项），返回结构化的查询对象列表
func decodeQueries(r io.Reader, config *Config) (QueryList, error) {
	if config == nil {
		return nil, errors.New("Bug! Config must not be nil")
	}

	queries := make(QueryList, 0)
	parsedQueries := make([]map[string]*Query, 0)

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(b, &parsedQueries); err != nil {
		return nil, err
	}

	for _, data := range parsedQueries {
		for k, q := range data {
			q.Name = k
			// 数据源，空则使用默认数据源
			if q.DataSourceRef == "" {
				q.DataSourceRef = config.Defaults.DataSourceRef
			}
			// 驱动，默认则使用数据源驱动
			if q.Driver == "" {
				if q.DataSourceRef != "" && len(config.DataSources) > 0 {
					var ds = config.DataSources[q.DataSourceRef]
					q.Driver = ds.Driver
					q.Connection = ds.Properties
				}
			}
			// 间隔时间，空则使用默认查询间隔时间
			if q.Interval == 0 {
				q.Interval = config.Defaults.QueryInterval
			}
			// 超时时间，空则使用默认超时时间
			if q.Timeout == 0 {
				q.Timeout = config.Defaults.QueryTimeout
			}
			// 值错误替代值，空则使用默认替代值
			if q.ValueOnError == "" && config.Defaults.QueryValueOnError != "" {
				q.ValueOnError = config.Defaults.QueryValueOnError
			}
			// data-field
			// sub-metrics，会被自动解析了一个 map ，并通过结构体别名与 SubMetrics 绑定
			q.DataField = strings.ToLower(q.DataField)
			if err := validateQuery(q); err != nil {
				return nil, err
			}

			queries = append(queries, q)
		}

	}

	return queries, nil
}

func loadQueriesInDir(path string, config *Config, allowFileErrors bool) (QueryList, error) {
	log.Printf("Load queries from directory [%s]", path)
	queries := make(QueryList, 0)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		fn := f.Name()
		if strings.HasSuffix(fn, ".yml") {
			fn := fmt.Sprintf("%s/%s", strings.TrimRight(path, "/"), fn)
			log.Println("Loading", fn)
			file, err := os.Open(fn)
			if err != nil {
				return nil, err
			}

			q, err := decodeQueries(file, config)
			file.Close()

			if err == nil {
				queries = append(queries, q...)
			} else if allowFileErrors {
				log.Printf("Ignoring error loading %s. err=%v", fn, err)
			} else {
				return nil, err
			}
		}
	}

	return queries, nil
}
