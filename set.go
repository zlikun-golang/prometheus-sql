package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type metricStatus int

const (
	registered metricStatus = iota
	unregistered
)

type QueryResult struct {
	Query  *Query
	Result map[string]prometheus.Gauge // Internally we represent each facet with a JSON-encoded string for simplicity
}

// NewSetMetrics initializes a new metrics collector.
func NewQueryResult(q *Query) *QueryResult {
	r := &QueryResult{
		Query:  q,
		Result: make(map[string]prometheus.Gauge),
	}

	return r
}

// 02. 注册指标项，facets指代标签集合，suffix则用于拼接到监控项的键上，这里实际尚未注册，只是标识出是否需要注册，由下面的批量注册函数来实现真正注册逻辑
func (r *QueryResult) registerMetric(facets map[string]interface{}, suffix string) (string, metricStatus) {
	labels := prometheus.Labels{}
	// 1) 监控项名由固定前缀（query_result_） + 查询名称（查询配置决定） + 后缀（_suffix，如果为空则省略）
	metricName := r.Query.Name
	if suffix != "" {
		metricName = fmt.Sprintf("%s_%s", r.Query.Name, suffix)
	}

	jsonData, _ := json.Marshal(facets)
	// 2) 是组装出：name{"label_1":"value_1","label_2":"value_2"} 这种结构么？
	// 这里组装出来的并非监控项的键，而是系统用于惟一标识监控项的标识，用于去重（判断是否重复注册）
	resultKey := fmt.Sprintf("%s%s", metricName, string(jsonData))

	// 3) 组装Labels
	for k, v := range facets {
		labels[k] = strings.ToLower(fmt.Sprintf("%v", v))
	}

	// 4) 检查指标项是否已注册过
	if _, ok := r.Result[resultKey]; ok { // A metric with this name is already registered
		return resultKey, registered
	}

	fmt.Println("Creating", resultKey)
	// 5) 新创建一个指标项
	r.Result[resultKey] = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        fmt.Sprintf("query_result_%s", metricName),
		Help:        "Result of an SQL query",
		ConstLabels: labels,
	})
	return resultKey, unregistered
}

type record map[string]interface{}
type records []record

func setValueForResult(r prometheus.Gauge, v interface{}) error {
	switch t := v.(type) {
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return err
		}
		r.Set(f)
	case int:
		r.Set(float64(t))
	case float64:
		r.Set(t)
	default:
		return fmt.Errorf("Unhandled type %s", t)
	}
	return nil
}

// 01. 设置监控指标
func (r *QueryResult) SetMetrics(recs records) (map[string]metricStatus, error) {
	// Queries that return only one record should only have one column
	if len(recs) > 1 && len(recs[0]) == 1 {
		return nil, errors.New("There is more than one row in the query result - with a single column")
	}

	if r.Query.DataField != "" && len(r.Query.SubMetrics) > 0 {
		return nil, errors.New("sub-metrics are not compatible with data-field")
	}

	submetrics := map[string]string{}

	// 1) 如果查询配置中指定了 SubMetrics，则使用指定 SubMetrics，否则将 DataField 填充到 SubMetrics 中（键为空）
	if len(r.Query.SubMetrics) > 0 {
		submetrics = r.Query.SubMetrics
	} else {
		submetrics = map[string]string{"": r.Query.DataField}
	}

	facetsWithResult := make(map[string]metricStatus, 0)
	// 2) 迭代查询结果集
	for _, row := range recs {
		// 3) 迭代子指标项，两层循环用于组合数据
		for suffix, datafield := range submetrics {
			facet := make(map[string]interface{})
			var (
				dataVal   interface{}
				dataFound bool
			)
			// 4) 遍历一行记录中的每一列，按列名（子监控指标名）组织数据
			for k, v := range row {
				if len(row) > 1 && strings.ToLower(k) != datafield { // facet field, add to facets
					// 5) 如果列大于一，但列名与指标项不匹配时，判断其它指标项中是否匹配，如果都不匹配，则表示是一个Label，而非值项
					submetric := false
					for _, n := range submetrics {
						if strings.ToLower(k) == n {
							submetric = true
						}
					}
					// it is a facet field and not a submetric field
					if !submetric {
						facet[strings.ToLower(fmt.Sprintf("%v", k))] = v
					}
				} else { // this is the actual gauge data
					// 6) 反之，表示是一个值项
					if dataFound {
						return nil, errors.New("Data field not specified for multi-column query")
					}
					dataVal = v
					dataFound = true
				}
			}

			if !dataFound {
				return nil, errors.New("Data field not found in result set")
			}

			// 7) 注册指标项
			key, status := r.registerMetric(facet, suffix)
			// 8) 设置指标值
			err := setValueForResult(r.Result[key], dataVal)
			if err != nil {
				return nil, err
			}
			facetsWithResult[key] = status
		}
	}

	return facetsWithResult, nil
}

// 03. 注册多个监控指标项，与Prometheus集成逻辑
func (r *QueryResult) RegisterMetrics(facetsWithResult map[string]metricStatus) {
	for key, m := range r.Result {
		status, ok := facetsWithResult[key]
		if !ok {
			fmt.Println("Unregistering metric", key)
			prometheus.Unregister(m)
			delete(r.Result, key)
			continue
		}
		if status == unregistered {
			defer func(key string, m prometheus.Gauge) {
				fmt.Println("Registering metric", key)
				prometheus.MustRegister(m)
			}(key, m)
		}
	}
}
