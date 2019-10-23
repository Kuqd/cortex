package querysharding

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

var (
	start  = time.Now()
	end    = start.Add(1 * time.Minute)
	step   = 30 * time.Second
	ctx    = context.Background()
	engine = promql.NewEngine(promql.EngineOpts{
		Reg:                prometheus.DefaultRegisterer,
		MaxConcurrent:      100,
		Logger:             util.Logger,
		Timeout:            1 * time.Hour,
		MaxSamples:         10e6,
		ActiveQueryTracker: nil,
	})
)

func Test_PromQL(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		normalQuery string
		shardQuery  string
		shouldEqual bool
	}{
		{
			`sum(bar1{baz="blip"})`,
			`sum(
				sum (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			false,
		},
		{
			`sum(bar1{baz="blip"})`,
			`sum(
				sum by(__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum by(__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum by(__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		{
			`sum by (foo) (bar1{baz="blip"})`,
			`sum by (foo) (
				sum by(foo) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum by(foo) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum by(foo) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			false,
		},
		{
			`sum by (foo) (bar1{baz="blip"})`,
			`sum by (foo) (
				sum by(foo,__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum by(foo,__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum by(foo,__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		{
			`sum by (foo,bar) (bar1{baz="blip"})`,
			` sum by (foo,bar)(
				sum by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.normalQuery, func(t *testing.T) {
			t.Parallel()

			baseQuery, err := engine.NewRangeQuery(shardAwareQueryable, tt.normalQuery, start, end, step)
			require.Nil(t, err)
			shardQuery, err := engine.NewRangeQuery(shardAwareQueryable, tt.shardQuery, start, end, step)
			require.Nil(t, err)
			baseResult := baseQuery.Exec(ctx)
			shardResult := shardQuery.Exec(ctx)
			// t.Logf("base: %v\n", baseResult)
			// t.Logf("shard: %v\n", shardResult)
			if tt.shouldEqual {
				require.Equal(t, baseResult, shardResult)
				return
			}
			require.NotEqual(t, baseResult, shardResult)
		})
	}

}

var shardAwareQueryable = storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &matrix{
		series: []*promql.StorageSeries{
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blop"}, {"foo", "barr"}}, factor(5)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blop"}, {"foo", "bazz"}}, factor(10)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blap"}, {"foo", "buzz"}}, factor(20)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blap"}, {"foo", "bozz"}}, factor(15)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blop"}, {"foo", "buzz"}}, factor(25)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blap"}, {"foo", "bazz"}}, factor(25)),
		},
	}, nil
})

type matrix struct {
	series []*promql.StorageSeries
}

func (m matrix) Next() bool { return len(m.series) != 0 }

func (m *matrix) At() storage.Series {
	res := m.series[0]
	m.series = m.series[1:]
	return res
}

func (s matrix) Err() error { return nil }

func (m *matrix) Select(selectParams *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	shardIndex := -1
	shardTotal := 0
	var err error
	for _, matcher := range matchers {
		if matcher.Name != "__cortex_shard__" {
			continue
		}
		shardData := strings.Split(matcher.Value, "_")
		shardIndex, err = strconv.Atoi(shardData[0])
		if err != nil {
			panic(err)
		}
		shardTotal, err = strconv.Atoi(shardData[2])
		if err != nil {
			panic(err)
		}
	}
	if shardIndex >= 0 {
		shard := splitByShard(shardIndex, shardTotal, m)
		return shard, nil, nil
	}
	return m, nil, nil
}

func (f *matrix) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}
func (f *matrix) LabelNames() ([]string, storage.Warnings, error) { return nil, nil, nil }
func (f *matrix) Close() error                                    { return nil }

func newSeries(metric labels.Labels, generator func(float64) float64) *promql.StorageSeries {
	sort.Sort(metric)
	var points []promql.Point

	for ts := start; ts.Unix() <= end.Unix(); ts = ts.Add(step) {
		t := ts.Unix() * 1e3
		points = append(points, promql.Point{
			T: t,
			V: generator(float64(t)),
		})
	}

	return promql.NewStorageSeries(promql.Series{
		Metric: metric,
		Points: points,
	})
}

func identity(t float64) float64 {
	return float64(t)
}

func factor(f float64) func(float64) float64 {
	i := 0.
	return func(float64) float64 {
		i++
		res := i * f
		return res
	}
}

// var identity(t int64) float64 {
// 	return float64(t)
// }

// splitByShard returns the shard subset of a matrix.
// e.g if a matrix has 6 series, and we want 3 shard, then each shard will contain
// 2 series.
func splitByShard(shardIndex, shardTotal int, matrixes *matrix) *matrix {
	res := &matrix{}
	for i, s := range matrixes.series {
		if i%shardTotal != shardIndex {
			continue
		}
		var points []promql.Point
		it := s.Iterator()
		for it.Next() {
			t, v := it.At()
			points = append(points, promql.Point{
				T: t,
				V: v,
			})

		}
		lbs := s.Labels().Copy()
		lbs = append(lbs, labels.Label{Name: "__cortex_shard__", Value: fmt.Sprintf("%d_of_%d", shardIndex, shardTotal)})
		res.series = append(res.series, promql.NewStorageSeries(promql.Series{
			Metric: lbs,
			Points: points,
		}))
	}
	return res
}
