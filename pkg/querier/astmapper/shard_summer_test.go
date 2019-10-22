package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestShardSummer(t *testing.T) {
	var testExpr = []struct {
		shards   int
		input    string
		expected string
	}{
		{
			shards: 3,
			input:  `sum by(foo) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum by(foo) (
			  sum by(foo) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
			  sum by(foo) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
			  sum by(foo) (rate(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
			)`,
		},
		{
			shards: 2,
			input: `sum(
				sum by (foo) (rate(bar1{baz="blip"}[1m]))
				/
				sum by (foo) (rate(foo{baz="blip"}[1m]))
			)`,
			expected: `sum(
			  sum by(foo) (
				sum by(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			  )
			  /
			  sum by(foo) (
				sum by(foo) (rate(foo{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo) (rate(foo{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			  )
			)`,
		},
		// This is currently redundant but still equivalent: sums split into sharded versions, including summed sums.
		{
			shards: 2,
			input:  `sum(sum by(foo) (rate(bar1{baz="blip"}[1m])))`,
			expected: `sum(
			  sum(
				sum by(foo) (
				  sum by(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				  sum by(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
				)
			  ) or
			  sum(
				sum by(foo) (
				  sum by(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				  sum by(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
				)
			  )
			)`,
		},
		// without
		{
			shards: 2,
			input:  `sum without(foo) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum without(foo) (
			  sum without(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum without(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)`,
		},
		// multiple dimensions
		{
			shards: 2,
			input:  `sum by(foo, bom) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum by(foo, bom) (
			  sum by(foo, bom) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum by(foo, bom) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer := NewShardSummer(c.shards, nil)
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)

			expected, err := promql.ParseExpr(c.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}

func TestParseShard(t *testing.T) {
	var testExpr = []struct {
		input string
		err   bool
		x     int
		of    int
	}{
		{
			input: "lsdjf",
			err:   true,
		},
		{
			input: "a_of_3",
			err:   true,
		},
		{
			input: "3_of_3",
			err:   true,
		},
		{
			input: "1_of_2",
			x:     1,
			of:    2,
		},
	}

	for _, c := range testExpr {
		t.Run(fmt.Sprint(c.input), func(t *testing.T) {
			x, of, err := ParseShard(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Equal(t, c.x, x)
				require.Equal(t, c.of, of)
			}
		})
	}

}
