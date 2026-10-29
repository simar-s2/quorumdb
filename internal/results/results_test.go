package results

import "testing"

func TestMedian(t *testing.T) {
	cases := []struct {
		values []float64
		want   float64
	}{
		{nil, 0},
		{[]float64{7}, 7},
		{[]float64{3, 1, 2}, 2},
		{[]float64{4, 1, 3, 2}, 2.5},
	}
	for _, c := range cases {
		if got := Median(c.values); got != c.want {
			t.Errorf("Median(%v) = %v, want %v", c.values, got, c.want)
		}
	}
}
