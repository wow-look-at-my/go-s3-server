package main

import "testing"

func TestParseCgroupMemMax(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"max", 0, false},
		{"", 0, false},
		{"garbage", 0, false},
		{"0", 0, false},
		{"-5", 0, false},
		{"1073741824", 1073741824, true},  // 1 GiB
		{"536870912", 536870912, true},    // 512 MiB
		{"9223372036854771712", 0, false}, // cgroup v1 "unlimited" sentinel
	}
	for _, c := range cases {
		got, ok := parseCgroupMemMax(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("parseCgroupMemMax(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestComputeMemLimit(t *testing.T) {
	cases := []struct {
		limit int64
		ratio float64
		want  int64
	}{
		{1073741824, 0.9, 966367641}, // 90% of 1 GiB
		{1000, 0.5, 500},
		{0, 0.9, 0},
		{1000, 0, 0},
		{-1, 0.9, 0},
	}
	for _, c := range cases {
		if got := computeMemLimit(c.limit, c.ratio); got != c.want {
			t.Errorf("computeMemLimit(%d, %v) = %d, want %d", c.limit, c.ratio, got, c.want)
		}
	}
}
