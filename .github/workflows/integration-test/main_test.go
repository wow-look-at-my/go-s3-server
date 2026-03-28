package main

import "testing"

func TestGreet(t *testing.T) {
	if got := greet("test"); got != "hello, test" {
		t.Errorf("got %q", got)
	}
}
