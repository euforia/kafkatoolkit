package kafkatoolkit

import (
	"encoding/json"
	"testing"
)

var testLine = `key=value key1="value1" some="wlkjdf l ''" key2='value2'`

func Test_parseKeyValuePairs(t *testing.T) {
	m := parseKeyValuePairs([]byte(testLine))
	b, _ := json.MarshalIndent(m, " ", "  ")
	t.Logf("%s", b)
}
