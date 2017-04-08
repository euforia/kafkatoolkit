package kafkatoolkit

import "bytes"

// Filter the data out or not.  This does basic keyword filtering
func Filter(filter, data []byte) bool {
	if filter == nil {
		return false
	}
	return !bytes.Contains(data, filter)
}
