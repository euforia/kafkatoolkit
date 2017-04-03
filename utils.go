package kafkatoolkit

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

func parseKeyValuePairs(line []byte) map[string]string {
	l := len(line)

	var (
		vs = 0
		ks = 0

		vqs = false
		vqd = false

		key string
	)

	out := map[string]string{}

	for i := 0; i < l; i++ {
		switch line[i] {
		case '\\':
			i++

		case ' ':
			if !vqs && !vqd {
				out[key] = string(line[vs:i])
				ks = i + 1
			}

		case '=':
			key = string(line[ks:i])
			//fmt.Println("key", key)
			vs = i + 1

		case '\'':
			if vqd {
				continue
			}
			if vqs {
				out[key] = string(line[vs:i])
				vqs = false
				ks = i + 1
				if i+1 < l && line[i+1] == ' ' {
					i++
				}
			} else {
				vs = i + 1
				vqs = true
			}

		case '"':
			if vqs {
				continue
			}
			if vqd {
				out[key] = string(line[vs:i])
				vqd = false
				if i+1 < l && line[i+1] == ' ' {
					i++
				}
				ks = i + 1
			} else {
				vs = i + 1
				vqd = true
			}
		}
	}

	return out
}

func ParsePeers(peerList string) []string {
	peers := []string{}
	for _, p := range strings.Split(peerList, ",") {
		if peer := strings.TrimSpace(p); len(peer) > 0 {
			peers = append(peers, peer)
		}
	}
	return peers
}

// OffsetFromPosition calculates the offset. If startPos is oldest then offset is added, otherwise subtracted.
// If the calculated offset is less than 0 it is set to zero.
func OffsetFromPosition(startPos string, offset uint64) (dOffset int64, err error) {

	switch startPos {
	case "oldest":
		dOffset = sarama.OffsetOldest
		dOffset += int64(offset)

	case "newest":
		dOffset = sarama.OffsetNewest
		dOffset -= int64(offset)
		if dOffset < 0 {
			dOffset = 0
		}

	default:
		//printUsageErrorExit("-offset must be 'oldest' or 'newest'")
		err = fmt.Errorf("invalid offset")
	}
	return
}
