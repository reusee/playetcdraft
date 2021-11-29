package main

import (
	"bytes"
	"encoding/json"
)

func dump(v any) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "   ")
	ce(enc.Encode(v))
	pt("%s\n", buf.Bytes())
}
