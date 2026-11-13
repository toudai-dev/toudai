//go:build toudaisnappy

// Snappy golden generator for toudai-ingest's gRPC snappy test. It is not part
// of this repository's build: `../regenerate.sh` copies it into a
// toudai-dev/collector checkout and runs it there, so the compressed bytes come
// from github.com/golang/snappy at the version the Collector's configgrpc uses
// for `grpc-encoding: snappy`, written the way configgrpc writes them (a
// buffered framed writer).
//
// It writes:
//   - `logs.otlp.pb`: an OTLP logs request of two records;
//   - `logs.otlp.pb.sz`: the same bytes snappy-framed, holding one compressed
//     chunk (a repetitive body) and one uncompressed chunk (a random one).
package toudaiexporter

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/snappy"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

func TestToudaiSnappy(t *testing.T) {
	dir := os.Getenv("TOUDAI_SNAPPY_DIR")
	if dir == "" {
		t.Fatal("TOUDAI_SNAPPY_DIR is not set")
	}
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	random := make([]byte, 3000)
	source := rand.New(rand.NewSource(20260901))
	for i := range random {
		random[i] = alphabet[source.Intn(len(alphabet))]
	}
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, body := range []string{string(bytes.Repeat([]byte("a"), 4096)), string(random)} {
		record := records.AppendEmpty()
		record.Attributes().PutStr("a", "1")
		record.Body().SetStr(body)
	}
	plain, err := plogotlp.NewExportRequestFromLogs(logs).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	split := bytes.Index(plain, random)
	var compressed bytes.Buffer
	w := snappy.NewBufferedWriter(&compressed)
	if _, err := w.Write(plain[:split]); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(plain[split:]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	kinds := chunkKinds(compressed.Bytes())
	if !kinds[0x00] || !kinds[0x01] {
		t.Fatalf("want a compressed and an uncompressed chunk, got %v", kinds)
	}
	write(t, filepath.Join(dir, "logs.otlp.pb"), plain)
	write(t, filepath.Join(dir, "logs.otlp.pb.sz"), compressed.Bytes())
}

// chunkKinds lists the chunk types in a snappy framed stream: 0x00 is a
// compressed chunk, 0x01 an uncompressed one, 0xff the stream identifier.
func chunkKinds(stream []byte) map[byte]bool {
	kinds := map[byte]bool{}
	for len(stream) >= 4 {
		length := int(stream[1]) | int(stream[2])<<8 | int(stream[3])<<16
		kinds[stream[0]] = true
		stream = stream[4+length:]
	}
	return kinds
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
