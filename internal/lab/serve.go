package lab

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed web
var webFS embed.FS

// IndexEntry is one line of results/index.json.
type IndexEntry struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Mechanism string `json:"mechanism"`
	Headline  string `json:"headline"`
	File      string `json:"file"`
}

// RunAll records one scenario (or all) to dir and rewrites the index.
func RunAll(ctx context.Context, id, dir string, o Options, log io.Writer) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var todo []*Scenario
	for _, s := range Catalog(o) {
		if id == "all" || s.ID == id {
			todo = append(todo, s)
		}
	}
	if len(todo) == 0 {
		return fmt.Errorf("no scenario %q (try: switchyard lab list)", id)
	}
	for _, s := range todo {
		fmt.Fprintf(log, "== %s: %s (%s, %.0fs per variant)\n", s.ID, s.Title, s.Mechanism, s.Duration.Seconds())
		t0 := time.Now()
		res, err := Run(ctx, s, o)
		if err != nil {
			return err
		}
		b, err := json.Marshal(res)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, s.ID+".json"), b, 0o644); err != nil {
			return err
		}
		fmt.Fprintf(log, "   %s\n", res.Headline)
		for _, m := range res.Summary {
			fmt.Fprintf(log, "   %-48s without %10.2f   with %10.2f  %s\n", m.Label, m.Without, m.With, m.Unit)
		}
		fmt.Fprintf(log, "   done in %s\n", time.Since(t0).Round(time.Second))
	}
	return writeIndex(dir)
}

func writeIndex(dir string) error {
	var idx []IndexEntry
	order := map[string]int{}
	for i, s := range Catalog(Options{}) {
		order[s.ID] = i
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, f := range files {
		if filepath.Base(f) == "index.json" {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		idx = append(idx, IndexEntry{ID: r.ID, Title: r.Title, Mechanism: r.Mechanism, Headline: r.Headline, File: filepath.Base(f)})
	}
	sort.Slice(idx, func(i, j int) bool { return order[idx[i].ID] < order[idx[j].ID] })
	b, _ := json.MarshalIndent(idx, "", "  ")
	return os.WriteFile(filepath.Join(dir, "index.json"), b, 0o644)
}

// Serve runs the dashboard: recorded results from dir, plus live runs streamed
// over server-sent events at /api/live?scenario=<id>.
func Serve(addr, dir string) error {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(webFS, "web")
	mux.Handle("/", http.FileServer(http.FS(sub)))
	mux.Handle("/results/", http.StripPrefix("/results/", http.FileServer(http.Dir(dir))))
	var liveMu sync.Mutex
	mux.HandleFunc("/api/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"live":true}`))
	})
	mux.HandleFunc("/api/live", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("scenario")
		sc := Find(Options{}, id)
		if sc == nil {
			http.Error(w, "unknown scenario", http.StatusNotFound)
			return
		}
		if !liveMu.TryLock() {
			http.Error(w, "a live run is already in progress", http.StatusConflict)
			return
		}
		defer liveMu.Unlock()
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		var wmu sync.Mutex
		send := func(event string, v any) {
			b, _ := json.Marshal(v)
			wmu.Lock()
			defer wmu.Unlock()
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
			fl.Flush()
		}
		meta := newResult(sc)
		meta.Live = true
		for _, v := range sc.Variants {
			meta.Variants = append(meta.Variants, Variant{Key: v.Key, Label: v.Label, Settings: v.Settings})
		}
		send("meta", meta)
		res, err := RunLive(r.Context(), sc, Options{OnTick: func(v string, t Tick) {
			send("tick", map[string]any{"variant": v, "tick": t})
		}})
		if err != nil {
			send("error", map[string]string{"error": err.Error()})
			return
		}
		send("done", res)
	})
	log.Printf("lab: dashboard on http://%s (results from %s)", addr, dir)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return srv.ListenAndServe()
}

// Report renders the recorded results as a Markdown table (used for the
// README, so the numbers there are generated rather than typed).
func Report(dir string, w io.Writer) error {
	b, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return err
	}
	var idx []IndexEntry
	if err := json.Unmarshal(b, &idx); err != nil {
		return err
	}
	for _, e := range idx {
		rb, err := os.ReadFile(filepath.Join(dir, e.File))
		if err != nil {
			return err
		}
		var r Result
		if err := json.Unmarshal(rb, &r); err != nil {
			return err
		}
		fmt.Fprintf(w, "#### %s: %s\n\n%s\n\n", r.Mechanism, r.Title, r.Headline)
		fmt.Fprintf(w, "| Metric | %s | %s |\n|---|---:|---:|\n", r.Variants[0].Label, r.Variants[1].Label)
		for _, m := range r.Summary {
			fmt.Fprintf(w, "| %s | %s | %s |\n", m.Label, fmtMetric(m.Without, m.Unit), fmtMetric(m.With, m.Unit))
		}
		fmt.Fprintf(w, "\n")
	}
	return nil
}

func fmtMetric(v float64, unit string) string {
	switch {
	case unit == "ms" && v < 0:
		return "none served"
	case unit == "s" && v < 0:
		return "never"
	case unit == "%":
		return strconv.FormatFloat(v, 'f', -1, 64) + "%"
	case unit == "ms" || unit == "s" || unit == "x":
		return strconv.FormatFloat(v, 'f', -1, 64) + " " + unit
	case unit == "req/s":
		return commas(v) + "/s"
	}
	return commas(v)
}

func commas(v float64) string {
	if v != float64(int64(v)) {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	s := strconv.FormatInt(int64(v), 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

// SingleOptions configure ExportSingle.
type SingleOptions struct {
	FontCSS string // optional @font-face CSS to inline in place of the web-font links
	Home    string // optional URL for a "back to" link in the header
}

// ExportSingle writes the dashboard and every recorded result as one
// self-contained HTML file with no runtime network requests (other than the
// fonts, unless FontCSS is given). This is what gets hosted on a static site.
func ExportSingle(dir, out string, o SingleOptions) error {
	b, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return err
	}
	var idx []IndexEntry
	if err := json.Unmarshal(b, &idx); err != nil {
		return err
	}
	results := map[string]json.RawMessage{}
	for _, e := range idx {
		rb, err := os.ReadFile(filepath.Join(dir, e.File))
		if err != nil {
			return err
		}
		results[e.ID] = rb
	}
	blob, err := json.Marshal(map[string]any{"index": idx, "results": results}) // escapes <, > and &
	if err != nil {
		return err
	}
	page, err := webFS.ReadFile("web/index.html")
	if err != nil {
		return err
	}
	h := strings.Replace(string(page), `data-mode="auto"`, `data-mode="static"`, 1)
	if o.Home != "" {
		h = strings.Replace(h, `data-mode="static"`, `data-mode="static" data-home="`+o.Home+`"`, 1)
	}
	if o.FontCSS != "" {
		css, err := os.ReadFile(o.FontCSS)
		if err != nil {
			return err
		}
		var kept []string
		for _, line := range strings.Split(h, "\n") {
			if strings.Contains(line, "fonts.googleapis.com") || strings.Contains(line, "fonts.gstatic.com") {
				continue
			}
			kept = append(kept, line)
		}
		h = strings.Join(kept, "\n")
		h = strings.Replace(h, "<style>", "<style>\n"+string(css), 1)
	}
	h = strings.Replace(h, "<script>\n(() => {", `<script id="sy-data" type="application/json">`+string(blob)+"</script>\n<script>\n(() => {", 1)
	if !strings.Contains(h, `id="sy-data"`) {
		return errors.New("export: could not find the dashboard script to embed data before")
	}
	return os.WriteFile(out, []byte(h), 0o644)
}

// Export writes a static, replay-only copy of the dashboard and results.
func Export(dir, out string) error {
	if _, err := os.Stat(filepath.Join(dir, "index.json")); err != nil {
		if err := writeIndex(dir); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(out, "results"), 0o755); err != nil {
		return err
	}
	err := fs.WalkDir(webFS, "web", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := webFS.ReadFile(p)
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(p, "web/")
		if rel == "index.html" {
			b = []byte(strings.Replace(string(b), `data-mode="auto"`, `data-mode="static"`, 1))
		}
		dst := filepath.Join(out, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o644)
	})
	if err != nil {
		return err
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	if len(files) == 0 {
		return errors.New("no results to export; run: switchyard lab run")
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, "results", filepath.Base(f)), b, 0o644); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(out, ".nojekyll"), nil, 0o644)
}
