// Package trace loads the real request-popularity distribution the lab
// replays: the 20,000 most-viewed English Wikipedia articles during the
// 12:00-13:00 UTC hour of 2024-09-01, taken from the public Wikimedia
// pageview dumps. Sampling uses Walker's alias method, so drawing a key is
// O(1) regardless of the number of pages.
package trace

import (
	"bufio"
	"compress/gzip"
	"embed"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
)

//go:embed data/enwiki-20240901-12.tsv.gz
var data embed.FS

const Source = "https://dumps.wikimedia.org/other/pageviews/2024/2024-09/pageviews-20240901-120000.gz"

type Page struct {
	Title string
	Views int
}

var (
	once  sync.Once
	pages []Page
	err   error
)

// Pages returns the pages sorted by views, most popular first.
func Pages() ([]Page, error) {
	once.Do(func() {
		f, e := data.Open("data/enwiki-20240901-12.tsv.gz")
		if e != nil {
			err = e
			return
		}
		defer f.Close()
		gz, e := gzip.NewReader(f)
		if e != nil {
			err = e
			return
		}
		sc := bufio.NewScanner(gz)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "#") || line == "" {
				continue
			}
			title, v, ok := strings.Cut(line, "\t")
			if !ok {
				continue
			}
			n, e := strconv.Atoi(v)
			if e != nil {
				err = fmt.Errorf("trace: bad view count %q", v)
				return
			}
			pages = append(pages, Page{Title: title, Views: n})
		}
		err = sc.Err()
	})
	return pages, err
}

// Sampler draws page paths with probability proportional to real views.
type Sampler struct {
	paths []string
	prob  []float64
	alias []int
}

// NewSampler uses the top n pages (all if n <= 0).
func NewSampler(n int) (*Sampler, error) {
	ps, err := Pages()
	if err != nil {
		return nil, err
	}
	if n <= 0 || n > len(ps) {
		n = len(ps)
	}
	ps = ps[:n]
	w := make([]float64, n)
	var total float64
	for i, p := range ps {
		w[i] = float64(p.Views)
		total += w[i]
	}
	s := &Sampler{paths: make([]string, n), prob: make([]float64, n), alias: make([]int, n)}
	for i, p := range ps {
		s.paths[i] = "/wiki/" + p.Title
	}
	// Vose's stable variant of the alias method.
	scaled := make([]float64, n)
	var small, large []int
	for i := range w {
		scaled[i] = w[i] * float64(n) / total
		if scaled[i] < 1 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}
	for len(small) > 0 && len(large) > 0 {
		l := small[len(small)-1]
		small = small[:len(small)-1]
		g := large[len(large)-1]
		large = large[:len(large)-1]
		s.prob[l], s.alias[l] = scaled[l], g
		scaled[g] = scaled[g] + scaled[l] - 1
		if scaled[g] < 1 {
			small = append(small, g)
		} else {
			large = append(large, g)
		}
	}
	for _, i := range append(small, large...) {
		s.prob[i] = 1
	}
	return s, nil
}

// Sample returns one path.
func (s *Sampler) Sample(rng *rand.Rand) string {
	i := rng.IntN(len(s.paths))
	if rng.Float64() < s.prob[i] {
		return s.paths[i]
	}
	return s.paths[s.alias[i]]
}

// Paths returns every path the sampler can produce, most popular first.
func (s *Sampler) Paths() []string { return s.paths }
