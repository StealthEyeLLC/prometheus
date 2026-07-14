// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"context"
	"slices"

	"github.com/prometheus/prometheus/storage"
)

// MemPostingsSnapshot holds the postings needed by one matcher query.
type MemPostingsSnapshot struct {
	values   map[string][]string
	postings map[string]map[string][]storage.SeriesRef
}

// Snapshot copies the label-value maps while no Add is running.
func (p *MemPostings) Snapshot(labelNames []string) *MemPostingsSnapshot {
	names := make(map[string]struct{}, len(labelNames)+1)
	for _, name := range labelNames {
		names[name] = struct{}{}
	}
	names[allPostingsKey.Name] = struct{}{}

	snapshot := &MemPostingsSnapshot{
		values:   make(map[string][]string, len(names)),
		postings: make(map[string]map[string][]storage.SeriesRef, len(names)),
	}

	p.gate.beginRead()
	defer p.gate.endRead()

	p.lvsMtx.RLock()
	for name := range names {
		snapshot.values[name] = slices.Clone(p.lvs[name])
	}
	p.lvsMtx.RUnlock()

	for name := range names {
		byValue := make(map[string][]storage.SeriesRef, len(snapshot.values[name]))
		for i := range p.shards {
			shard := &p.shards[i]
			shard.mtx.RLock()
			for value, refs := range shard.m[name] {
				byValue[value] = refs
			}
			shard.mtx.RUnlock()
		}
		snapshot.postings[name] = byValue
	}

	return snapshot
}

func (s *MemPostingsSnapshot) Postings(ctx context.Context, name string, values ...string) Postings {
	lps := make([]listPostings, len(values))
	its := make([]*listPostings, 0, len(values))
	for i, value := range values {
		if refs := s.postings[name][value]; len(refs) > 0 {
			lps[i] = listPostings{list: refs}
			its = append(its, &lps[i])
		}
	}
	return Merge(ctx, its...)
}

func (s *MemPostingsSnapshot) PostingsForAllLabelValues(ctx context.Context, name string) Postings {
	return s.Postings(ctx, name, s.values[name]...)
}

func (s *MemPostingsSnapshot) PostingsForLabelMatching(ctx context.Context, name string, match func(string) bool) Postings {
	values := s.values[name]
	lps := make([]listPostings, len(values))
	its := make([]*listPostings, 0, len(values))
	for i, value := range values {
		if i%checkContextEveryNIterations == 0 && ctx.Err() != nil {
			return ErrPostings(ctx.Err())
		}
		if !match(value) {
			continue
		}
		if refs := s.postings[name][value]; len(refs) > 0 {
			lps[i] = listPostings{list: refs}
			its = append(its, &lps[i])
		}
	}
	return Merge(ctx, its...)
}
