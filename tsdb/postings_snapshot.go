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

package tsdb

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

type postingsForMatchersSnapshotter interface {
	postingsForMatchersSnapshot([]*labels.Matcher) IndexReader
}

type headPostingsSnapshotReader struct {
	*headIndexReader
	snapshot *index.MemPostingsSnapshot
}

func (h *headIndexReader) postingsForMatchersSnapshot(matchers []*labels.Matcher) IndexReader {
	labelNames := make([]string, 0, len(matchers))
	for _, matcher := range matchers {
		labelNames = append(labelNames, matcher.Name)
	}
	return &headPostingsSnapshotReader{
		headIndexReader: h,
		snapshot:        h.head.postings.Snapshot(labelNames),
	}
}

func (r *headPostingsSnapshotReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	return r.snapshot.Postings(ctx, name, values...), nil
}

func (r *headPostingsSnapshotReader) PostingsForLabelMatching(ctx context.Context, name string, match func(string) bool) index.Postings {
	return r.snapshot.PostingsForLabelMatching(ctx, name, match)
}

func (r *headPostingsSnapshotReader) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	return r.snapshot.PostingsForAllLabelValues(ctx, name)
}
