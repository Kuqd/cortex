package queryrange

import (
	"context"
	"time"
)

// StepAlignMiddleware aligns the start and end of request to the step to
// improved the cacheability of the query results.
var StepAlignMiddleware = MiddlewareFunc(func(next Handler) Handler {
	return stepAlign{
		next: next,
	}
})

type stepAlign struct {
	next Handler
}

func (s stepAlign) Do(ctx context.Context, r Request) (Response, error) {
	startNs := (r.GetStart().UnixNano() / r.GetStep().Nanoseconds()) * r.GetStep().Nanoseconds()
	endNs := (r.GetEnd().UnixNano() / r.GetStep().Nanoseconds()) * r.GetStep().Nanoseconds()
	return s.next.Do(ctx, r.WithStartEnd(time.Unix(0, startNs), time.Unix(0, endNs)))
}
