package gqm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CronExpr represents a parsed cron expression.
//
// Accepts 6 fields: second minute hour day_of_month month day_of_week
// or 5 fields: minute hour day_of_month month day_of_week (second defaults to 0).
//
// Field syntax:
//   - * (any value)
//   - N (specific value)
//   - N-M (range)
//   - */S or N/S (step from min or N)
//   - N-M/S (range with step)
//   - N,M,O (comma-separated list)
//
// Day matching: when both day_of_month and day_of_week are explicitly set
// (not *), the match uses OR semantics (standard cron behavior).
//
// Day of week: 0 = Sunday, 1 = Monday, ..., 6 = Saturday, 7 = Sunday (alias).
type CronExpr struct {
	second     []int // 0-59
	minute     []int // 0-59
	hour       []int // 0-23
	dayOfMonth []int // 1-31
	month      []int // 1-12
	dayOfWeek  []int // 0-6 (0 = Sunday)

	domSet bool // day_of_month explicitly set (not *)
	dowSet bool // day_of_week explicitly set (not *)

	raw string
}

type cronFieldSpec struct {
	min int
	max int
}

var cronFieldSpecs = [6]cronFieldSpec{
	{0, 59}, // second
	{0, 59}, // minute
	{0, 23}, // hour
	{1, 31}, // day of month
	{1, 12}, // month
	{0, 7},  // day of week (0 and 7 both = Sunday)
}

// ParseCronExpr parses a cron expression string into a CronExpr.
// It accepts 6 fields (second minute hour dom month dow) or 5 fields
// (minute hour dom month dow, with second implicitly 0).
func ParseCronExpr(expr string) (*CronExpr, error) {
	expr = strings.TrimSpace(expr)
	fields := strings.Fields(expr)

	switch len(fields) {
	case 5:
		fields = append([]string{"0"}, fields...)
	case 6:
		// ok
	default:
		return nil, fmt.Errorf("cron: expected 5 or 6 fields, got %d", len(fields))
	}

	c := &CronExpr{raw: expr}
	var err error

	c.second, err = parseCronField(fields[0], cronFieldSpecs[0])
	if err != nil {
		return nil, fmt.Errorf("cron: second: %w", err)
	}
	c.minute, err = parseCronField(fields[1], cronFieldSpecs[1])
	if err != nil {
		return nil, fmt.Errorf("cron: minute: %w", err)
	}
	c.hour, err = parseCronField(fields[2], cronFieldSpecs[2])
	if err != nil {
		return nil, fmt.Errorf("cron: hour: %w", err)
	}
	c.dayOfMonth, err = parseCronField(fields[3], cronFieldSpecs[3])
	if err != nil {
		return nil, fmt.Errorf("cron: day_of_month: %w", err)
	}
	c.domSet = fields[3] != "*"

	c.month, err = parseCronField(fields[4], cronFieldSpecs[4])
	if err != nil {
		return nil, fmt.Errorf("cron: month: %w", err)
	}
	c.dayOfWeek, err = parseCronField(fields[5], cronFieldSpecs[5])
	if err != nil {
		return nil, fmt.Errorf("cron: day_of_week: %w", err)
	}
	c.dowSet = fields[5] != "*"

	// Normalize day_of_week: 7 → 0 (both mean Sunday).
	c.dayOfWeek = normalizeDOW(c.dayOfWeek)

	return c, nil
}

func parseCronField(field string, spec cronFieldSpec) ([]int, error) {
	if field == "*" {
		vals := make([]int, 0, spec.max-spec.min+1)
		for i := spec.min; i <= spec.max; i++ {
			vals = append(vals, i)
		}
		return vals, nil
	}

	set := make(map[int]bool)
	for _, part := range strings.Split(field, ",") {
		vals, err := parseCronPart(part, spec)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			set[v] = true
		}
	}

	if len(set) == 0 {
		return nil, fmt.Errorf("empty field")
	}

	vals := make([]int, 0, len(set))
	for v := range set {
		vals = append(vals, v)
	}
	sort.Ints(vals)
	return vals, nil
}

func parseCronPart(part string, spec cronFieldSpec) ([]int, error) {
	var step int
	if idx := strings.IndexByte(part, '/'); idx != -1 {
		s, err := strconv.Atoi(part[idx+1:])
		if err != nil || s <= 0 {
			return nil, fmt.Errorf("invalid step %q", part[idx+1:])
		}
		step = s
		part = part[:idx]
	}

	var start, end int

	if part == "*" {
		start = spec.min
		end = spec.max
	} else if idx := strings.IndexByte(part, '-'); idx != -1 {
		var err error
		start, err = strconv.Atoi(part[:idx])
		if err != nil {
			return nil, fmt.Errorf("invalid range start %q", part[:idx])
		}
		end, err = strconv.Atoi(part[idx+1:])
		if err != nil {
			return nil, fmt.Errorf("invalid range end %q", part[idx+1:])
		}
	} else {
		var err error
		start, err = strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid value %q", part)
		}
		end = start
	}

	if start < spec.min || start > spec.max {
		return nil, fmt.Errorf("value %d out of range [%d, %d]", start, spec.min, spec.max)
	}
	if end < spec.min || end > spec.max {
		return nil, fmt.Errorf("value %d out of range [%d, %d]", end, spec.min, spec.max)
	}
	if start > end {
		return nil, fmt.Errorf("range start %d > end %d", start, end)
	}

	if step == 0 {
		step = 1
	}

	vals := make([]int, 0, (end-start)/step+1)
	for i := start; i <= end; i += step {
		vals = append(vals, i)
	}
	return vals, nil
}

// normalizeDOW maps day_of_week value 7 to 0 (both mean Sunday) and deduplicates.
func normalizeDOW(vals []int) []int {
	var has [8]bool
	for _, v := range vals {
		has[v] = true
	}
	if has[7] {
		has[0] = true
	}
	result := make([]int, 0, 7)
	for i := 0; i < 7; i++ {
		if has[i] {
			result = append(result, i)
		}
	}
	return result
}

// Next returns the next time after from that matches the cron expression.
// The search covers up to 4 years ahead. Returns the zero time if no match is found.
func (c *CronExpr) Next(from time.Time) time.Time {
	t := from.Truncate(time.Second).Add(time.Second)
	limit := from.Add(4 * 366 * 24 * time.Hour)

	for i := 0; i < 500_000 && t.Before(limit); i++ {
		if !cronInSet(c.month, int(t.Month())) {
			t = c.nextMonth(t)
			continue
		}

		if !c.matchDay(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}

		if !cronInSet(c.hour, t.Hour()) {
			t = c.nextHour(t)
			continue
		}

		if !cronInSet(c.minute, t.Minute()) {
			t = c.nextMinute(t)
			continue
		}

		if !cronInSet(c.second, t.Second()) {
			t = c.nextSecond(t)
			continue
		}

		return t
	}

	return time.Time{}
}

// matchDay checks whether t matches the day fields.
// Standard cron: if both day_of_month and day_of_week are explicitly set,
// the match is OR (union). Otherwise, only the set field is checked.
func (c *CronExpr) matchDay(t time.Time) bool {
	dom := t.Day()
	dow := int(t.Weekday())

	if c.domSet && c.dowSet {
		return cronInSet(c.dayOfMonth, dom) || cronInSet(c.dayOfWeek, dow)
	}
	if c.domSet {
		return cronInSet(c.dayOfMonth, dom)
	}
	if c.dowSet {
		return cronInSet(c.dayOfWeek, dow)
	}
	return true
}

func (c *CronExpr) nextMonth(t time.Time) time.Time {
	next, wrapped := cronNextInSet(c.month, int(t.Month()))
	year := t.Year()
	if wrapped {
		year++
	}
	return time.Date(year, time.Month(next), 1, 0, 0, 0, 0, t.Location())
}

func (c *CronExpr) nextHour(t time.Time) time.Time {
	next, wrapped := cronNextInSet(c.hour, t.Hour())
	if wrapped {
		return time.Date(t.Year(), t.Month(), t.Day()+1, next, c.minute[0], c.second[0], 0, t.Location())
	}
	return time.Date(t.Year(), t.Month(), t.Day(), next, c.minute[0], c.second[0], 0, t.Location())
}

func (c *CronExpr) nextMinute(t time.Time) time.Time {
	next, wrapped := cronNextInSet(c.minute, t.Minute())
	if wrapped {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, next, c.second[0], 0, t.Location())
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), next, c.second[0], 0, t.Location())
}

func (c *CronExpr) nextSecond(t time.Time) time.Time {
	next, wrapped := cronNextInSet(c.second, t.Second())
	if wrapped {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, next, 0, t.Location())
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), next, 0, t.Location())
}

// cronInSet reports whether v is in the sorted slice vals.
func cronInSet(vals []int, v int) bool {
	i := sort.SearchInts(vals, v)
	return i < len(vals) && vals[i] == v
}

// cronNextInSet returns the smallest value in vals that is >= v.
// If no such value exists, it returns vals[0] and wrapped=true.
// Panics if vals is empty (caller must guarantee non-empty sets).
func cronNextInSet(vals []int, v int) (next int, wrapped bool) {
	if len(vals) == 0 {
		panic("cronNextInSet: empty value set")
	}
	i := sort.SearchInts(vals, v)
	if i < len(vals) {
		return vals[i], false
	}
	return vals[0], true
}

// String returns the original cron expression string.
func (c *CronExpr) String() string {
	return c.raw
}

// OverlapPolicy determines behavior when a cron schedule fires while the
// previous job from the same entry is still running.
type OverlapPolicy string

const (
	// OverlapSkip skips the new execution if the previous job is still running.
	OverlapSkip OverlapPolicy = "skip"
	// OverlapAllow enqueues a new job regardless of previous job status.
	OverlapAllow OverlapPolicy = "allow"
	// OverlapReplace cancels the previous job and enqueues a new one.
	OverlapReplace OverlapPolicy = "replace"
)

// CronEntry defines a recurring job schedule.
type CronEntry struct {
	// User-configurable fields.
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	CronExpr      string        `json:"cron_expr"`
	Timezone      string        `json:"timezone,omitempty"`
	JobType       string        `json:"job_type"`
	Queue         string        `json:"queue,omitempty"`
	Payload       Payload       `json:"payload,omitempty"`
	Timeout       int           `json:"timeout,omitempty"`
	MaxRetry      int           `json:"max_retry,omitempty"`
	OverlapPolicy OverlapPolicy `json:"overlap_policy,omitempty"`
	Enabled       bool          `json:"enabled"`

	// System-managed state (populated by the scheduler, not by the user).
	LastRun    int64  `json:"last_run,omitempty"`
	LastStatus string `json:"last_status,omitempty"`
	NextRun    int64  `json:"next_run,omitempty"`
	CreatedAt  int64  `json:"created_at,omitempty"`
	UpdatedAt  int64  `json:"updated_at,omitempty"`

	// Parsed cron expression (not serialized).
	expr *CronExpr
}

// validate checks required fields and parses the cron expression.
func (e *CronEntry) validate() error {
	if e.ID == "" {
		return fmt.Errorf("cron entry ID must not be empty")
	}
	if e.CronExpr == "" {
		return fmt.Errorf("cron entry %q: cron_expr must not be empty", e.ID)
	}
	if e.JobType == "" {
		return fmt.Errorf("cron entry %q: job_type must not be empty", e.ID)
	}

	parsed, err := ParseCronExpr(e.CronExpr)
	if err != nil {
		return fmt.Errorf("cron entry %q: %w", e.ID, err)
	}
	e.expr = parsed

	if e.Timezone != "" {
		if _, err := time.LoadLocation(e.Timezone); err != nil {
			return fmt.Errorf("cron entry %q: invalid timezone %q: %w", e.ID, e.Timezone, err)
		}
	}

	return nil
}

// applyDefaults fills in default values for optional fields.
func (e *CronEntry) applyDefaults() {
	if e.Queue == "" {
		e.Queue = "default"
	}
	if e.OverlapPolicy == "" {
		e.OverlapPolicy = OverlapSkip
	}
}
