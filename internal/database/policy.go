package database

const (
	minSize          = 64 * 1024 * 1024
	growthPercentage = 100
)

type RewritePolicy struct {
	MinSize          int64 // eg: 64MB
	GrowthPercentage int   // eg: 100
}
