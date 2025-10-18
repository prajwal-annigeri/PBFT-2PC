package config

const (
	InitialBalance   = 10
	F                = 1
	CheckpointPeriod = 1000
)

var States = map[string]int64{
	"pre-prepared": 1,
	"prepared":     2,
	"committed":    3,
	"pexecuted":    4,
	"executed":     5,
}

var IntToStates = map[int64]string{
	1: "PP",
	2: "P",
	3: "C",
	4: "PE",
	5: "E",
}
