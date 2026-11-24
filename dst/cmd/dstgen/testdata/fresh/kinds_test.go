package fresh

// testIn is open: test scripts switch over part of it.
//
//dst:sum open
type testIn interface {
	Message
	isTestIn()
}

//dst:cases testIn
type (
	ping struct{ N uint64 }
	pong struct{ Msg in }
)
