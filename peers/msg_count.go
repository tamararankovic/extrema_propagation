package peers

import "sync"

var (
	MessagesSent     = 0
	MessagesRcvd     = 0
	MessagesSentLock = new(sync.Mutex)
	MessagesRcvdLock = new(sync.Mutex)
)
