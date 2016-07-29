// Хуки приложения
package hooks

var nop = func() {}

var (
	OnProcess       = nop // func()
	OnRecover       = nop
	OnJobPerform    = nop
	OnPoolExit      = nop
	OnListenerExit  = nop
	OnReadStart     = nop
	OnReadExit      = nop
	OnReadReconnect = nop
	OnReadFanOut    = nop
	OnMonitorStart  = nop
	OnMonitorExit   = nop
)

var nip = func(int) {}

var (
	OnWorkerExit  = nip // func(int)
	OnWorkerStart = nip
	OnTaskFinish  = nip
)

func Reset() {
	OnProcess = nop
	OnRecover = nop
	OnJobPerform = nop
	OnPoolExit = nop
	OnListenerExit = nop
	OnReadStart = nop
	OnReadExit = nop
	OnReadReconnect = nop
	OnReadFanOut = nop
	OnMonitorStart = nop
	OnMonitorExit = nop

	OnWorkerExit = nip
	OnWorkerStart = nip
	OnTaskFinish = nip
}
