package fleet

import (
	"fmt"
	"sync"

	pb "github.com/nonasking/fleet-lite/gen/fleetpb"
)

// Hub routes commands to the live CommandChannel stream of each robot.
// One buffered channel per connected robot; Send fails fast when the robot has no
// channel open (offline) or its buffer is full (backpressure made visible instead of
// silently queueing forever — a deliberate contrast with MQTT's fire-and-forget QoS0).
type Hub struct {
	mu    sync.Mutex
	conns map[string]chan *pb.Command
}

func NewHub() *Hub {
	return &Hub{conns: make(map[string]chan *pb.Command)}
}

// Register attaches a robot's command stream. The returned function detaches it and
// must be called when the stream dies. Registering the same robot again replaces the
// old channel (a reconnect wins over a zombie stream).
func (h *Hub) Register(robotID string) (<-chan *pb.Command, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if old, ok := h.conns[robotID]; ok {
		close(old)
	}
	ch := make(chan *pb.Command, 8)
	h.conns[robotID] = ch
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.conns[robotID] == ch { // only remove if a reconnect hasn't replaced us
			delete(h.conns, robotID)
			close(ch)
		}
	}
}

// Send queues a command for a connected robot.
func (h *Hub) Send(robotID string, cmd *pb.Command) error {
	h.mu.Lock()
	ch, ok := h.conns[robotID]
	h.mu.Unlock()
	if !ok {
		return fmt.Errorf("robot %q has no open command channel", robotID)
	}
	select {
	case ch <- cmd:
		return nil
	default:
		return fmt.Errorf("robot %q command buffer full", robotID)
	}
}

// Connected reports whether a robot currently holds a command channel.
func (h *Hub) Connected(robotID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.conns[robotID]
	return ok
}
