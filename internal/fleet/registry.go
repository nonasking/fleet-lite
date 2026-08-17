// Package fleet holds the server's in-memory model of the fleet.
//
// The registry answers one question — "what do we believe about each robot right now?" —
// and is deliberately separated from transport (gRPC) so it can be tested without a network.
// Liveness is judged the same way daijin's MQTT fleet does it, but here the server owns the
// judgment instead of a broker LWT: a robot that hasn't been heard from within Timeout is
// marked offline by Sweep, and the transition (not the state) is what gets reported.
package fleet

import (
	"sync"
	"time"

	pb "github.com/nonasking/fleet-lite/gen/fleetpb"
)

// State is the server's belief about one robot.
type State struct {
	RobotID  string
	Online   bool
	LastSeen time.Time
	Last     *pb.Telemetry // nil until the first telemetry arrives
}

type Registry struct {
	mu      sync.Mutex
	states  map[string]*State
	timeout time.Duration
}

func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{states: make(map[string]*State), timeout: timeout}
}

// Update ingests one telemetry message. It returns true when this update brought the
// robot online (first contact or recovery) — the caller logs transitions, not states.
func (r *Registry) Update(t *pb.Telemetry, now time.Time) (cameOnline bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.states[t.RobotId]
	if !ok {
		st = &State{RobotID: t.RobotId}
		r.states[t.RobotId] = st
	}
	cameOnline = !st.Online
	st.Online = true
	st.LastSeen = now
	st.Last = t
	return cameOnline
}

// Sweep marks robots silent for longer than the timeout as offline and returns
// the IDs that transitioned on this call. Run it periodically.
func (r *Registry) Sweep(now time.Time) (wentOffline []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, st := range r.states {
		if st.Online && now.Sub(st.LastSeen) > r.timeout {
			st.Online = false
			wentOffline = append(wentOffline, id)
		}
	}
	return wentOffline
}

// Snapshot returns a copy of every state for the HTTP API. Copies, not pointers —
// the caller must not be able to mutate registry internals.
func (r *Registry) Snapshot() []State {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]State, 0, len(r.states))
	for _, st := range r.states {
		out = append(out, *st)
	}
	return out
}
