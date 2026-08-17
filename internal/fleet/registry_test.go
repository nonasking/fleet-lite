package fleet

import (
	"testing"
	"time"

	pb "github.com/nonasking/fleet-lite/gen/fleetpb"
)

func tele(id string) *pb.Telemetry { return &pb.Telemetry{RobotId: id, BatteryPct: 80} }

// 상태 전이 판정: 등록/복귀는 Update가, 오프라인은 Sweep이 "전이"만 보고해야 한다.
func TestRegistryTransitions(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	cases := []struct {
		name        string
		script      func(r *Registry) (cameOnline bool, wentOffline []string)
		wantOnline  bool
		wantOffline int
	}{
		{
			name: "first contact comes online",
			script: func(r *Registry) (bool, []string) {
				return r.Update(tele("r1"), t0), nil
			},
			wantOnline: true,
		},
		{
			name: "steady heartbeat is not a transition",
			script: func(r *Registry) (bool, []string) {
				r.Update(tele("r1"), t0)
				return r.Update(tele("r1"), t0.Add(time.Second)), nil
			},
			wantOnline: false,
		},
		{
			name: "silence past timeout goes offline exactly once",
			script: func(r *Registry) (bool, []string) {
				r.Update(tele("r1"), t0)
				first := r.Sweep(t0.Add(6 * time.Second))
				second := r.Sweep(t0.Add(7 * time.Second)) // 이미 오프라인 → 전이 아님
				if len(second) != 0 {
					return false, append(first, second...)
				}
				return false, first
			},
			wantOffline: 1,
		},
		{
			name: "silence within timeout stays online",
			script: func(r *Registry) (bool, []string) {
				r.Update(tele("r1"), t0)
				return false, r.Sweep(t0.Add(3 * time.Second))
			},
			wantOffline: 0,
		},
		{
			name: "telemetry after offline is a recovery",
			script: func(r *Registry) (bool, []string) {
				r.Update(tele("r1"), t0)
				off := r.Sweep(t0.Add(10 * time.Second))
				return r.Update(tele("r1"), t0.Add(11*time.Second)), off
			},
			wantOnline:  true,
			wantOffline: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRegistry(5 * time.Second)
			cameOnline, wentOffline := tc.script(r)
			if cameOnline != tc.wantOnline {
				t.Errorf("cameOnline = %v, want %v", cameOnline, tc.wantOnline)
			}
			if len(wentOffline) != tc.wantOffline {
				t.Errorf("wentOffline = %v, want %d transitions", wentOffline, tc.wantOffline)
			}
		})
	}
}

// 스냅샷은 복사본이어야 한다 — 호출자가 레지스트리 내부를 오염시킬 수 없어야 함.
func TestSnapshotIsACopy(t *testing.T) {
	r := NewRegistry(5 * time.Second)
	r.Update(tele("r1"), time.Now())
	snap := r.Snapshot()
	snap[0].Online = false
	if got := r.Snapshot()[0].Online; !got {
		t.Fatal("mutating a snapshot changed registry state")
	}
}

// 명령 라우팅: 연결된 로봇에만 전달, 미연결은 즉시 에러, 재연결은 좀비 스트림을 대체.
func TestHubRouting(t *testing.T) {
	h := NewHub()

	if err := h.Send("r1", &pb.Command{Verb: "stop"}); err == nil {
		t.Fatal("send to unconnected robot should fail")
	}

	ch, done := h.Register("r1")
	if err := h.Send("r1", &pb.Command{CommandId: "c1", Verb: "goto", Arg: "1,2"}); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	got := <-ch
	if got.CommandId != "c1" || got.Verb != "goto" {
		t.Fatalf("wrong command routed: %+v", got)
	}

	// 재연결: 새 채널이 이기고, 옛 채널은 닫힌다
	ch2, done2 := h.Register("r1")
	if _, ok := <-ch; ok {
		t.Fatal("old channel should be closed after reconnect")
	}
	if err := h.Send("r1", &pb.Command{CommandId: "c2"}); err != nil {
		t.Fatalf("send after reconnect failed: %v", err)
	}
	if got := <-ch2; got.CommandId != "c2" {
		t.Fatalf("command went to the wrong stream: %+v", got)
	}

	// 옛 스트림의 정리 함수가 새 등록을 지우면 안 된다
	done()
	if !h.Connected("r1") {
		t.Fatal("stale cleanup removed the fresh registration")
	}
	done2()
	if h.Connected("r1") {
		t.Fatal("cleanup failed to remove registration")
	}
}
