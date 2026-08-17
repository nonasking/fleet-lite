// Virtual robot: a small state machine (IDLE → MOVING → CHARGING, plus FAULT) that
// streams telemetry and obeys commands. Deliberately simple — the point is fleet
// behavior (many robots, disconnects, recovery), not robot realism (see README:
// "What this deliberately does not do").
//
// Run three of them:
//
//	go run ./cmd/robot --id r1 &
//	go run ./cmd/robot --id r2 --flaky &   # drops its connection now and then
//	go run ./cmd/robot --id r3 &
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/nonasking/fleet-lite/gen/fleetpb"
)

type sim struct {
	id      string
	x, y    float64
	tx, ty  float64 // goto target
	battery float64
	status  pb.RobotStatus
}

func (s *sim) tick() {
	switch s.status {
	case pb.RobotStatus_MOVING:
		dx, dy := s.tx-s.x, s.ty-s.y
		d := math.Hypot(dx, dy)
		if d < 0.3 {
			s.status = pb.RobotStatus_IDLE
			break
		}
		s.x += dx / d * 0.5 // 0.5 m/tick
		s.y += dy / d * 0.5
		s.battery -= 0.4
	case pb.RobotStatus_CHARGING:
		s.battery += 2
		if s.battery >= 95 {
			s.status = pb.RobotStatus_IDLE
		}
	case pb.RobotStatus_IDLE:
		s.battery -= 0.05
	}
	if s.battery < 20 && s.status != pb.RobotStatus_CHARGING && s.status != pb.RobotStatus_FAULT {
		s.tx, s.ty = 0, 0 // dock
		s.status = pb.RobotStatus_CHARGING
	}
}

// handle applies a command and returns the ack detail. Unknown verbs fail loudly —
// the server should learn about protocol drift, not guess.
func (s *sim) handle(cmd *pb.Command) (bool, string) {
	switch cmd.Verb {
	case "goto":
		if s.status == pb.RobotStatus_FAULT {
			return false, "in FAULT; clear_fault first"
		}
		if _, err := fmt.Sscanf(strings.TrimSpace(cmd.Arg), "%f,%f", &s.tx, &s.ty); err != nil {
			return false, "bad arg, want x,y"
		}
		s.status = pb.RobotStatus_MOVING
		return true, fmt.Sprintf("moving to %.1f,%.1f", s.tx, s.ty)
	case "stop":
		s.status = pb.RobotStatus_IDLE
		return true, "stopped"
	case "charge":
		s.tx, s.ty = 0, 0
		s.status = pb.RobotStatus_CHARGING
		return true, "docking"
	case "fault": // 데모용: 고장 주입
		s.status = pb.RobotStatus_FAULT
		return true, "fault injected"
	case "clear_fault":
		s.status = pb.RobotStatus_IDLE
		return true, "fault cleared"
	default:
		return false, "unknown verb " + cmd.Verb
	}
}

// session runs one connected session (both streams) until something breaks.
// 재접속·백오프는 daijin에서와 같은 문제의식: 스트림은 반드시 끊긴다,
// 원상복구가 기본 동작이어야 한다.
func session(ctx context.Context, addr string, s *sim, period time.Duration, flaky bool) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewFleetServiceClient(conn)

	tstream, err := client.StreamTelemetry(ctx)
	if err != nil {
		return err
	}
	cstream, err := client.CommandChannel(ctx)
	if err != nil {
		return err
	}
	if err := cstream.Send(&pb.RobotEvent{Kind: &pb.RobotEvent_Hello{Hello: &pb.Hello{RobotId: s.id}}}); err != nil {
		return err
	}

	// 수신 고루틴은 Recv만 한다. 시뮬레이터 상태와 cstream.Send는 전부 아래 메인 루프
	// 한 곳에서만 만진다 — sim에 뮤텍스를 다는 대신 소유권을 한 고루틴에 몰아주는 방식
	// (-race로 실증된 tick↔handle 경합의 수리).
	errc := make(chan error, 1)
	cmdc := make(chan *pb.Command, 8)
	go func() {
		for {
			cmd, err := cstream.Recv()
			if err != nil {
				errc <- err
				return
			}
			cmdc <- cmd
		}
	}()

	deadline := time.Time{}
	if flaky { // 30~60초마다 스스로 연결을 끊어 장애·복구를 만든다
		deadline = time.Now().Add(time.Duration(30+rand.Intn(30)) * time.Second)
	}
	tick := time.NewTicker(period)
	defer tick.Stop()
	for {
		select {
		case err := <-errc:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case cmd := <-cmdc:
			ok, detail := s.handle(cmd)
			log.Printf("[%s] cmd %s(%s) → %v %s", s.id, cmd.Verb, cmd.Arg, ok, detail)
			if err := cstream.Send(&pb.RobotEvent{Kind: &pb.RobotEvent_Ack{Ack: &pb.CommandAck{
				CommandId: cmd.CommandId, RobotId: s.id, Ok: ok, Detail: detail}}}); err != nil {
				return err
			}
		case <-tick.C:
			s.tick()
			if !deadline.IsZero() && time.Now().After(deadline) {
				return fmt.Errorf("flaky: simulated link drop")
			}
			err := tstream.Send(&pb.Telemetry{
				RobotId: s.id, TsUnixMs: time.Now().UnixMilli(),
				BatteryPct: s.battery, TempC: 35 + rand.Float64()*5,
				Status: s.status,
				Pos:    &pb.Position{X: s.x, Y: s.y},
			})
			if err != nil {
				return err
			}
		}
	}
}

func main() {
	id := flag.String("id", "r1", "robot id")
	addr := flag.String("server", "localhost:9090", "fleet server gRPC address")
	period := flag.Duration("period", time.Second, "telemetry period")
	flaky := flag.Bool("flaky", false, "randomly drop the connection to exercise offline/recovery")
	flag.Parse()

	s := &sim{id: *id, battery: 100, status: pb.RobotStatus_IDLE}
	backoff := 500 * time.Millisecond
	for {
		start := time.Now()
		err := session(context.Background(), *addr, s, *period, *flaky)
		log.Printf("[%s] session ended: %v — reconnecting in %s", s.id, err, backoff)
		// 오래 살았던 세션이면 백오프 리셋, 즉사 반복이면 지수 증가 (최대 8초)
		if time.Since(start) > 30*time.Second {
			backoff = 500 * time.Millisecond
		}
		time.Sleep(backoff)
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}
