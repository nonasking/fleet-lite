// fleet-lite server: gRPC fleet ingress + tiny HTTP control panel.
//
//	gRPC :9090  — robots connect here (StreamTelemetry, CommandChannel)
//	HTTP :8080  — humans/agents: GET /fleet (JSON snapshot), POST /command
//
// Run: go run ./cmd/server
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"

	pb "github.com/nonasking/fleet-lite/gen/fleetpb"
	"github.com/nonasking/fleet-lite/internal/fleet"
)

type server struct {
	pb.UnimplementedFleetServiceServer
	reg *fleet.Registry
	hub *fleet.Hub
}

// StreamTelemetry: 로봇이 자기 페이스로 밀어넣는 클라이언트 스트림.
// 전이(첫 접속/복귀)만 로그에 남긴다 — 상태 로그는 소음, 전이 로그는 신호.
func (s *server) StreamTelemetry(stream pb.FleetService_StreamTelemetryServer) error {
	var n uint64
	for {
		t, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.StreamSummary{Received: n})
		}
		if err != nil {
			return err // 연결 끊김 → 이후 liveness는 Sweep이 판정
		}
		n++
		if s.reg.Update(t, time.Now()) {
			log.Printf("🟢 %s online (battery %.0f%%)", t.RobotId, t.BatteryPct)
		}
	}
}

// CommandChannel: 첫 이벤트는 반드시 Hello. 이후 서버→로봇 명령과 로봇→서버 ACK가
// 한 스트림 위에서 오간다.
func (s *server) CommandChannel(stream pb.FleetService_CommandChannelServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("first event must be Hello")
	}
	id := hello.RobotId
	cmds, done := s.hub.Register(id)
	defer done()
	log.Printf("🔌 %s command channel open", id)

	// 서버 → 로봇: 허브 채널을 스트림으로 흘림
	go func() {
		for cmd := range cmds {
			if err := stream.Send(cmd); err != nil {
				return
			}
		}
	}()

	// 로봇 → 서버: ACK 수신
	for {
		ev, err := stream.Recv()
		if err != nil {
			log.Printf("🔌 %s command channel closed: %v", id, err)
			return err
		}
		if ack := ev.GetAck(); ack != nil {
			log.Printf("✅ %s ack %s ok=%v %s", id, ack.CommandId, ack.Ok, ack.Detail)
		}
	}
}

func main() {
	grpcAddr := flag.String("grpc", ":9090", "gRPC listen address")
	httpAddr := flag.String("http", ":8080", "HTTP listen address")
	timeout := flag.Duration("timeout", 5*time.Second, "offline judgment: max silence before a robot is marked offline")
	flag.Parse()

	s := &server{reg: fleet.NewRegistry(*timeout), hub: fleet.NewHub()}

	// 주기 스윕: 침묵 초과 → 오프라인 전이 보고 (daijin의 LWT 역할을 서버가 직접 수행)
	go func() {
		for range time.Tick(time.Second) {
			for _, id := range s.reg.Sweep(time.Now()) {
				log.Printf("🔴 %s offline (>%s silent)", id, *timeout)
			}
		}
	}()

	// HTTP: 사람/에이전트용 얇은 창구
	mux := http.NewServeMux()
	mux.HandleFunc("GET /fleet", func(w http.ResponseWriter, r *http.Request) {
		type row struct {
			RobotID  string        `json:"robot_id"`
			Online   bool          `json:"online"`
			AgoMs    int64         `json:"last_seen_ms_ago"`
			Battery  float64       `json:"battery_pct"`
			Status   string        `json:"status"`
			X, Y     float64       `json:"-"`
			Pos      [2]float64    `json:"pos"`
			CmdOpen  bool          `json:"command_channel"`
		}
		now := time.Now()
		var out []row
		for _, st := range s.reg.Snapshot() {
			rw := row{RobotID: st.RobotID, Online: st.Online,
				AgoMs: now.Sub(st.LastSeen).Milliseconds(), CmdOpen: s.hub.Connected(st.RobotID)}
			if st.Last != nil {
				rw.Battery = st.Last.BatteryPct
				rw.Status = st.Last.Status.String()
				if p := st.Last.Pos; p != nil {
					rw.Pos = [2]float64{p.X, p.Y}
				}
			}
			out = append(out, rw)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("POST /command", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ RobotID, Verb, Arg string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		cmd := &pb.Command{CommandId: fmt.Sprintf("c-%d", time.Now().UnixMilli()), Verb: req.Verb, Arg: req.Arg}
		if err := s.hub.Send(req.RobotID, cmd); err != nil {
			http.Error(w, err.Error(), 409) // 오프라인/버퍼포화 — 실패를 숨기지 않는다
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"command_id": cmd.CommandId})
	})

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterFleetServiceServer(gs, s)
	log.Printf("fleet-lite: gRPC %s · HTTP %s · offline timeout %s", *grpcAddr, *httpAddr, *timeout)
	go func() { log.Fatal(http.ListenAndServe(*httpAddr, mux)) }()
	log.Fatal(gs.Serve(lis))
}
