package fleet

import (
	"sync"
	"testing"

	pb "github.com/nonasking/fleet-lite/gen/fleetpb"
)

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

// close-vs-send 경합 회귀 테스트: Register(교체·close)와 Send가 동시에 몰아쳐도
// 패닉이 없어야 한다. -race와 함께 돌 때 의미가 있다.
func TestHubConcurrentReconnectAndSend(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() { // 재연결 폭풍
		defer wg.Done()
		for i := 0; i < 200; i++ {
			ch, done := h.Register("r1")
			go func() { // 소비자: 채널이 닫힐 때까지 비움
				for range ch {
				}
			}()
			done()
		}
		close(stop)
	}()

	wg.Add(1)
	go func() { // 송신 폭풍: 성공/실패 무관, 패닉만 없으면 된다
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = h.Send("r1", &pb.Command{CommandId: "x"})
			}
		}
	}()
	wg.Wait()
}
