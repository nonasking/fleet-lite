.PHONY: gen test vet demo clean

# proto 재생성 — module 모드라 gen/fleetpb에 바로 떨어진다 (mv 불필요)
gen:
	protoc --go_out=. --go_opt=module=github.com/nonasking/fleet-lite \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/nonasking/fleet-lite \
	       --proto_path=proto proto/fleet.proto

test:
	go vet ./...
	go test -race ./...

# 서버 + 로봇 3대(1대는 flaky)를 한 번에 — 데모·개발용
demo:
	@trap 'kill 0' EXIT; \
	go run ./cmd/server & sleep 1; \
	go run ./cmd/robot --id r1 & \
	go run ./cmd/robot --id r2 --flaky & \
	go run ./cmd/robot --id r3 & \
	wait

clean:
	pkill -f "cmd/server" 2>/dev/null; pkill -f "cmd/robot" 2>/dev/null; true
