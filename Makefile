
KAFKA_TOOL_NAME = kafcat

BUILD_CMD = go build -a -tags netgo -installsuffix netgo
LD_FLAGS = -ldflags="-X main.branch=${BRANCH} -X main.commit=${COMMIT} -X main.buildtime=${BUILDTIME} -w"


clean:
	rm -f kafcat

$(KAFKA_TOOL_NAME):
	go build -o $(KAFKA_TOOL_NAME) ./cmd/$(KAFKA_TOOL_NAME)/main.go

dist:
	mkdir -f ./build
	$(BUILD_CMD) -o ./build/$(KAFKA_TOOL_NAME) ./cmd/$(KAFKA_TOOL_NAME)/main.go
	tar -cvzf ./build/$(KAFKA_TOOL_NAME)-darwin.tgz ./build/$(KAFKA_TOOL_NAME)
