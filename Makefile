
KAFKA_TOOL_NAME = kafcat

BUILD_CMD = CGO_ENABLED=0 go build -a -tags netgo -installsuffix netgo
LD_FLAGS = -ldflags="-X main.branch=${BRANCH} -X main.commit=${COMMIT} -X main.buildtime=${BUILDTIME} -w"


clean:
	rm -rf ./build
	rm -f kafcat

$(KAFKA_TOOL_NAME):
	go build -o $(KAFKA_TOOL_NAME) ./cmd/$(KAFKA_TOOL_NAME)/main.go

dist: clean
	mkdir ./build

	$(BUILD_CMD) -o ./build/$(KAFKA_TOOL_NAME) .
	cd ./build && tar -czf $(KAFKA_TOOL_NAME)-$(GOOS).tgz $(KAFKA_TOOL_NAME); rm -f $(KAFKA_TOOL_NAME)

	GOOS=linux $(BUILD_CMD) -o ./build/$(KAFKA_TOOL_NAME) .
	cd ./build && tar -czf $(KAFKA_TOOL_NAME)-linux.tgz $(KAFKA_TOOL_NAME); rm -f $(KAFKA_TOOL_NAME)

	GOOS=windows $(BUILD_CMD) -o ./build/$(KAFKA_TOOL_NAME).exe .
	cd ./build && zip $(KAFKA_TOOL_NAME)-windows.zip $(KAFKA_TOOL_NAME).exe; rm -f $(KAFKA_TOOL_NAME).exe
