TARGETS := stress.test compaction_setup

all: $(TARGETS)

stress.test: stress_test.go
	go test -c stress_test.go

compaction_setup: compaction_setup.go
	go build compaction_setup.go

.PHONY: clean
clean:
	rm -rf $(TARGETS)
