.PHONY: setup
setup:
	docker network create music-stream-network

clean:
	docker network create music-stream-network