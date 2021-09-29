package dnstapserver

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	dnstap "passivedns/dnstap"

	framestream "github.com/farsightsec/golang-framestream"
	protobuf "google.golang.org/protobuf/proto"
)

func spawn(thread func(), wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		thread()
	}()
}

type DnstapMessageHandler interface {
	Handle(message *dnstap.Message)
	Close()
}

type DnstapServer interface {
	Read(input io.Reader, bidrectional bool, timeout time.Duration)
	Stop()
	Wait()
}

type dnstapworker struct {
	id       int
	handlers []DnstapMessageHandler
}

func (this *dnstapworker) Id() int {
	return this.id
}

func (this *dnstapworker) Stop() {
	for _, handler := range this.handlers {
		handler.Close()
	}
}

type DnstapWorker interface {
	Id() int
	Stop()
}

func (this *dnstapworker) listen(pipe <-chan []byte) {
	fmt.Fprintf(os.Stderr, "Dnstap worker %v is now listening for messages\n", this.id)
	for frame := range pipe {
		dns := dnstap.Dnstap{}
		if e := protobuf.Unmarshal(frame, &dns); e == nil {
			if *dns.Type == dnstap.Dnstap_MESSAGE && dns.Message != nil && this.handlers != nil && 0 < len(this.handlers) {
				for _, handler := range this.handlers {
					if handler != nil {
						handler.Handle(dns.Message)
					}
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "protobuf.Unmarshal(...) failed: %s", e)
			break
		}
	}
	fmt.Fprintf(os.Stderr, "Dnstap %v worker thread terminated\n", this.id)
}

func New(workers int, queue int, handlers func(worker DnstapWorker) []DnstapMessageHandler) DnstapServer {
	fmt.Fprintln(os.Stderr, "Creating Dnstap server")
	server := &dnstapserver{wg: new(sync.WaitGroup), pipe: make(chan []byte, queue), mutex: new(sync.RWMutex), running: true, workers: make([]DnstapWorker, 0, workers)}

	fmt.Fprintf(os.Stderr, "Spawning %v Dnstap worker thread(s)\n", workers)
	for i := 0; i < workers; i++ {
		// create worker
		worker := &dnstapworker{id: i}

		// register the message handlers
		worker.handlers = handlers(worker)

		// associate the worker with the server
		server.workers = append(server.workers, worker)

		// spawn worker and start listen for frames in the pipe
		spawn(func() { worker.listen(server.pipe) }, server.wg)
	}

	return server
}

type dnstapserver struct {
	wg      *sync.WaitGroup
	mutex   *sync.RWMutex
	running bool
	pipe    chan []byte
	workers []DnstapWorker
}

// MaxFrameSize sets the upper limit on input Dnstap payload (frame) sizes. If an Input
// receives a Dnstap payload over this size limit, ReadInto will log an error and return.
//
// EDNS0 and DNS over TCP use 2 octets for DNS message size, imposing a maximum
// size of 65535 octets for the DNS message, which is the bulk of the data carried
// in a Dnstap message. Protobuf encoding overhead and metadata with some size
// guidance (e.g., identity and version being DNS strings, which have a maximum
// length of 255) add up to less than 1KB. The default 96KiB size of the buffer
// allows a bit over 30KB space for "extra" metadata.
const MAXFRAMESIZE uint32 = 96 * 1024

func (this *dnstapserver) redirect(reader *framestream.Reader, pipe chan<- []byte) {
	buffer := make([]byte, MAXFRAMESIZE)
	for this.running {
		if length, e := reader.ReadFrame(buffer); e == nil {
			frame := make([]byte, length)
			if copy(frame, buffer) != length {
				panic(fmt.Sprintf("Something went terribly wrong, failed to copy %v bytes from the receive buffer to a Dnstap frame", length))
			}

			this.mutex.RLock()
			if this.running {
				// write dnstap frame to channel
				pipe <- frame
			}
			this.mutex.RUnlock()
		} else {
			if e != io.EOF {
				fmt.Fprintf(os.Stderr, "Dnstap server thread encountered the following unexpected error \"%v\"\n", e)
			}
			break
		}
	}
	fmt.Fprintln(os.Stderr, "Dnstap server thread terminated")
}

const CONTENT_TYPE_PROTOBUF_DNSTAP = "protobuf:dnstap.Dnstap"

func (this *dnstapserver) Read(input io.Reader, bidirectional bool, timeout time.Duration) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	if this.running {
		if stream, e := framestream.NewReader(input, &framestream.ReaderOptions{ContentTypes: [][]byte{[]byte(CONTENT_TYPE_PROTOBUF_DNSTAP)}, Bidirectional: bidirectional, Timeout: timeout}); e == nil {
			fmt.Fprintln(os.Stderr, "Spawning Dnstap server thread")
			spawn(func() { this.redirect(stream, this.pipe) }, this.wg)
		} else {
			panic(e)
		}
	} else {
		panic("Dnstap server is stopped (closed)\n")
	}
}

func (this *dnstapserver) Stop() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.running {
		fmt.Fprintln(os.Stderr, "Stopping the Dnstap server")

		for _, worker := range this.workers {
			worker.Stop()
		}

		this.running = false

		close(this.pipe)
		//this.pipe = nil

		fmt.Fprintln(os.Stderr, "Dnstap server stopped")
	}
}

func (this *dnstapserver) Wait() {
	this.wg.Wait()
}
